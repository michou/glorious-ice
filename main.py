import base64
import datetime
import hashlib
import logging
import os
import tempfile
import time
import zipfile
from logging import FileHandler

import boto3
import argparse

import model as data_model

from botocore.exceptions import ClientError
from boto3.exceptions import Boto3Error

REGION_NAME = 'eu-central-1'
VAULT_NAME = 'michou-photos'
BUCKET_NAME = 'michou-photos-metadata'
DB_KEY = 'solid-glorious-ice.db'


class LoggingObject(object):
    def __init__(self):
        self._log = logging.getLogger(self.__class__.__name__)
        self._log.addHandler(FileHandler('glorious-ice.log'))
        self._log.setLevel('INFO')


class FolderProcessor(LoggingObject):
    def __init__(self, absolute_folder_path, destination_folder):
        super().__init__()
        self.absolute_folder_path = absolute_folder_path
        self.destination_folder = destination_folder
        self.hash = ''

    def list_contents(self):
        paths = [os.path.join(dirname, filename)
                 for dirname, _, files in os.walk(self.absolute_folder_path)
                 for filename in files]
        return sorted(paths)

    def get_hash(self):
        hasher = hashlib.sha256()

        for path in self.list_contents():
            with open(path, 'rb') as file:
                self._log.debug('Hashing <%s>', path)
                for block in iter(lambda: file.read(4096), b''):
                    hasher.update(block)

        self.hash = base64.b64encode(hasher.digest())
        return self.hash

    def archive(self):
        _, archive_base_name = os.path.split(self.absolute_folder_path)

        archive_base_name = '{}_{}.zip'.format(
            archive_base_name.replace(' ', '-'),
            base64.b32encode(
                int(time.time()).to_bytes(6, byteorder='little')
            ).decode('utf-8').replace('=', ''))

        archive_path = os.path.join(self.destination_folder, archive_base_name)
        self._log.info('Writing to archive: <%s>', archive_path)
        with zipfile.ZipFile(archive_path, mode='x') as archive:
            for path in self.list_contents():
                self._log.debug('Adding <%s> to archive', path)
                archive.write(path)

        return archive_path


class FolderWalker(object):
    def __init__(self, root_folder):
        self.root_folder = root_folder

    # TODO Can be improved to specify a depth to stop parsing at. The current implementation assumes all folders in
    # root_folder` are source folders
    def list_folders(self):
        for folder in os.listdir(self.root_folder):
            full_folder_path = os.path.join(self.root_folder, folder)
            if os.path.isdir(full_folder_path):
                yield full_folder_path, folder


class Index(object):
    def __init__(self, db_file):
        self.db_file = db_file

        self._database = data_model.external_database

        self._database.init(self.db_file)
        self._database.create_tables([data_model.Archive, data_model.FileEntry], safe=True)

    def get_folder_checksums(self, folder_name):
        archives = data_model.Archive.select().where(data_model.Archive.folder_name == folder_name)
        return [ archive.checksum for archive in archives]

    def add_folder_entry(self, folder_name, checksum, archive_name, contents):
        archive = data_model.Archive.create(
            name=archive_name,
            folder_name=folder_name,
            checksum=checksum
        )

        for path in contents:
            file_entry = data_model.FileEntry(
                archive=archive,
                path=path
            )
            file_entry.save()

    def update_archive(self, archive_name, external_id, uploaded_at=datetime.datetime.now()):

        archive = data_model.Archive.get(data_model.Archive.name == archive_name)
        archive.external_id = external_id
        archive.uploaded_at = uploaded_at
        archive.save()

    def flush(self):
        self._database.commit()

    def close(self):
        self._database.close()


class IndexManager(LoggingObject):
    def __init__(self, region, bucket, local_folder):
        super().__init__()
        self.region = region
        self.bucket_name = bucket

        self._s3 = boto3.resource('s3', region_name=self.region)
        self._s3_bucket = self._s3.Bucket(name=self.bucket_name)

        self.index = None
        self._index_location = local_folder

        self._check_bucket()

    def _check_bucket(self):
        self._s3_bucket.load()

        if not self._s3_bucket.creation_date:
            try:
                self._log.info('Could not find bucket <%s> in region <%s>.', self.bucket_name, self.region)
                self._s3_bucket.create(ACL='private', Bucket=self.bucket_name,
                                       CreateBucketConfiguration={
                                           'LocationConstraint': self.region
                                       })
                self._s3_bucket.wait_until_exists()
                self._log.info('Created bucket <%s>.', self.bucket_name)
            except Boto3Error as error:
                self._log.error('Could not create bucket <%s>', self.bucket_name, exc_info=error)
        else:
            self._log.info('Using bucket created at %s', self._s3_bucket.creation_date.strftime('%Y-%m-%d %H:%M'))

    def retrieve_index(self):
        local_db_path = os.path.join(self._index_location, DB_KEY)
        try:
            self._s3_bucket.download_file(DB_KEY, local_db_path)
        except ClientError as error:
            if error.response['Error']['Code'] == '404':
                self._log.warning('Could not find DB in bucket. Will create a new one')
            else:
                self._log.error('Error retrieving DB from bucket', error)
        finally:
            self.index = Index(local_db_path)

    def upload_index(self):
        self.index.flush()
        self._s3_bucket.upload_file(self.index.db_file, DB_KEY)


class VaultManager(LoggingObject):
    def __init__(self, region, vault_name):
        super().__init__()

        self.region = region
        self.vault_name = vault_name
        self._glacier = boto3.resource('glacier', region_name=self.region)
        self._glacier_vault = None

        self._check_vault()

    def _check_vault(self):
        try:
            self._glacier_vault = self._glacier.create_vault(vaultName=self.vault_name)
        except Boto3Error as error:
            self._log.error('Could not retrieve/create vault <%s>', self.vault_name, exc_info=error)

    def _dummy_upload(self, archive_file):
        import uuid
        from collections import namedtuple
        return namedtuple('Archive', 'id')(uuid.uuid4().hex)

    def upload_archive(self, archive_file):
        self._log.info('Uploading archive <%s> to Glacier', archive_file)
        result = self._glacier_vault.upload_archive(
            body=archive_file
        )
        # result = self._dummy_upload(archive_file)
        self._log.info('Done uploading <%s> to Glacier. External ID is <%s>', archive_file, result.id)

        return result


class BackupOrchestrator(LoggingObject):
    def __init__(self, region_name, bucket_name, vault_name, root_folder):
        super().__init__()

        self.temporary_folder = tempfile.TemporaryDirectory(prefix='glorious-ice')

        self.vault_manager = VaultManager(region_name, vault_name)
        self.index_manager = IndexManager(region_name, bucket_name, self.temporary_folder.name)
        self.folder_walker = FolderWalker(root_folder)

    def perform_backup(self, limit=-1):
        self._log.info('Performing backup with limit %d', limit)

        self.index_manager.retrieve_index()
        self._log.info('Retrieved index')

        self._log.info('Starting to walk <%s>', self.folder_walker.root_folder)
        for full_folder_path, folder_name in self.folder_walker.list_folders():
            self._log.info('Currently processing <%s>', folder_name)

            folder_processor = FolderProcessor(
                absolute_folder_path=full_folder_path,
                destination_folder=self.temporary_folder.name
            )

            folder_hash = folder_processor.get_hash()
            if folder_hash in self.index_manager.index.get_folder_checksums(folder_name):
                self._log.info('<%s> hash already exists for <%s>. Skipping', folder_hash, folder_name)
                continue

            folder_contents = folder_processor.list_contents()
            archive_path = folder_processor.archive()
            _, archive_name = os.path.split(archive_path)
            self._log.info('Archived <%s> to <%s>', folder_name, archive_name)

            self.index_manager.index.add_folder_entry(
                folder_name=folder_name,
                archive_name=archive_name,
                checksum=folder_hash,
                contents=folder_contents
            )

            glacier_archive = self.vault_manager.upload_archive(archive_path)

            self.index_manager.index.update_archive(
                archive_name=archive_name,
                external_id=glacier_archive.id
            )
            self.index_manager.upload_index()

            limit -= 1
            if limit == 0:
                self._log.info('Limit reached, exiting!')
                break

        self._log.info('Closing local database.')
        self.index_manager.index.close()

        self._log.info('Cleaning up temporary folder.')
        self.temporary_folder.cleanup()


def parse_arguments():
    parser = argparse.ArgumentParser(description='Backup a hierarchy of folders, with 1-depth-level granularity. '
                                                 'When the folder already exist but has been modified a new version '
                                                 'is uploaded alongside the existing version. It requires access to '
                                                 'Amazon S3 and Amazon Glacier')
    parser.add_argument('--region', action='store', dest='region_name', required=True,
                        help='AWS region to use')
    parser.add_argument('--bucket', action='store', dest='bucket_name', required=True,
                        help='AWS S3 bucket where the index database is stored. Will be created if it doesn\'t exist')
    parser.add_argument('--vault', action='store', dest='vault_name', required=True,
                        help='AWS Glacier vault where data is actually stored. Will be created if it doesn\'t exist')
    parser.add_argument('--root-folder', action='store', dest='root_folder', required=True,
                        help='The folder to scan for changes and backup')

    return vars(parser.parse_args())


if __name__ == '__main__':
    # arguments = parse_arguments()
    # perform_backup(root_folder=arguments.root_folder,
    #                aws_region=arguments.region_name,
    #                aws_bucket=arguments.bucket_name,
    #                aws_vault=arguments.vault_name)

    logging.basicConfig(filename='glorious-ice-others.log', level=logging.INFO)

    b = BackupOrchestrator(**parse_arguments())
    b.perform_backup(limit=1)

    # i = Index(':memory:')
    # i.add_folder_entry('juju', '12345', 'juju.zip', ['1', '2'])
    # i.add_folder_entry('juju', 'abcdef', 'juju2.zip', ['1', '2', '3'])
    # print(i.get_folder_checksums('juju'))
    # print(i.get_folder_checksums('jiji'))

    # with tempfile.TemporaryDirectory(prefix='solid-ice') as destination:
    #     fp = FolderProcessor(os.path.abspath('.'), destination)
    #
    #     print(fp.get_hash())
    #     print(fp.archive())
