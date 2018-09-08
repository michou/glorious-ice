import logging
import os
import tempfile

import boto3
import peewee
import argparse

from boto3.exceptions import Boto3Error

REGION_NAME = 'eu-central-1'
VAULT_NAME = 'michou-photos'
BUCKET_NAME = 'michou-photos-metadata'
DB_KEY = 'solid-glorious-ice.db'


def list_local_folders(root_folder):
    return []


def hash_folder(folder_name):
    return ''


def get_stored_folder_checksums(database, folder_name):
    return []


def retrieve_remote_database():
    pass


def pack_folder(folder_name):
    return ''


def upload_archive(archive_name):
    pass


def store_folder_version(database, folder_name, folder_checksum, archive_name):
    pass


def update_remote_database(database):
    pass


def perform_backup(root_folder, aws_region, aws_bucket, aws_vault):
    database = retrieve_remote_database()
    for folder_name in list_local_folders(root_folder):
        folder_checksum = hash_folder(folder_name)
        stored_folder_checksums = get_stored_folder_checksums(database, folder_name)
        if folder_checksum not in stored_folder_checksums:
            archive_name = pack_folder(folder_name)
            upload_archive(archive_name)
            store_folder_version(database, folder_name, folder_checksum, archive_name)
    update_remote_database(database)


class Index(object):
    def __init__(self, db_file):
        self.db_file = db_file

    def get_folder_checksums(self):
        return []

    def add_folder_entry(self, folder_name, checksum, archive_name):
        pass

    def flush(self):
        pass

class IndexManager(object):
    def __init__(self, region, bucket):
        self.region = region
        self.bucket_name = bucket

        self._s3 = boto3.resource('s3', region_name=self.region)
        self._s3_bucket = self._s3.Bucket(name=self.bucket_name)

        self.index = None
        self._index_location = tempfile.TemporaryDirectory(prefix='solid-ice')

        self._log = logging.getLogger('Indexer')

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
        self._check_bucket()

        local_db_path = os.path.join(self._index_location.name, DB_KEY)
        self._s3_bucket.download_file(DB_KEY, local_db_path)

        self.index = Index(local_db_path)

    def upload_index(self):
        self.index.flush()
        self._s3_bucket.upload_file(self.index.db_file, DB_KEY)
        self._index_location.cleanup()


class VaultManager(object):
    def __init__(self, region, vault_name):
        self.region = region
        self.vault_name = vault_name
        self._glacier = boto3.resource('glacier', region_name=self.region)
        self._glacier_vault = None
        self._log = logging.getLogger('Uploader')

    def check_vault(self):
        try:
            self._glacier_vault = self._glacier.create_vault(vaultName=self.vault_name)
        except Boto3Error as error:
            self._log.error('Could not retrieve/create vault <%s>', self.vault_name, exc_info=error)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Backup a hierarchy of folders, with 1-depth-level granularity. '
                                                 'When the folder already exist but has been modified a new version '
                                                 'is uploaded alongside the existing version. It requires access to '
                                                 'Amazon S3 and Amazon Glacier')
    parser.add_argument('--region', action='store', dest='region_name',
                        help='AWS region to use')
    parser.add_argument('--bucket', action='store', dest='bucket_name',
                        help='AWS S3 bucket where the index database is stored. Will be created if it doesn\'t exist')
    parser.add_argument('--vault', action='store', dest='vault_name',
                        help='AWS Glacier vault name where data is actually stored. Will be created if it doesn\'t exist')
    parser.add_argument('--root_folder', action='store', dest='root_folder',
                        help='The folder to scan for changes and backup')

    return parser.parse_args()


if __name__ == '__main__':
    arguments = parse_arguments()
    perform_backup(root_folder=arguments.root_folder,
                   aws_region=arguments.region_name,
                   aws_bucket=arguments.bucket_name,
                   aws_vault=arguments.vault_name)
