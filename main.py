import boto3
import peewee

REGION_NAME = 'eu-central-1'
VAULT_NAME = 'michou-photos'
BUCKET_NAME = 'michou-photos-metadata'


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


def main(root_folder):
    database = retrieve_remote_database()
    for folder_name in list_local_folders(root_folder):
        folder_checksum = hash_folder(folder_name)
        stored_folder_checksums = get_stored_folder_checksums(database, folder_name)
        if folder_checksum not in stored_folder_checksums:
            archive_name = pack_folder(folder_name)
            upload_archive(archive_name)
            store_folder_version(database, folder_name, folder_checksum, archive_name)
    update_remote_database(database)


if __name__ == '__main__':
    main()