import boto3

REGION_NAME = 'eu-central-1'
VAULT_NAME = 'michou-photos'
BUCKET_NAME = 'michou-photos-metadata'


def main():
    s3 = boto3.resource('s3', region_name=REGION_NAME)

    if BUCKET_NAME not in map(lambda b: b.name, s3.buckets.all()):
        print('Creating ', BUCKET_NAME)
        s3.create_bucket(ACL='private', Bucket=BUCKET_NAME, CreateBucketConfiguration={
            'LocationConstraint': REGION_NAME
        })

    glacier = boto3.resource('glacier', region_name=REGION_NAME)

    glacier.create_vault(vaultName=VAULT_NAME)

    for vault in glacier.vaults.all():
        print(vault.name)


if __name__ == '__main__':
    main()
