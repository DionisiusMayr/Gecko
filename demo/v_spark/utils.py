import boto3
import botocore

AWS_ACCESS_KEY_ID = 'test_key_id'
AWS_SECRET_ACCESS_KEY = 'test_access_key'
HOST = 's3'
# HOST = 'localhost'


class S3_conn():
    def __init__(self):
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=f'http://{HOST}:4566'
        )

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=f'http://{HOST}:4566'
        )


    def create_bucket(self, bucket):
        self.s3_client.create_bucket(Bucket=bucket)


    def list_buckets(self):
        print(self.s3_client.list_buckets())


    def put_in_bucket(self, bucket, file_key, data):
        print(f'Putting {data[:10]} in {bucket}/{file_key}')
        self.s3.Object(bucket, file_key).put(Body=data)


    def put_file_in_bucket(self, bucket, file_key, file_path):
        self.s3.Bucket(bucket).upload_file(file_path, file_key)


    def list_all_files_in_bucket(self, bucket):
        for obj in self.s3.Bucket(bucket).objects.all():
            print(obj)


    def get_file(self, bucket, file_key):
        try:
            ret = self.s3.Object(bucket, file_key)
            return ret.get()['Body'].read()
        except:
            return None


    def file_exists(self, bucket, file_key) -> bool:
        try:
            self.s3.Object(bucket, file_key).load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise e


    def delete_everything_from_bucket(self, bucket):
        for obj in self.s3.Bucket(bucket).objects.all():
            obj.delete()


    def get_keys_with_prefix(self, bucket, prefix):
        # TODO: This thing paginates. Need to somehow iterate through all the pages.
        all_keys = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']
        return [k['Key'] for k in all_keys]


if __name__ == '__main__':
    s3 = S3_conn()
    s3.list_buckets()
    file_path = '/home/dionisius/bdma/upc/big_data_management/project/Gecko/demo/compose.yaml'
    s3.put_file_in_bucket('raw-data', 'path/to/test' + 'compose.yaml', file_path)
    s3.list_all_files_in_bucket('raw-data')
    print(s3.file_exists('raw-data', 'path/to/testcompose.yaml'))
    print(s3.file_exists('raw-data', 'path/to/fail'))
    keys_in_bucket = s3.get_keys_with_prefix('raw-data', 'boardgame/boardgame/')
    for k in keys_in_bucket:
        print(k)
