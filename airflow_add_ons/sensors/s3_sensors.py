from airflow.hooks.S3_hook import S3Hook
from airflow.operators.sensors import S3KeySensor


class ReturnS3KeySensor(S3KeySensor):

    def __init__(self,
                 delimiter='/',
                 *args,
                 **kwargs):
        super(ReturnS3KeySensor, self).__init__(*args, **kwargs)
        self.delimiter = delimiter

    def get_object_key(self):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_object = s3.get_wildcard_key(
            bucket_name=self.bucket_name,
            wildcard_key=self.bucket_key,
            delimiter=self.delimiter,
        )

        if s3_object is None:
            raise AttributeError(
                'file not found in {}:{}'.format(self.bucket_name, self.bucket_key)
            )

        return s3_object.key

    def execute(self, context):
        super(ReturnS3KeySensor, self).execute(context)
        return self.get_object_key()
