from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from botocore.config import Config

import boto3


class AthenaQueryOperator(BaseOperator):
    template_fields = ('query',)

    @apply_defaults
    def __init__(
        self,
        query,
        output_location,
        aws_conn_id='aws_default',
        read_timeout=2000,
        workgroup='primary',
        *args,
        **kwargs
    ):
        """
        Trigger AWS Athena query
        :param query: query to be executed from athena
        :param read_timeout: read time in order to wait Resource response before closing connection
        :param args:
        :param kwargs:
        """
        super(AthenaQueryOperator, self).__init__(*args, **kwargs)

        self.query = query
        self.output_location = output_location
        self.workgroup = workgroup
        if read_timeout is not None:
            print('check read_timeout')
            print(read_timeout)
            config = Config(read_timeout=read_timeout, retries={'max_attempts': 0})
        else:
            config = Config(retries={'max_attempts': 0})
        if aws_conn_id is not None:
            connection = BaseHook.get_connection(aws_conn_id)
            self.client = boto3.client('athena', aws_access_key_id=connection.login,
                                       aws_secret_access_key=connection.password,
                                       config=config,
                                       region_name=connection.extra_dejson.get('region_name'))
        else:
            raise AttributeError('Please pass a valid aws_connection_id')

    def execute(self, context):

        print('Executing AWS Athena query {} with workgroup {}'.format(self.query, self.workgroup))

        response = self.client.start_query_execution(
            QueryString=self.query,
            ResultConfiguration={
                'OutputLocation': self.output_location,
                'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}

            },
            WorkGroup=self.workgroup
        )

        print('Query execution id {}'.format(response.get('QueryExecutionId')))

        return response.get('QueryExecutionId')
