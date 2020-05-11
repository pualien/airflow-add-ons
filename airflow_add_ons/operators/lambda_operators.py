from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from botocore.config import Config

import boto3
import json
import base64


class ExecuteLambdaOperator(BaseOperator):
    template_fields = ('additional_payload',)

    @apply_defaults
    def __init__(
        self,
        lambda_function_name,
        airflow_context_to_lambda_payload=None,
        additional_payload=None,
        aws_conn_id='aws_default',
        read_timeout=2000,
        *args,
        **kwargs
    ):
        """
        Trigger AWS Lambda function
        :param lambda_function_name: name of Lambda function
        :param airflow_context_to_lambda_payload: function extracting fields from Airflow context to Lambda payload
        :param additional_payload: additional parameters for Lambda payload
        :param aws_conn_id: aws connection id in order to call Lambda function
        :param read_timeout: read time in order to wait Resource response before closing connection
        :param args:
        :param kwargs:
        """
        super(ExecuteLambdaOperator, self).__init__(*args, **kwargs)
        if additional_payload is None:
            additional_payload = {}
        self.airflow_context_to_lambda_payload = airflow_context_to_lambda_payload

        self.additional_payload = additional_payload
        self.lambda_function_name = lambda_function_name
        if read_timeout is not None:
            print('check read_timeout')
            print(read_timeout)
            config = Config(read_timeout=read_timeout, retries={'max_attempts': 0})
        else:
            config = Config(retries={'max_attempts': 0})
        if aws_conn_id is not None:
            connection = BaseHook.get_connection(aws_conn_id)
            self.lambda_client = boto3.client('lambda', aws_access_key_id=connection.login,
                                              aws_secret_access_key=connection.password,
                                              config=config,
                                              region_name=connection.extra_dejson.get('region_name'))
        else:
            raise AttributeError('Please pass a valid aws_connection_id')

    def execute(self, context):
        request_payload = self.__create_lambda_payload(context)

        print('Executing AWS Lambda {} with payload {}'.format(self.lambda_function_name, request_payload))

        response = self.lambda_client.invoke(
            FunctionName=self.lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(request_payload),
            LogType='Tail'
        )

        response_log_tail = base64.b64decode(response.get('LogResult'))
        response_payload = json.loads(response.get('Payload').read())
        response_code = response.get('StatusCode')

        log_msg_logs = 'Tail of logs from AWS Lambda:\n{logs}'.format(logs=response_log_tail)
        log_msg_payload = 'Response payload from AWS Lambda:\n{resp}'.format(resp=response_payload)

        if response_code == 200:
            print(log_msg_logs)
            print(log_msg_payload)
            return response_code
        else:
            print(log_msg_logs)
            print(log_msg_payload)
            raise AirflowException('Lambda invoke failed')

    def __create_lambda_payload(self, context):
        payload = self.airflow_context_to_lambda_payload(
            context) if self.airflow_context_to_lambda_payload is not None else {}
        payload.update(self.additional_payload)
        print('payload: {}'.format(payload))
        return payload
