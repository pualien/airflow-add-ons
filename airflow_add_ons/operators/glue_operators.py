from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from botocore.config import Config

import boto3


class GlueCrawlerOperator(BaseOperator):
    template_fields = ('glue_crawler_name',)

    @apply_defaults
    def __init__(
        self,
        glue_crawler_name,
        aws_conn_id='aws_default',
        read_timeout=2000,
        *args,
        **kwargs
    ):
        """
        Trigger AWS Glue crawler
        :param glue_crawler_name: name of Glue crawler
        :param read_timeout: read time in order to wait Resource response before closing connection
        :param args:
        :param kwargs:
        """
        super(GlueCrawlerOperator, self).__init__(*args, **kwargs)

        self.glue_crawler_name = glue_crawler_name
        if read_timeout is not None:
            print('check read_timeout')
            print(read_timeout)
            config = Config(read_timeout=read_timeout, retries={'max_attempts': 0})
        else:
            config = Config(retries={'max_attempts': 0})
        if aws_conn_id is not None:
            connection = BaseHook.get_connection(aws_conn_id)
            self.client = boto3.client('glue', aws_access_key_id=connection.login,
                                       aws_secret_access_key=connection.password,
                                       config=config,
                                       region_name=connection.extra_dejson.get('region_name'))
        else:
            raise AttributeError('Please pass a valid aws_connection_id')

    def execute(self, context):

        print('Executing AWS Glue Crawler {}'.format(self.glue_crawler_name))

        response = self.client.start_crawler(Name=self.glue_crawler_name)

        return response


from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
import os.path
import time


class AwsGlueJobHook(AwsHook):
    """
    Interact with AWS Glue - create job, trigger, crawler
    :param job_name: unique job name per AWS account
    :type str
    :param desc: job description
    :type str
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :type int
    :param script_location: path to etl script either on s3 or local
    :type str
    :param conns: A list of connections used by the job
    :type list
    :param retry_limit: Maximum number of times to retry this job if it fails
    :type int
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job
    :type int
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
    :type str
    :param iam_role_name: AWS IAM Role for Glue Job
    :type str
    """

    def __init__(self,
                 job_name=None,
                 desc=None,
                 concurrent_run_limit=None,
                 script_location=None,
                 conns=None,
                 retry_limit=None,
                 num_of_dpus=None,
                 aws_conn_id='aws_default',
                 region_name=None,
                 iam_role_name=None,
                 worker_type='Standard',
                 num_workers=1,
                 tags=None,
                 default_args=None,
                 s3_bucket=None,
                 overwrite_job=True, *args, **kwargs):
        self.job_name = job_name
        self.desc = desc
        self.concurrent_run_limit = concurrent_run_limit or 1
        self.script_location = script_location
        self.conns = conns or ["s3"]
        self.retry_limit = retry_limit or 0
        self.num_of_dpus = num_of_dpus or 10
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.role_name = iam_role_name
        self.worker_type = worker_type
        self.num_workers = num_workers
        self.tags = tags
        self.default_args = default_args
        self.overwrite_job = overwrite_job
        self.S3_PROTOCOL = "s3://"
        self.S3_ARTIFACTS_PREFIX = 'artifacts/glue-scripts/'
        self.S3_GLUE_LOGS = 'logs/glue-logs/'
        super(AwsGlueJobHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        conn = self.get_client_type('glue', self.region_name)
        return conn

    def list_jobs(self):
        conn = self.get_conn()
        return conn.get_jobs()

    def get_iam_execution_role(self):
        """
        :return: iam role for job execution
        """
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
            self.log.info("Iam Role Name: {}".format(self.role_name))
            return glue_execution_role
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def initialize_job(self, script_arguments=None):
        """
        Initializes connection with AWS Glue
        to run job
        :return:
        """
        if self.s3_bucket is None:
            raise AirflowException(
                'Could not initialize glue job, '
                'error: Specify Parameter `s3_bucket`'
            )

        glue_client = self.get_conn()

        try:
            job_response = self.get_or_create_glue_job()
            job_name = job_response['Name']
            job_run = glue_client.start_job_run(
                JobName=job_name,
                Arguments=self.default_args
            )
            return job_run['JobRunId']
            # return self.job_completion(job_name, job_run['JobRunId'])
        except Exception as general_error:
            raise AirflowException(
                'Failed to run aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def job_completion(self, job_name=None, run_id=None):
        """
        :param job_name:
        :param run_id:
        :return:
        """
        glue_client = self.get_conn()
        job_status = glue_client.get_job_run(
            JobName=job_name,
            RunId=run_id,
            PredecessorsIncluded=True
        )
        job_run_state = job_status['JobRun']['JobRunState']
        failed = job_run_state == 'FAILED'
        stopped = job_run_state == 'STOPPED'
        completed = job_run_state == 'SUCCEEDED'

        while True:
            if failed or stopped or completed:
                self.log.info("Exiting Job {} Run State: {}"
                              .format(run_id, job_run_state))
                return {'JobRunState': job_run_state, 'JobRunId': run_id}
            else:
                self.log.info("Polling for AWS Glue Job {} current run state"
                              .format(job_name))
                time.sleep(6)

    def get_or_create_glue_job(self):
        glue_client = self.get_conn()
        try:
            self.log.info("Now creating and running AWS Glue Job")
            s3_log_path = "s3://{bucket_name}/{logs_path}{job_name}" \
                .format(bucket_name=self.s3_bucket,
                        logs_path=self.S3_GLUE_LOGS,
                        job_name=self.job_name)

            execution_role = self.get_iam_execution_role()
            script_location = self._check_script_location()
            if self.overwrite_job:
                try:
                    len(glue_client.get_job(JobName=self.job_name).get('Job'))
                except Exception as e:
                    if 'EntityNotFoundException' in str(e) or 'IdempotentParameterMismatchException' in str(e):
                        self.log.info('Deleting {}'.format(self.job_name))
                        glue_client.delete_job(JobName=self.job_name)
                    else:
                        raise AirflowException(
                            'Failed to get aws glue job, error: {error}'.format(
                                error=str(e)
                            )
                        )

            params = {
                'Description': self.desc,
                'LogUri': s3_log_path,
                'Role': execution_role['Role']['RoleName'],
                'ExecutionProperty': {"MaxConcurrentRuns": self.concurrent_run_limit},
                'Command': {"Name": "glueetl", "ScriptLocation": script_location},
                'WorkerType': self.worker_type,
                'NumberOfWorkers': self.num_workers,
                'MaxRetries': self.retry_limit,
                # AllocatedCapacity=self.num_of_dpus, # TODO: handle AllocatedCapacity with NumberOfWorkers
                'Tags': self.tags,
                # 'GlueVersion': '1.0'
                'DefaultArguments': self.default_args,
            }
            create_job_response = glue_client.create_job(
                Name=self.job_name,
                **params
            )
            # print(create_job_response)
            return create_job_response
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue job, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def _check_script_location(self):
        """
        :return: S3 Script location path
        """
        if self.script_location[:5] == self.S3_PROTOCOL:
            return self.script_location
        elif os.path.isfile(self.script_location):
            s3 = self.get_resource_type('s3', self.region_name)
            script_name = os.path.basename(self.script_location)
            s3.meta.client.upload_file(self.script_location,
                                       self.s3_bucket,
                                       self.S3_ARTIFACTS_PREFIX + script_name)

            s3_script_path = "s3://{s3_bucket}/{prefix}{job_name}/{script_name}" \
                .format(s3_bucket=self.s3_bucket,
                        prefix=self.S3_ARTIFACTS_PREFIX,
                        job_name=self.job_name,
                        script_name=script_name)
            return s3_script_path
        else:
            return None


class AWSGlueJobOperator(BaseOperator):
    """
    Creates an AWS Glue Job. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala
    :param job_name: unique job name per AWS Account
    :type str
    :param script_location: location of ETL script. Must be a local or S3 path
    :type str
    :param job_desc: job description details
    :type str
    :param concurrent_run_limit: The maximum number of concurrent runs allowed for a job
    :type int
    :param script_args: etl script arguments and AWS Glue arguments
    :type dict
    :param connections: AWS Glue connections to be used by the job.
    :type list
    :param retry_limit: The maximum number of times to retry this job if it fails
    :type int
    :param num_of_dpus: Number of AWS Glue DPUs to allocate to this Job.
    :type int
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param s3_bucket: S3 bucket where logs and local etl script will be uploaded
    :type str
    :param iam_role_name: AWS IAM Role for Glue Job Execution
    :type str
    """
    template_fields = ('script_args', 'script_location', 's3_bucket', 'iam_role_name',)
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 job_name='aws_glue_default_job',
                 job_desc='AWS Glue Job with Airflow',
                 script_location=None,
                 concurrent_run_limit=None,
                 script_args={},
                 connections=[],
                 retry_limit=None,
                 num_of_dpus=3,
                 aws_conn_id='aws_default',
                 region_name=None,
                 s3_bucket=None,
                 iam_role_name=None,
                 worker_type='Standard',  # | 'G.1X' | 'G.2X'
                 num_workers=2,
                 tags=None,
                 *args, **kwargs
                 ):
        super(AWSGlueJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.job_desc = job_desc
        self.script_location = script_location
        self.concurrent_run_limit = concurrent_run_limit
        self.script_args = script_args
        self.connections = connections
        self.retry_limit = retry_limit
        self.num_of_dpus = num_of_dpus
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.iam_role_name = iam_role_name
        self.worker_type = worker_type
        self.num_workers = num_workers
        self.tags = tags

    def execute(self, context):
        """
        Executes AWS Glue Job from Airflow
        :return:
        """
        glue_job = AwsGlueJobHook(job_name=self.job_name,
                                  desc=self.job_desc,
                                  concurrent_run_limit=self.concurrent_run_limit,
                                  script_location=self.script_location,
                                  conns=self.connections,
                                  retry_limit=self.retry_limit,
                                  num_of_dpus=self.num_of_dpus,
                                  aws_conn_id=self.aws_conn_id,
                                  region_name=self.region_name,
                                  s3_bucket=self.s3_bucket,
                                  iam_role_name=self.iam_role_name,
                                  worker_type=self.worker_type,
                                  num_workers=self.num_workers,
                                  default_args=self.script_args,
                                  tags=self.tags)

        self.log.info("Initializing AWS Glue Job: {}".format(self.job_name))
        job_run_id = glue_job.initialize_job(None)
        return job_run_id
