from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class ExtendedAwsGlueCatalogHook(AwsGlueCatalogHook):

    def get_job_run(self, job_name, job_run_id):
        return self.get_conn().get_job_run(JobName=job_name, RunId=job_run_id)


class GlueJobFlowSensor(BaseSensorOperator):
    """
    Asks for the state of the JobFlow until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_name: job name to check the state of
    :type job_name: string
    :param job_run_id: job run identifier to check the state of
    :type job_run_id: string
    """

    ui_color = '#66c3ff'

    NON_TERMINAL_STATES = ['STARTING', 'RUNNING', 'STOPPING', 'STOPPED']
    FAILED_STATE = ['FAILED', 'TIMEOUT']
    template_fields = ('job_name', 'job_run_id')
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 job_name,
                 job_run_id,
                 *args,
                 **kwargs):
        super(GlueJobFlowSensor, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.job_name = job_name
        self.job_run_id = job_run_id

    def poke(self, context):
        response = self.get_glue_job_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        self.log.info('Job flow currently %s', state)

        if state in self.NON_TERMINAL_STATES:
            return False

        if state in self.FAILED_STATE:
            raise AirflowException('Glue job failed')

        return True

    def get_glue_job_response(self):
        glue = ExtendedAwsGlueCatalogHook(aws_conn_id=self.aws_conn_id)

        self.log.info('Poking for job %s %s', self.job_name, self.job_run_id)
        return glue.get_job_run(job_name=self.job_name, job_run_id=self.job_run_id)

    @staticmethod
    def state_from_response(response):
        return response['JobRun']['JobRunState']
