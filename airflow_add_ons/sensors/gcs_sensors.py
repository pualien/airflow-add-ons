from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageObjectKeySensor(GoogleCloudStorageObjectSensor):
    """
    Checks for the existence of a file in Google Cloud Storage.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: str
    :param object: The name of the object to check in the Google cloud
        storage bucket.
    :type object: str
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('bucket', 'object')

    ui_color = '#f0eee4'


    @apply_defaults
    def __init__(self,
                 bucket,
                 object,  # pylint:disable=redefined-builtin
                 google_cloud_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):

        super(GoogleCloudStorageObjectKeySensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return hook.exists(self.bucket, self.object)

    def get_object_key(self):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        client = hook.get_conn()
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(blob_name=self.object)
        return blob.path()

    def execute(self, context):
        super(GoogleCloudStorageObjectKeySensor, self).execute(context)
        return self.get_object_key()