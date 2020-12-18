import tempfile

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook as GoogleCloudStorageHook
except Exception:
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class EmptyFileToGoogleCloudStorageOperator(BaseOperator):
    """
    Uploads a file to Google Cloud Storage.
    Optionally can compress the file for upload.

    :param src: Suffix of tempfile, not required. (templated)
    :type src: str
    :param dst: Destination path within the specified bucket. (templated)
    :type dst: str
    :param bucket: The bucket to upload to. (templated)
    :type bucket: str
    :param google_cloud_storage_conn_id: The Airflow connection ID to upload with
    :type google_cloud_storage_conn_id: str
    :param mime_type: The mime-type string
    :type mime_type: str
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :type gzip: bool
    """
    template_fields = ('src', 'dst', 'bucket')

    @apply_defaults
    def __init__(self,
                 dst,
                 bucket,
                 src='',
                 google_cloud_storage_conn_id='google_cloud_default',
                 mime_type='application/octet-stream',
                 delegate_to=None,
                 gzip=False,
                 *args,
                 **kwargs):
        super(EmptyFileToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.src = src
        self.dst = dst
        self.bucket = bucket
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip

    def execute(self, context):
        """
        Uploads the file to Google cloud storage
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        with tempfile.NamedTemporaryFile('w', suffix=self.src) as temp:
            hook.upload(
                bucket_name=self.bucket,
                object_name=self.dst,
                mime_type=self.mime_type,
                filename=temp.name,
                gzip=self.gzip,
            )
            temp.flush()
