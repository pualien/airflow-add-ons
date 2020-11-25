from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airflow_add_ons.hooks.gmail_imap_hooks import GmailImapHook


class GmailImapEmailSensor(BaseSensorOperator):
    """
    Waits for a specific email on a mail server.

    :param mail_filter: Email filter to check email existence
    :param mail_folder: The mail folder in where to search for the attachment.
                        The default value is 'INBOX'.
    :type mail_folder: str
    :param conn_id: The connection to run the sensor against.
                    The default value is 'imap_default'.
    :type conn_id: str
    """

    template_fields = ('mail_filter',)

    @apply_defaults
    def __init__(self,
                 mail_filter,
                 mail_folder='"[Gmail]/All Mail"',
                 conn_id='gmail_imap',
                 *args,
                 **kwargs):
        super(GmailImapEmailSensor, self).__init__(*args, **kwargs)
        self.mail_filter = mail_filter
        self.mail_folder = mail_folder
        self.conn_id = conn_id

    def poke(self, context):
        """
        Pokes for a mail on the mail server.

        :param context: The context that is being provided when poking.
        :type context: dict
        :return: True if attachment with the given name is present and False if not.
        :rtype: bool
        """
        self.log.info('Sensor checks existence of : %s', self.mail_filter)

        gmail_imap_hook = GmailImapHook(imap_conn_id=self.conn_id)
        return gmail_imap_hook.mail_exists(
                mail_filter=self.mail_filter,
                mail_folder=self.mail_folder,
            )