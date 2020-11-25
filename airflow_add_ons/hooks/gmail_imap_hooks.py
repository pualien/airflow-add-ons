from imapclient import imapclient
from airflow.hooks.base_hook import BaseHook


class GmailImapHook(BaseHook):
    """
    This hook connects to a mail server by using the imap protocol.

    .. note:: Please call this Hook as context manager via `with`
        to automatically open and close the connection to the mail server.

    :param imap_conn_id: The connection id that contains the information used to authenticate the client.
    :type imap_conn_id: str
    """

    def __init__(self, source=None, imap_conn_id='gmail_imap'):
        super().__init__(source)
        self.imap_conn_id = imap_conn_id
        self.mail_client = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Logout and closes the connection when exiting the context manager.

        All exceptions during logout and connection shutdown are caught because
        an error here usually means the connection was already closed.
        """
        try:
            self.mail_client.logout()
        except Exception:
            try:
                self.mail_client.shutdown()
            except Exception as e:
                print("Could not close the connection cleanly: %s", e)
        self.mail_client = None

    def get_conn(self):
        """
        Login to the mail server.

        .. note:: Please call this Hook as context manager via `with`
            to automatically open and close the connection to the mail server.

        :return: an authorized ImapHook object.
        :rtype: ImapHook
        """

        if not self.mail_client:
            conn = self.get_connection(self.imap_conn_id)
            self.mail_client = imapclient.IMAPClient(conn.host)
            self.mail_client.login(conn.login, conn.password)

        return self

    def mail_exists(self, mail_folder='INBOX', mail_filter='', latest_only=True):
        """
        Checks the mail folder for mails containing attachments with the given name.

        :param mail_folder: The mail folder where to look at.
        :type mail_folder: str
        :param mail_filter: If set other than '' only specific mails will be checked.
        :type mail_filter: str
        :returns: True if there is an attachment with the given name and False if not.
        :rtype: bool
        """
        self.get_conn()
        mails = self._retrieve_mails(
            latest_only=latest_only,
            mail_folder=mail_folder,
            mail_filter=mail_filter)
        return len(mails) > 0

    def _retrieve_mails(self, latest_only, mail_folder, mail_filter):
        mails = []

        self.mail_client.select_folder(mail_folder)
        for mail in self.mail_client.gmail_search(mail_filter, 'UTF-8'):
            mails.append(mail)
            if latest_only:
                break
        return mails