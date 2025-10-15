from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from plone import api
from plone.registry.interfaces import IRegistry
from Products.CMFPlone.interfaces.controlpanel import IMailSchema
from redturtle.rsync.interfaces import IRedturtleRsyncAdapter
from redturtle.rsync.scripts.rsync import logger
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from zope.component import adapter
from zope.component import getUtility
from zope.interface import implementer
from zope.interface import Interface

import json
import re
import requests
import uuid


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super(TimeoutHTTPAdapter, self).__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super(TimeoutHTTPAdapter, self).send(request, **kwargs)


@implementer(IRedturtleRsyncAdapter)
@adapter(Interface, Interface)
class RsyncAdapterBase:
    """
    This is the base class for all rsync adapters.
    It provides a common interface for all adapters and some default
    implementations of the methods.
    Default methods works with some data in restapi-like format.
    """

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.options = None
        self.logdata = []
        self.n_updated = 0
        self.n_created = 0
        self.n_items = 0
        self.n_todelete = 0
        self.sync_uids = set()
        self.start = datetime.now()
        self.end = None
        self.send_log_template = None

    def requests_retry_session(
        self,
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 501, 502, 503, 504),
        timeout=5.0,
        session=None,
    ):
        """
        https://dev.to/ssbozy/python-requests-with-retries-4p03
        """
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        # adapter = HTTPAdapter(max_retries=retry)
        http_adapter = TimeoutHTTPAdapter(max_retries=retry, timeout=timeout)
        session.mount("http://", http_adapter)
        session.mount("https://", http_adapter)
        return session

    def log_item_title(self, start):
        """
        Return the title of the log item for the rsync command.
        """
        return f"Report sync {start.strftime('%d-%m-%Y %H:%M:%S')}"

    def autolink(self, text):
        """
        Fix links in the text.
        """
        return re.sub(
            r"(https?://\S+|/\S+)",
            r'<a href="\1">\1</a>',
            text,
            re.MULTILINE | re.DOTALL,
        )

    def get_frontend_url(self, item):
        frontend_domain = api.portal.get_registry_record(
            name="volto.frontend_domain", default=""
        )
        if not frontend_domain or frontend_domain == "https://":
            frontend_domain = "http://localhost:3000"
        if frontend_domain.endswith("/"):
            frontend_domain = frontend_domain[:-1]
        portal_url = api.portal.get().portal_url()

        return item.absolute_url().replace(portal_url, frontend_domain)

    def log_info(self, msg, type="info", force_sys_log=False):
        """
        append a message to the logdata list and print it.

        """
        style = ""
        if type == "error":
            style = "padding:2px;background-color:red;color:#fff"
        if type == "warning":
            style = "padding:2px;background-color:#ff9d00;color:#fff"
        # msg = f"[{datetime.now().strftime('%d-%m-%Y %H:%M:%S')}] {msg}"
        self.logdata.append(
            f'<p><span style="{style}">[{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}]</span>&nbsp;{self.autolink(msg)}</p>'
        )

        # print the message on standard output
        if type == "error":
            logger.error(msg)
        elif type == "warning":
            logger.warning(msg)
        else:
            if self.options.verbose or force_sys_log:
                logger.info(msg)

    def get_log_container(self):
        logpath = getattr(self.options, "logpath", None)
        if not logpath:
            logger.warning("No logpath specified, skipping log write into database.")
            return
        logcontainer = api.content.get(logpath)
        if not logcontainer:
            logger.warning(
                f'Log container not found with path "{logpath}", skipping log write into database.'
            )
            return
        return logcontainer

    def write_log(self):
        """
        Write the log into the database.
        """
        logcontainer = self.get_log_container()
        if not logcontainer:
            return
        description = f"{self.n_items} elementi trovati, {self.n_created} creati, {self.n_updated} aggiornati, {self.n_todelete} da eliminare"
        blockid = str(uuid.uuid4())
        api.content.create(
            logcontainer,
            "Document",
            title=self.log_item_title(start=self.start),
            description=description,
            blocks={
                blockid: {
                    "@type": "html",
                    "html": "\n".join(self.logdata),
                }
            },
            blocks_layout={
                "items": [blockid],
            },
        )

    def send_log(self):
        """
        Send the log by email.
        """

        send_to_email = getattr(self.options, "send_to_email", None)
        if not send_to_email:
            return
        if not self.send_log_template:
            logger.warning("No email template found, skipping log send by email.")
            return
        mailhost = api.portal.get_tool(name="MailHost")
        if not mailhost:
            logger.warning("No MailHost found, skipping log send by email.")
            return

        body_view = api.content.get_view(
            name=self.send_log_template, context=self.context, request=self.request
        )
        body = body_view(logs=self.logdata)
        encoding = api.portal.get_registry_record(
            "plone.email_charset", default="utf-8"
        )

        registry = getUtility(IRegistry)
        mail_settings = registry.forInterface(IMailSchema, prefix="plone")
        email_from_address = mail_settings.email_from_address
        email_from_name = mail_settings.email_from_name
        mfrom = formataddr((email_from_name, email_from_address))

        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = self.log_item_title(start=self.start)
        msg["From"] = mfrom
        msg["Reply-To"] = mfrom
        msg["To"] = send_to_email
        msg.replace_header("Content-Type", 'text/html; charset="utf-8"')

        mailhost.send(msg, charset=encoding)

    def set_args(self, parser):
        """
        Set some additional arguments for the rsync command.

        For example:
        parser.add_argument(
            "--import-type",
            choices=["xxx", "yyy", "zzz"],
            help="Import type",
        )
        """
        return

    def setup_environment(self):
        """ """
        return

    def get_data(self):
        """ """
        try:
            data = self.do_get_data()
        except Exception as e:
            logger.exception(e)
            msg = f"Error in data generation: {e}"
            self.log_info(msg=msg, type="error")
            return
        if not data:
            msg = "No data to sync."
            self.log_info(msg=msg, type="info", force_sys_log=True)
            return
        return data

    def convert_source_data(self, data):
        """
        If needed, convert the source data to a format that can be used by the rsync command.
        """
        return data, None

    def find_item_from_row(self, row):
        """
        Find the item in the context from the given row of data.
        This method should be implemented by subclasses to find the specific type of content item.
        """
        try:
            return self.do_find_item_from_row(row=row)
        except Exception as e:
            msg = f"[Error] Unable to find item from row {row}: {e}"
            self.log_info(msg=msg, type="error")
            return None

    def create_or_update_item(self, row):
        item = self.find_item_from_row(row=row)
        if not item:
            self.create_item(row=row)
        else:
            self.update_item(item=item, row=row)

    def create_item(self, row):
        """
        Create the item.
        """
        try:
            res = self.do_create_item(row=row)
        except Exception as e:
            msg = f"[Error] Unable to create item {row}: {e}"
            self.log_info(msg=msg, type="error")
            return
        if not res:
            msg = f"[SKIPPED] item {row} not created."
            self.log_info(msg=msg)
            return

        # adapter could create a list of items (maybe also children or related items)
        if isinstance(res, list):
            self.n_created += len(res)
            for item in res:
                msg = f"[CREATED] {'/'.join(item.getPhysicalPath())}"
                self.log_info(msg=msg)
        else:
            self.n_created += 1
            msg = f"[CREATED] {'/'.join(res.getPhysicalPath())}"
            self.log_info(msg=msg)
        return res

    def update_item(self, item, row):
        """
        Handle update of the item.
        """
        try:
            res = self.do_update_item(item=item, row=row)
        except Exception as e:
            msg = f"[Error] Unable to update item {self.get_frontend_url(item)}: {e}"
            self.log_info(msg=msg, type="error")
            return

        if not res:
            msg = f"[SKIPPED] {self.get_frontend_url(item)}"
            self.log_info(msg=msg)
            return

        if isinstance(res, list):
            self.n_updated += len(res)
            for updated in res:
                msg = f"[UPDATED] {updated.absolute_url()}"
                self.log_info(msg=msg)
                self.sync_uids.add(updated.UID())
                updated.reindexObject()
        else:
            self.n_updated += 1
            msg = f"[UPDATED] {self.get_frontend_url(item)}"
            self.log_info(msg=msg)
            self.sync_uids.add(item.UID())
            item.reindexObject()

    def delete_items(self, data):
        """
        See if there are items to delete.
        """
        res = self.do_delete_items(data=data)
        if not res:
            return
        if isinstance(res, list):
            self.n_todelete += len(res)
            for item in res:
                msg = f"[DELETED] {item}"
                self.log_info(msg=msg)
        else:
            self.n_todelete += 1
            msg = f"[DELETED] {res}"
            self.log_info(msg=msg)

    def do_get_data(self):
        """
        Convert the data to be used for the rsync command.
        Return:
        - data: the data to be used for the rsync command
        - error: an error message if there was an error, None otherwise
        """
        data = None
        # first, read source data
        if getattr(self.options, "source_path", None):
            file_path = Path(self.options.source_path)
            if file_path.exists() and file_path.is_file():
                with open(file_path, "r") as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        data = f.read()
            else:
                self.log_info(
                    msg=f"Source file not found in: {file_path}", type="warning"
                )
                return
        elif getattr(self.options, "source_url", None):
            http = self.requests_retry_session(retries=7, timeout=30.0)
            response = http.get(self.options.source_url)
            if response.status_code != 200:
                self.log_info(
                    msg=f"Error getting data from {self.options.source_url}: {response.status_code}",
                    type="warning",
                )
                return
            if "application/json" in response.headers.get("Content-Type", ""):
                try:
                    data = response.json()
                except ValueError:
                    data = response.content
            else:
                data = response.content

        return self.convert_source_data(data)

    def do_find_item_from_row(self, row):
        raise NotImplementedError()

    def do_update_item(self, item, row):
        """
        Update the item from the given row of data.
        This method should be implemented by subclasses to update the specific type of content item.
        """
        raise NotImplementedError()

    def do_create_item(self, row):
        """
        Create a new content item from the given row of data.
        This method should be implemented by subclasses to create the specific type of content item.
        """
        raise NotImplementedError()

    def do_delete_items(self, data):
        """
        Delete items
        """
        raise NotImplementedError()
