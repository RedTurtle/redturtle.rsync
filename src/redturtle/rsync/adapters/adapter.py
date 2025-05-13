from pathlib import Path
from redturtle.rsync.interfaces import IRedturtleRsyncAdapter
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from zope.component import adapter
from zope.interface import implementer
from zope.interface import Interface

import json
import requests


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

    def log_item_title(self, start, options):
        """
        Return the title of the log item for the rsync command.
        """
        return f"Report sync {start.strftime('%d-%m-%Y %H:%M:%S')}"

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

    def get_data(self, options):
        """
        Convert the data to be used for the rsync command.
        Return:
        - data: the data to be used for the rsync command
        - error: an error message if there was an error, None otherwise
        """
        error = None
        data = None
        # first, read source data
        if getattr(options, "source_path", None):
            file_path = Path(options.source_path)
            if file_path.exists() and file_path.is_file():
                with open(file_path, "r") as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        data = f.read()
            else:
                error = f"Source file not found in: {file_path}"
                return data, error
        elif getattr(options, "source_url", None):
            http = self.requests_retry_session(retries=7, timeout=30.0)
            response = http.get(options.source_url)
            if response.status_code != 200:
                error = f"Error getting data from {options.source_url}: {response.status_code}"
                return data, error
            if "application/json" in response.headers.get("Content-Type", ""):
                try:
                    data = response.json()
                except ValueError:
                    data = response.content
            else:
                data = response.content

        if data:
            data, error = self.convert_source_data(data)
        return data, error

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
        raise NotImplementedError()

    def create_item(self, row, options):
        """
        Create a new content item from the given row of data.
        This method should be implemented by subclasses to create the specific type of content item.
        """
        raise NotImplementedError()

    def update_item(self, item, row):
        """
        Update an existing content item from the given row of data.
        This method should be implemented by subclasses to update the specific type of content item.
        """
        raise NotImplementedError()

    def delete_items(self, data, sync_uids):
        """
        params:
        - data: the data to be used for the rsync command
        - sync_uids: the uids of the items thata has been updated

        Delete items if needed.
        This method should be implemented by subclasses to delete the specific type of content item.
        """
        return
