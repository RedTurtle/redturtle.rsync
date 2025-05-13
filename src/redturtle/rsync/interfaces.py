# -*- coding: utf-8 -*-
"""Module where all interfaces, events and exceptions live."""
from zope.interface import Interface
from zope.publisher.interfaces.browser import IDefaultBrowserLayer


class IRedturtleRsyncLayer(IDefaultBrowserLayer):
    """Marker interface that defines a browser layer."""


class IRedturtleRsyncAdapter(Interface):
    """Marker interface for the redturtle rsync adapter."""

    def __init__(context, request):
        """Initialize the adapter with the given context and request."""

    def log_item_title(start, options):
        """
        Return the title of the log item for the rsync command.
        """

    def set_args(parser):
        """
        Set some additional arguments for the rsync command.
        """

    def get_data(options):
        """
        Set some additional arguments for the rsync command.
        """

    def handle_row(row):
        """
        Method to handle a row of data.
        For example it could do the following steps:
        - check if there is already a content item with the same id
        - if not, create a new content item
        - if yes, update the existing content item

        It should return the content item created or updated and the status of the operation.
        The status could be one of the following:
        - "created": a new content item was created
        - "updated": an existing content item was updated
        - "skipped": the content item was skipped because it already exists and is up to date
        - "error": an error occurred while processing the content item

        for example:
        return {'item': content_item, 'status': status}
        """

    def create_item(row):
        """
        Create a new content item from the given row of data.
        This method should be implemented by subclasses to create the specific type of content item.
        """

    def update_item(item, row):
        """
        Update an existing content item from the given row of data.
        This method should be implemented by subclasses to update the specific type of content item.
        """

    def delete_items(data, sync_uids):
        """
        params:
        - data: the data to be used for the rsync command
        - sync_uids: the uids of the items thata has been updated

        Delete items if needed.
        This method should be implemented by subclasses to delete the specific type of content item.
        """
