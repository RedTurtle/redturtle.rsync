# -*- coding: utf-8 -*-
from datetime import datetime
from plone import api
from redturtle.rsync.interfaces import IRedturtleRsyncAdapter
from zope.component import getMultiAdapter

import argparse
import logging
import re
import sys
import transaction
import uuid


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ScriptRunner:
    """
    Run the script.
    """

    def __init__(self, args):
        portal = api.portal.get()
        self.adapter = getMultiAdapter((portal, portal.REQUEST), IRedturtleRsyncAdapter)
        self.options = self.get_args(args=args)
        self.logdata = []
        self.n_items = 0
        self.n_created = 0
        self.n_updated = 0
        self.n_todelete = 0
        self.sync_uids = set()
        self.start = None
        self.end = None

    def get_args(self, args):
        """
        Get the parameters from the command line arguments.
        """
        # first, set the default values
        parser = argparse.ArgumentParser()

        # dry-run mode
        parser.add_argument(
            "--dry-run", action="store_true", default=False, help="Dry-run mode"
        )

        # verbose mode
        parser.add_argument("--verbose", default=False, help="Verbose mode")

        # logpath to write the log on Plone content
        parser.add_argument(
            "--logpath",
            default=None,
            help="Log destination path (relative to Plone site)",
        )

        # set data source
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--source-path", help="Local source path")
        group.add_argument("--source-url", help="Remote source URL")

        # then get from the adapter
        self.adapter.set_args(parser)

        # Parsing degli argomenti
        options = parser.parse_args(args)
        return options

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

    def log_info(self, msg, type="info"):
        """
        append a message to the logdata list and print it.
        """
        style = ""
        if type == "error":
            style = "padding:5px;background-color:red;color:#fff"
        msg = f"[{datetime.now()}] {msg}"
        self.logdata.append(f'<p style="{style}">{self.autolink(msg)}</p>')

        # print the message
        if type == "error":
            logger.error(msg)
        elif type == "warning":
            logger.warning(msg)
        else:
            if self.options.verbose:
                logger.info(msg)

    def write_log(self):
        """
        Write the log into the database.
        """
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
        description = f"{self.n_items} elementi trovati, {self.n_created} creati, {self.n_updated} aggiornati, {self.n_todelete} da eliminare"
        blockid = str(uuid.uuid4())
        api.content.create(
            logcontainer,
            "Document",
            title=self.adapter.log_item_title(start=self.start, options=self.options),
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

    def get_data(self):
        """
        get the data from the adapter.

        The adapter should return:
        - data: the data to be used for the rsync command
        - error: an error message if there was an error in the data generation
        """
        try:
            data, error = self.adapter.get_data(options=self.options)
        except Exception as e:
            msg = f"Error in data generation: {e}"
            self.log_info(msg=msg, type="error")
            return None
        if error:
            msg = f"Error in data generation: {error}"
            self.log_info(msg=msg, type="error")
            return None
        if not data:
            msg = "No data to sync."
            self.log_info(msg=msg, type="error")
            return None
        return data

    def create_item(self, row, options):
        """
        Create the item.
        """
        try:
            res = self.adapter.create_item(row=row, options=self.options)
        except Exception as e:
            msg = f"[Error] Unable to create item {row}: {e}"
            self.log_info(msg=msg, type="error")
            return
        if not res:
            msg = f"[Error] item {row} not created."
            self.log_info(msg=msg, type="error")
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

    def update_item(self, item, row, options):
        """
        Update the item.
        """
        try:
            res = self.adapter.update_item(item=item, row=row, options=options)
        except Exception as e:
            msg = f"[Error] Unable to update item {self.get_frontend_url(item)}: {e}"
            self.log_info(msg=msg, type="error")
            return

        if not res:
            msg = f"[SKIPPED] {self.get_frontend_url(item)}"
            self.log_info(msg=msg)
            return

        # adapter could create a list of items (maybe also children or related items)
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
        res = self.adapter.delete_items(data=data, sync_uids=self.sync_uids)
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

    def rsync(self):
        """
        Do the rsync.
        """
        self.start = datetime.now()
        logger.info(f"[{self.start}] - START RSYNC")
        data = self.get_data()
        if not data:
            # we already logged the error
            logger.info(f"[{datetime.now()}] - END RSYNC")
            return

        self.n_items = len(data)
        self.log_info(msg=f"START - ITERATE DATA ({self.n_items} items)")

        # last_commit = 0
        i = 0
        for row in data:
            i += 1
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{self.n_items}")
            try:
                item = self.adapter.find_item_from_row(row=row, options=self.options)
            except Exception as e:
                msg = f"[Error] Unable to find item from row {row}: {e}"
                self.log_info(msg=msg, type="error")
                continue
            if not item:
                self.create_item(row=row, options=self.options)
            else:
                self.update_item(item=item, row=row, options=self.options)

            # if self.n_updated + self.n_created - last_commit > 5:
            #     last_commit = self.n_updated + self.n_created
            #     if not getattr(self.options, "dry_run", False):
            #         logger.info(
            #             f"[{datetime.now()}] COMMIT ({i}/{self.n_items} items processed)"
            #         )
            #         transaction.commit()

        self.delete_items(data)


def _main(args):
    with api.env.adopt_user(username="admin"):
        runner = ScriptRunner(args=args)
        runner.rsync()
        runner.write_log()
        if not getattr(runner.options, "dry_run", False):
            print(f"[{datetime.now()}] COMMIT")
            transaction.commit()


def main():
    _main(sys.argv[3:])


if __name__ == "__main__":
    main()
