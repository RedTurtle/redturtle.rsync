# -*- coding: utf-8 -*-
from datetime import datetime
from plone import api
from redturtle.rsync.interfaces import IRedturtleRsyncAdapter
from zope.component import getMultiAdapter

import argparse
import logging
import sys
import transaction


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
        self.adapter.options = self.options

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
        parser.add_argument(
            "--verbose", action="store_true", default=False, help="Verbose mode"
        )

        # logpath to write the log on Plone content
        parser.add_argument(
            "--logpath",
            default=None,
            help="Log destination path (relative to Plone site)",
        )

        # email to send the log to
        parser.add_argument(
            "--send-to-email",
            default=None,
            help="Email address to send the log to",
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

    def rsync(self):
        """
        Do the rsync.
        """
        start = datetime.now()
        logger.info(f"[{start}] - START RSYNC")
        self.adapter.setup_environment()
        data = self.adapter.get_data()
        if data:
            n_items = len(data)
            logger.info(f"START - ITERATE DATA ({n_items} items)")

            # last_commit = 0
            i = 0
            for row in data:
                i += 1
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{n_items}")

                self.adapter.create_or_update_item(row=row)

        self.adapter.delete_items(data)

        # finish, write log
        self.adapter.write_log()
        # send log by email
        self.adapter.send_log()
        end = datetime.now()
        delta = end - start
        total_seconds = int(delta.total_seconds())

        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        logger.info(
            f"[{end}] - END RSYNC (duration {hours:02d}:{minutes:02d}:{seconds:02d})"
        )


def _main(args):
    with api.env.adopt_user(username="admin"):
        runner = ScriptRunner(args=args)
        runner.rsync()
        if not getattr(runner.options, "dry_run", False):
            print(f"[{datetime.now()}] COMMIT")
            transaction.get().note(
                runner.adapter.log_item_title(start=runner.adapter.start)
            )
            transaction.commit()


def main():
    _main(sys.argv[3:])


if __name__ == "__main__":
    main()
