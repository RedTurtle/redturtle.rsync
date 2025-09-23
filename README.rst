.. This README is meant for consumption by humans and PyPI. PyPI can render rst files so please do not use Sphinx features.
   If you want to learn more about writing documentation, please check out: http://docs.plone.org/about/documentation_styleguide.html
   This text does not appear on PyPI or github. It is a comment.

.. image:: https://github.com/collective/redturtle.rsync/actions/workflows/plone-package.yml/badge.svg
    :target: https://github.com/collective/redturtle.rsync/actions/workflows/plone-package.yml

.. image:: https://coveralls.io/repos/github/collective/redturtle.rsync/badge.svg?branch=main
    :target: https://coveralls.io/github/collective/redturtle.rsync?branch=main
    :alt: Coveralls

.. image:: https://codecov.io/gh/collective/redturtle.rsync/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/collective/redturtle.rsync

.. image:: https://img.shields.io/pypi/v/redturtle.rsync.svg
    :target: https://pypi.python.org/pypi/redturtle.rsync/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/status/redturtle.rsync.svg
    :target: https://pypi.python.org/pypi/redturtle.rsync
    :alt: Egg Status

.. image:: https://img.shields.io/pypi/pyversions/redturtle.rsync.svg?style=plastic   :alt: Supported - Python Versions

.. image:: https://img.shields.io/pypi/l/redturtle.rsync.svg
    :target: https://pypi.python.org/pypi/redturtle.rsync/
    :alt: License


===============
redturtle.rsync
===============

This package provides sync features to massive upload contents in a Plone site.

It generates a script in bin/redturtle_rsync that accept the following parameters:

    - `--dry-run`: Dry-run mode (default is False)
    - `--verbose`: Verbose mode (default is False)
    - `--logpath LOGPATH`: Log destination path (relative to Plone site)
    - `--send-to-email SEND_TO_EMAIL`: Email address to send the log to
    - `--source-path SOURCE_PATH`: Local data source path (complementary to source-url)
    - `--source-url SOURCE_URL`: Remote data source URL (complementary to source-path)

Example::

    ./bin/instance -OPlone run bin/redturtle_rsync --logpath /Plone/it/test-sync/log-sync --source-path /opt/some-data


Features
--------

This package provides a general tool to sync data and upload them in a Plone site.

You need to create an add-on to manage your specific data structure and content types that creates an adapter for IRedturtleRsyncAdapter.

It is also possible to add additional script options.

Installation
------------

Install redturtle.rsync by adding it to your buildout::

    [buildout]

    ...

    eggs =
        redturtle.rsync


and then running ``bin/buildout``


Authors
-------

This product was developed by **RedTurtle Technology** team.

.. image:: https://avatars1.githubusercontent.com/u/1087171?s=100&v=4
   :alt: RedTurtle Technology Site
   :target: http://www.redturtle.it/


Contribute
----------

- Issue Tracker: https://github.com/RedTurtle/redturtle.rsync/issues
- Source Code: https://github.com/RedTurtle/redturtle.rsync



License
-------

The project is licensed under the GPLv2.
