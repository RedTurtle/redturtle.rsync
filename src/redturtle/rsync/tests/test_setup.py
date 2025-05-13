# -*- coding: utf-8 -*-
"""Setup tests for this package."""
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from redturtle.rsync.testing import REDTURTLE_RSYNC_INTEGRATION_TESTING  # noqa: E501

import unittest


try:
    from Products.CMFPlone.utils import get_installer
except ImportError:
    get_installer = None


class TestSetup(unittest.TestCase):
    """Test that redturtle.rsync is properly installed."""

    layer = REDTURTLE_RSYNC_INTEGRATION_TESTING

    def setUp(self):
        """Custom shared utility setup for tests."""
        self.portal = self.layer["portal"]
        if get_installer:
            self.installer = get_installer(self.portal, self.layer["request"])
        else:
            self.installer = api.portal.get_tool("portal_quickinstaller")

    def test_product_installed(self):
        """Test if redturtle.rsync is installed."""
        self.assertTrue(self.installer.is_product_installed("redturtle.rsync"))

    def test_browserlayer(self):
        """Test that IRedturtleRsyncLayer is registered."""
        from plone.browserlayer import utils
        from redturtle.rsync.interfaces import IRedturtleRsyncLayer

        self.assertIn(IRedturtleRsyncLayer, utils.registered_layers())


class TestUninstall(unittest.TestCase):
    layer = REDTURTLE_RSYNC_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        if get_installer:
            self.installer = get_installer(self.portal, self.layer["request"])
        else:
            self.installer = api.portal.get_tool("portal_quickinstaller")
        roles_before = api.user.get_roles(TEST_USER_ID)
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.installer.uninstall_product("redturtle.rsync")
        setRoles(self.portal, TEST_USER_ID, roles_before)

    def test_product_uninstalled(self):
        """Test if redturtle.rsync is cleanly uninstalled."""
        self.assertFalse(self.installer.is_product_installed("redturtle.rsync"))

    def test_browserlayer_removed(self):
        """Test that IRedturtleRsyncLayer is removed."""
        from plone.browserlayer import utils
        from redturtle.rsync.interfaces import IRedturtleRsyncLayer

        self.assertNotIn(IRedturtleRsyncLayer, utils.registered_layers())
