# -*- coding: utf-8 -*-
from plone.app.robotframework.testing import REMOTE_LIBRARY_BUNDLE_FIXTURE
from plone.app.testing import applyProfile
from plone.app.testing import FunctionalTesting
from plone.app.testing import IntegrationTesting
from plone.app.testing import PLONE_FIXTURE
from plone.app.testing import PloneSandboxLayer
from plone.testing import z2

import redturtle.rsync


class RedturtleRsyncLayer(PloneSandboxLayer):
    defaultBases = (PLONE_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        # Load any other ZCML that is required for your tests.
        # The z3c.autoinclude feature is disabled in the Plone fixture base
        # layer.
        import plone.app.dexterity

        self.loadZCML(package=plone.app.dexterity)
        import plone.restapi

        self.loadZCML(package=plone.restapi)
        self.loadZCML(package=redturtle.rsync)

    def setUpPloneSite(self, portal):
        applyProfile(portal, "redturtle.rsync:default")


REDTURTLE_RSYNC_FIXTURE = RedturtleRsyncLayer()


REDTURTLE_RSYNC_INTEGRATION_TESTING = IntegrationTesting(
    bases=(REDTURTLE_RSYNC_FIXTURE,),
    name="RedturtleRsyncLayer:IntegrationTesting",
)


REDTURTLE_RSYNC_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(REDTURTLE_RSYNC_FIXTURE,),
    name="RedturtleRsyncLayer:FunctionalTesting",
)


REDTURTLE_RSYNC_ACCEPTANCE_TESTING = FunctionalTesting(
    bases=(
        REDTURTLE_RSYNC_FIXTURE,
        REMOTE_LIBRARY_BUNDLE_FIXTURE,
        z2.ZSERVER_FIXTURE,
    ),
    name="RedturtleRsyncLayer:AcceptanceTesting",
)
