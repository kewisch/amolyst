# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This file contains processors related to addons.mozilla.org
"""

from ..base import Processor


class AMOProcessor(Processor):
    """
    A processor that filters and selects data related to AMO addons and users
    """

    def process(self):
        def validator(entry):
            """Processes the validator category"""
            libs = set([
                warning['detail']
                for warning in entry['warnings']
                if warning['code'] == "FOUND_REQUIRE"
            ])

            return {
                "errors": entry['summary']['errors'],
                "warnings": entry['summary']['warnings'],
                "notices": entry['summary']['notices'],
                "requires": ";".join(libs)
            }

        self.processfields('amo.addon', 'validator', validator)

        self.selectfields('amo.addon', 'basemeta',
                          ['id', 'guid', 'name', 'current_version'])

        self.selectfields('amo.user', 'basemeta',
                          ['display_name', 'email', 'is_verified', 'lang',
                           'location', 'region', 'username'])

        self.relate("amo.addon", "amo.user")
