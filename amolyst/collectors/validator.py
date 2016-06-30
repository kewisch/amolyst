# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
The collector implementation that reads from the addons-linter output, using JSON files
saved in a directory.

The implementation may change in the future to read from memory or an API.
"""

from ..base import JSONCollector


class ValidatorCollector(JSONCollector):
    """
    Collects validator results from json files in a preconfigured directory
    Validator message format as follows:
        {
           "summary" : {
              "errors" : 0,
              "warnings" : 0,
              "notices" : 0
           },
           "notices" : [],
           "count" : 0,
           "warnings" : [],
           "metadata" : {
              "version" : "1.0",
              "id" : "amotabclose@mozilla.kewis.ch",
              "jsLibs" : {},
              "name" : "AMO Tab Closer",
              "manifestVersion" : 2,
              "emptyFiles" : [],
              "type" : 1,
              "architecture" : "extension"
           },
           "errors" : []
        }
    """

    NAMESPACE = "amo.addon"
    CATEGORY = "validator"

    def read(self):
        basedir = self.config['collectors']['ValidatorCollector']['basedir']
        self.read_json(basedir)

    def collect_entry(self, entry):
        self.field(entry['metadata']['id'], entry)
