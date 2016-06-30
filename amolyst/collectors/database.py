# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Various collector implementations that access a database to retrieve the data
"""

from ..base import MySQLCollector


class AMOCollector(MySQLCollector):
    """
    The collector base class for all AMO related database collectors. Sets up the database
    connection using the configuration data in collectors/AMOCollector.
    """

    def read(self):
        dbconfig = self.config['collectors']['AMOCollector']
        self.read_sql(**dbconfig)


class DatabaseAddonsCollector(AMOCollector):
    """
    A collector for the ``addons`` table, collecting all possible information about addons
    """

    NAMESPACE = "amo.addon"
    CATEGORY = "basemeta"

    SQL_QUERY = """
           SELECT translations.localized_string AS name,
                  versions.version AS current_version,
                  addons.*
             FROM addons
        LEFT JOIN translations
               ON (addons.name = translations.id
                   AND translations.locale='en-us')
        LEFT JOIN versions
               ON (addons.current_version = versions.id)
    """

    def collect_entry(self, entry):
        self.field(entry['guid'], entry)


class DatabaseUsersCollector(AMOCollector):
    """
    A collector for the ``users`` table, collecting all possible information about users
    """

    NAMESPACE = "amo.user"
    CATEGORY = "basemeta"

    SQL_QUERY = "SELECT * FROM users"


class DatabaseJunctionCollector(AMOCollector):
    """
    A collector for the ``addons_users`` table, collecting relationships between addons
    and users.
    """
    NAMESPACE = "amo.addon--amo.user"

    SQL_QUERY = """
        SELECT addon_id, user_id
          FROM addons_users
         WHERE role = 5
           AND position = 0
    """

    def collect_entry(self, entry):
        self.relate(entry['addon_id'], entry['user_id'])
