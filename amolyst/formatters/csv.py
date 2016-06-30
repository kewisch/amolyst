# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
A formatter that writes to CSV files, one per namespace.
"""

from __future__ import absolute_import

import os
import csv

from ..base import Formatter


class CSVFormatter(Formatter):
    """
    A formatter that outputs all processed fields into CSV files. Each namespace gets its
    separate CSV file.
    """
    # pylint: disable=too-few-public-methods

    def write(self):
        for namespace, data in self.state.items():
            self._write_namespace_file(namespace, self.state.fieldnames(namespace), data)

    def _write_namespace_file(self, name, fieldnames, data):
        basedir = self.config['formatters']['CSVFormatter']['basedir']
        with open(os.path.join(basedir, name + '.csv'), 'w') as csvfile:
            self._write_namespace_handle(csvfile, fieldnames, data)

    @staticmethod
    def _write_namespace_handle(csvfile, fieldnames, data):
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldnames))
        writer.writeheader()

        for _, entry in data.iteritems():
            writer.writerow(entry)
