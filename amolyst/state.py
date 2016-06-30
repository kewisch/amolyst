# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This file contains classes related to the collector state and processor state
"""

from collections import defaultdict
from functools import partial
from pprint import pformat

from .base import Collector


class ProcessorState(object):
    """
    The marker class for the processor state. Subclasses may decide what
    storage mechanism to use, e.g. memory, disk or database.
    """
    def update(self, namespace, ident, value):
        """
        Update the entry identified by `ident` in the given processor namespace
        with the values provided.

        Args:
            namespace (string):
                The namespace entries should be updated in
            ident (string):
                The identifier for the processor entry
            value (dict):
                The values to merge in
        """
        raise NotImplementedError()

    def fieldnames(self, namespace):
        """
        Retrieve the field names for the given namespace

        Args:
            namespace (string):
                The namespace entries should be retrieved from
        """
        raise NotImplementedError()

    def items(self):
        """
        Retrieve all items in the processor state.
        """
        raise NotImplementedError()


class ProcessorMemoryState(ProcessorState):
    """
    A processor state implemented with a memory backend
    """
    def __init__(self):
        self.outdata = defaultdict(partial(defaultdict, dict))
        self.fields = defaultdict(set)

    def update(self, namespace, ident, value):
        self.outdata[namespace][ident].update(value)
        self.fields[namespace].update(value.keys())

    def fieldnames(self, namespace):
        return self.fields[namespace]

    def items(self):
        return self.outdata.iteritems()

    def _strentry(self, data):
        strres = ""
        for namespace, nsentry in data.iteritems():
            strres += "NAMESPACE {} ({})\n".format(namespace, ", ".join(self.fields[namespace]))
            for idkey, itementry in nsentry.iteritems():
                if isinstance(itementry, dict):
                    fmtentry = pformat(itementry).replace("\n", "\n\t\t\t")
                    strres += "\tENTRY({}):\n".format(idkey)
                    strres += "\t\t{}\n".format(fmtentry)
                else:
                    strres += "\tENTRY({}) = {}\n".format(idkey, itementry)
        return strres

    def __str__(self):
        return self._strentry(self.outdata)


class CollectorState(object):
    """
    The marker class for the collector state. Subclasses may decide what
    storage mechanism to use, e.g. memory, disk or database.
    """

    def field(self, namespace, category, ident, entry):
        """
        Define a field in the given namespace and category, identified by
        `ident`.

        Args:
            namespace (string):
                The namespace the field should be created in
            category (string):
                The category the field should be created in
            ident (string):
                The identifier for the field
            entry (dict)
                The value for the field
        """
        raise NotImplementedError()

    def relate(self, namespace, category, from_id, to_id):
        """
        Create a relation between `fromid` and `toid` in the given namespace
        and category. The namespace should be a combination of the namespaces
        used in each of the items and the category can be the default category.

        Args:
            namespace (string):
                The namespace for the relation
            category (string):
                The category for the relation
            from_id (string)
                The id in the left side of the relation
            to_id (string)
                The id in the right side of the relation
        """
        raise NotImplementedError()

    def items(self, namespace, category):
        """
        Iterate all items in the collector state for the given namespace and
        category.

        Args:
            namespace (string):
                The namespace to retrieve items for
            category (string):
                The category to retrieve items for
        """
        raise NotImplementedError()

    def relationitems(self, namespace):
        """
        Iterate all relations in the collector state for the given namespace

        Args:
            namespace (string):
                The namespace to retrieve items for
        """
        raise NotImplementedError()


class CollectorMemoryState(CollectorState):
    """
    A collector state implemented with a memory backend
    """

    def __init__(self):
        self.data = defaultdict(partial(defaultdict, dict))
        self.relations = defaultdict(partial(defaultdict, dict))

    def field(self, namespace, category, ident, entry):
        self.data[namespace][category][ident] = entry

    def relate(self, namespace, category, fromid, toid):
        self.relations[namespace][category][fromid] = toid

    def items(self, namespace, category):
        return self.data[namespace][category].iteritems()

    def relationitems(self, namespace):
        return self.relations[namespace][Collector.CATEGORY].iteritems()

    @staticmethod
    def _strentry(data):
        strres = ""
        for namespace, nsentry in data.iteritems():
            strres += "NAMESPACE {}\n".format(namespace)
            for category, catentry in nsentry.iteritems():
                strres += "\tCATEGORY {}\n".format(category)
                for idkey, itementry in catentry.iteritems():
                    if isinstance(itementry, dict):
                        fmtentry = pformat(itementry).replace("\n", "\n\t\t\t")
                        strres += "\t\tENTRY({}):\n".format(idkey)
                        strres += "\t\t\t{}\n".format(fmtentry)
                    else:
                        strres += "\t\tENTRY({}) = {}\n".format(idkey, itementry)
        return strres

    def __str__(self):
        return self._strentry(self.data) + self._strentry(self.relations)
