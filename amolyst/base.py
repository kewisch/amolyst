# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
The base classes used in collectors, procesors and formatters.
This file should be split up once it has more subclasses
"""

import os

import fnmatch
import json
import MySQLdb


class Formatter(object):
    """
    The base class for a formatter. The formatter takes the completed processor state and
    formats it in a way that is favorable for the target of the data.

    Args:
        state (amolyst.state.ProcessorState):
            The processor state to format
        config (dict):
            The JSON configuration to use in this formatter

    Attributes:
        state (amolyst.state.ProcessorState):
            The processor state to format
        config (dict):
            The JSON configuration to use in this formatter
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, state, config):
        self.state = state
        self.config = config

    def write(self):
        """
        This method should be overwritten to write the processor state into whatever
        format the formatter pleases.
        """
        raise NotImplementedError()


class Collector(object):
    """
    The base class for an amolyst collector. Allows connecting an external source to
    amolyst, for use in the processor. Collectors should collect all possible data and not
    filter out anything, as the filtering step is meant for the Processor.

    Args:
        state (amolyst.state.CollectorState):
            The state used to collect data from the external source
        config (dict)
            The JSON configuration to use in this collector

    Attributes:
        NAMESPACE (string):
            The namespace for this collector, the type of object it describes
        CATEGORY (string):
            The category for this collector, a unique identifier for the collector within
            the namespace.
    """

    NAMESPACE = "default"
    CATEGORY = "_root"

    def __init__(self, state, config):
        self.state = state
        self.config = config

    def read(self):
        """
        Overwrite this method with code that would load from the external source and then
        call :py:meth:`collect_entry<amolyst.base.Collector.collect_entry>` for each
        entry.
        """
        raise NotImplementedError

    def collect_entry(self, entry):
        """
        Process one entry from the external source. The default implementation assumes
        there is a key ``id`` that serves as a unique key for the entry. In this method,
        use one of the definition methods like
        :py:meth:`field<amolyst.base.Collector.field>` or
        :py:meth:`relate<amolyst.base.Collector.relate>`.

        Args:
            entry (dict):
                The entry to process
        """
        self.field(entry['id'], entry)

    def field(self, ident, entry):
        """
        Define a field in the current namespace and category using the collector state

        Args:
            ident (string):
                The identifier for the field
            entry (dict)
                The value for the field
        """

        self.state.field(self.NAMESPACE, self.CATEGORY, ident, entry)

    def relate(self, from_id, to_id):
        """
        Define a relation in the current namespace and category using the collector state.

        Args:
            from_id (string)
                The id in the left side of the relation
            to_id (string)
                The id in the right side of the relation
        """
        self.state.relate(self.NAMESPACE, self.CATEGORY, from_id, to_id)


class JSONCollector(Collector):
    """
    A :py:class:`Collector<amolyst.base.Collector>` sub-class that reads from JSON files.
    Implementations can overwrite the :py:meth:`read<amolyst.base.JSONCollector.read>`
    method to determine the base path for the JSON files, then call
    :py:meth:`read_json<amolyst.base.JSONCollector.read_json>` to start processing.
    """

    def read(self):
        """
        The default implementation for reading JSON files. Uses <NAMESPACE>.<CATEGORY> as
        the base directory, or just <NAMESPACE> if the category is the default category.
        """
        if self.CATEGORY != Collector.CATEGORY:
            path = (self.NAMESPACE + "." + self.CATEGORY)
        else:
            path = self.NAMESPACE

        self.read_json(path)

    def read_json(self, basedir):
        """
        Read JSON files from the base directory and use
        :py:meth:`collect_entry<amolyst.base.Collector.collect_entry>` to process each
        file.

        Args:
            basedir (string):
                The base directory to read JSON files from
        """
        for root, _, files in os.walk(basedir):
            for item in fnmatch.filter(files, "*.json"):
                with open(os.path.join(root, item)) as jsonfile:
                    entry = json.load(jsonfile)
                    self.collect_entry(entry)


class MySQLCollector(Collector):
    """
    Collect entries from a MySQL database. This class should be subclassed to provide
    connection information from a config file or similar.

    Attributes:
        SQL_DATABASE (string):
            The database to read from. Only used with the default `read` implementation.
        SQL_QUERY (string):
            The SQL query to execute to read all entries
    """
    SQL_DATABASE = "database"
    SQL_QUERY = "SELECT * FROM table"

    def read(self):
        """
        The default read implementation, which connects with user root and no password to
        localhost:3306 via TCP, using the database defined in
        :py:attr`SQL_QUERY<amolyst.base.MySQLCollector.SQL_QUERY>`
        """
        self.read_sql(host='127.0.0.1', port=3306, user='root', db=self.SQL_DATABASE)

    def read_sql(self, **kwargs):
        """
        Read entries from the database and process each one with
        :py:meth:`collect_entry<amolyst.base.Collector.collect_entry>`.

        Args:
            **kwargs: Parameters passed directly to :py:meth:`MySQLdb.connect`.
        """
        conn = MySQLdb.connect(**kwargs)

        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        print self
        cursor.execute(self.SQL_QUERY)

        for entry in cursor.fetchall():
            self.collect_entry(entry)


class Processor(object):
    """
    The base class for a processor. A processor takes care of selecting and filtering data
    from various sources in a way that only relevant data is saved. Data is read from the
    collector state and saved in the processor state.

    Args:
        collector_state (amolyst.state.CollectorState):
            The collector state to read data from
        processor_state (amolyst.state.ProcessorState):
            The processor state to save data in
        config (dict):
            The configuration data to use while processing
    """
    def __init__(self, collector_state, processor_state, config):
        self.cstate = collector_state
        self.pstate = processor_state
        self.config = config

    def process(self):
        """
        This method should be overwritten by subclasses and call one of the processing
        methods, e.g.  :py:meth:`processfields<amolyst.base.Processor.processfields>`,
        :py:meth:`selectfields<amolyst.base.Processor.selectfields>` or
        :py:meth:`relate<amolyst.base.Processor.relate>`.
        """
        raise NotImplementedError()

    def processfields(self, namespace, category, func):
        """
        Process all fields in the category using the passed function.

        Args:
            namespace (string):
                The namespace of the fields to process
            category (string):
                The category to process
            func (function):
                The function to call for each entry in the category
        """
        for ident, entry in self.cstate.items(namespace, category):
            fields = func(entry)
            fielddata = {}
            for key, value in fields.iteritems():
                catkey = "%s.%s" % (category, key)
                fielddata[catkey] = value

            self.pstate.update(namespace, ident, fielddata)

    def selectfields(self, namespace, category, fields=None):
        """
        Select one or more fields from the given category.

        Args:
            namespace (string):
                The namespace of the fields to process
            category (string):
                The category to process
            fields (Optional[list[string]]):
                The fields to select. If not passed, all fields are selected
        """
        def limitfields(entry):
            """Returns a copy of the entry, limited to the fields"""
            return {field: entry[field] for field in fields}

        def allfields(entry):
            """Returns the entry itself, to use all fields"""
            return entry

        self.processfields(namespace, category, allfields if fields is None else limitfields)

    def relate(self, from_ns, to_ns):
        """
        Define a relation between the source and target namespace. The relation must have
        been prepared by a separate collector that defines relations between two items,
        this method merely exposes the relationship in the processor state.

        Args:
            from_ns (string):
                The namespace being put in relation
            to_ns (string):
                The namespace related to
        """
        namespace = from_ns + "--" + to_ns
        for fromdata, todata in self.cstate.relationitems(namespace):
            ident = "{}-{}".format(fromdata, todata)
            self.pstate.update(namespace, ident, {
                from_ns: fromdata,
                to_ns: todata
            })
