# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This is the main executable module. Currently, the only command line parameter
is the path to a config file, defaulting to config.json in the current
directory.

It shows an example on how the amolyst classes can be used in combination.
"""

import sys
import json

from .state import CollectorMemoryState, ProcessorMemoryState

from .processors import ALL_PROCESSORS
from .collectors import ALL_COLLECTORS

from .formatters import CSVFormatter


def loadconfig(configname):
    """
    Loads the JSON config file with the given path

    Args:
        configname (string):
            The filename to load
    """

    with open(configname, 'r') as cfg:
        return json.load(cfg)


def main():
    """
    The main function for this module.
    """
    config = loadconfig(sys.argv[1] if len(sys.argv) > 1 else 'config.json')

    # for each collector
    cstate = CollectorMemoryState()
    for collector_class in ALL_COLLECTORS:
        collector = collector_class(cstate, config)
        collector.read()

    print "Collector State:"
    print cstate

    # for each processor
    pstate = ProcessorMemoryState()
    for processor_class in ALL_PROCESSORS:
        processor = processor_class(cstate, pstate, config)
        processor.process()

    print "\n\nProcessor State:"
    print pstate

    # format
    formatter = CSVFormatter(pstate, config)
    formatter.write()


if __name__ == "__main__":
    main()
