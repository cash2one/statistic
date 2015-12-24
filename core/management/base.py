# coding=utf-8
"""
Base classes for writing management commands 

"""
import os
import sys
import inspect

import core


class BaseCommand(object):
    """
    """

    def __init__(self):
        pass

    def set_log(self, *args):
        pass

    def get_version(self):
        return core.get_version()

    def print_help(self, prog_name, subcommand):
        """
        Print the help message for this command
        handle's args and __doc__
        """
        args = inspect.getargspec(self.handle).args[1:]

        usage = [
            u"Usage: %s %s %s" % (prog_name, subcommand, " ".join(args)),
        ]
        help = self.handle.__doc__
        usage.append(help)
        usage = "\n".join(usage)

        sys.stdout.write((usage + '\n').encode("utf-8"))

    def assert_argv(self, *args):
        pass

    def run_from_argv(self, argv):
        self.assert_argv(*argv)
        args = argv[2:]
        self.handle(*args)

    def handle(self, *args):
        """
        """
        raise NotImplementedError()

