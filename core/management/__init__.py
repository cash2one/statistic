# coding=utf-8
import os
import sys

u"""
    提供命令行功能
"""
# from django.core.management.base import BaseCommand, CommandError, handle_default_options
# from django.core.management.color import color_style
from importlib import import_module

# For backwards compatibility: get_version() used to be in this module.
from core import get_version
from conf import conf


def get_commands():
    u"""
    从core/commands目录下读取所有的命令
    :return:
    """
    core_dir = os.path.join(conf.BASE_DIR, 'core')
    command_dir = os.path.join(core_dir, 'commands')
    try:
        return [f[:-3] for f in os.listdir(command_dir)
                if not f.startswith('_') and f.endswith('.py')]
    except OSError:
        return []


def load_command_class(name):
    u"""
    从commands中读取Command类
    :param name:
    :return:
    """
    module = import_module('core.commands.%s' % (name, ))
    return module.Command()


class ManagementUtility(object):
    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])

    def main_help_text(self):
        """
        Returns the script's main help text, as a string.
        """
        names = get_commands()
        names = ["\t" + name for name in names]

        usage = [
            "Usage: %s subcommand [args]" % self.prog_name,
            "Options:",
            "\t--version\t\tshow program's version number and exit",
            "\t--help   \t\tshow this help message and exit",
            "",
            "Type '%s help <subcommand>' for help on a specific subcommand." % self.prog_name,
            "",
            "Available subcommands:",
        ]
        usage.extend(names)

        return '\n'.join(usage)

    def fetch_command(self, subcommand):
        """
        Tries to fetch the given subcommand, printing a message with the
        appropriate command called from the command line (usually
        "django-admin.py" or "manage.py") if it can't be found.
        """
        if subcommand not in get_commands():
            sys.stderr.write("Unknown command: %r\nType '%s help' for usage.\n" %
                             (subcommand, self.prog_name))
            sys.exit(1)  
        klass = load_command_class(subcommand)
        return klass

    def execute(self):
        """
        执行命令
        """
        args = self.argv

        try:
            subcommand = self.argv[1]
        except IndexError:
            subcommand = 'help'  # Display help if no arguments were given.

        if subcommand == 'help' or subcommand == '--help':
            if len(args) <= 2:
                sys.stdout.write(self.main_help_text() + '\n')
            else:
                self.fetch_command(args[2]).print_help(self.prog_name, args[2])

        elif subcommand == 'version' or subcommand == '--version':
            sys.stdout.write(get_version() + '\n')

        else:
            try:
                self.fetch_command(subcommand).run_from_argv(self.argv)
            except AssertionError:
                sys.stdout.write("error!\n")
                self.fetch_command(subcommand).print_help(self.prog_name, subcommand)
                exit(1)


def execute_from_command_line(argv=None):
    """
    执行一个命令行命令
    """
    utility = ManagementUtility(argv)
    utility.execute()


