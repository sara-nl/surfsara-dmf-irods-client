import sys
import atexit
import json
import time
from argparse import ArgumentParser
from .socket_server.client import Client
from .socket_server.server import ReturnCode
from .server import ensure_daemon_is_running
from .server import DmIRodsServer
from .cprint import terminal_erase
from .cprint import terminal_home
from .cprint import print_request_error
from .table import Table
from .table import get_term_size


WATCH_DEALY = 2


def dm_ilist(argv=sys.argv[1:]):
    parser = ArgumentParser(description='List files in archive.')
    help_format = ('Configure columns to be displayed' +
                   'Examples:\n' +
                   'dmf,time,status,mod,file,local_file (default)\n' +
                   'dmf,time,status,mod,file:20,local_file:20')
    help_watch = 'display the list and refresh screen automatically'
    parser.add_argument('--format',
                        type=str,
                        default='dmf,time,status,mod,file,local_file',
                        help=help_format)
    parser.add_argument('--limit',
                        type=int,
                        help='limit number of items to be listed')
    parser.add_argument('--watch', '-w',
                        action='store_true',
                        help=help_watch)
    parser.add_argument('--active', '-a',
                        action='store_true',
                        help='only active objects')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    if args.watch:
        terminal_erase()
        if args.limit is None:
            (columns, lines) = get_term_size()
            args.limit = lines - 3
        atexit.register(terminal_erase)
    while True:
        table = Table(format=args.format)
        for code, result in client.request_all({"list": True,
                                                "all": True,
                                                "filter": {"active":
                                                           args.active},
                                                "limit": args.limit}):
            if code != ReturnCode.OK:
                print_request_error(code, result)
                sys.exit(8)
            table.print_row(json.loads(result))
        if args.watch:
            time.sleep(WATCH_DEALY)
            terminal_home()
        else:
            break


if __name__ == "__main__":
    dm_ilist()
