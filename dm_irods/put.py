import sys
import os
import json
from argparse import ArgumentParser
from .socket_server.client import Client
from .socket_server.server import ReturnCode
from .server import ensure_daemon_is_running
from .server import DmIRodsServer
from .cprint import print_request_error


def dm_iput(argv=sys.argv[1:]):
    parser = ArgumentParser(description='Put files to archive.')
    parser.add_argument('files', type=str, nargs='+', help='files')
    parser.add_argument('--coll', type=str,
                        default='/{zone}/home/{user}',
                        help='target collection (default /{zone}/home/{user})')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    lst = []
    for f in args.files:
        remote_file = os.path.join(args.coll, os.path.basename(f))
        code, result = client.request({"put": os.path.abspath(f),
                                       "remote_file": remote_file})
        if code != ReturnCode.OK:
            print_request_error(code, result)
            sys.exit(8)
        lst += [json.loads(result)]
    fmt = '{0: <20}{1: <40}'
    print(fmt.format("STATUS", "FILE"))
    for item in lst:
        print(fmt.format(item.get('msg', ''),
                         item.get('file', '')))


if __name__ == "__main__":
    dm_iput()
