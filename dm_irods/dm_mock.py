import sys
import os
import subprocess
import json
import time
from argparse import ArgumentParser
from .dm_mock_server import DmMockServer
from .socket_server.server_app import ServerApp
from .socket_server.client import Client
from .socket_server.server import ReturnCode


LS_PATH = '/bin/ls'
DMATTR_FIELDS = ['bfid',
                 'emask',
                 'fhandle',
                 'flags',
                 'nregn',
                 'owner',
                 'path',
                 'projid',
                 'sitetag',
                 'size',
                 'space',
                 'state']


def ensure_daemon_is_running():
    app = ServerApp(DmMockServer,
                    socket_file=DmMockServer.get_socket_file(),
                    verbose=False)
    app.start()


def ls_inode_object(path):
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    code, result = client.request({"op": "ls",
                                   "path": path})
    if code == ReturnCode.OK:
        return json.loads(result)
    else:
        print(result)
        raise ValueError("failed: %s" % ReturnCode.to_string(code))


def get_inode_object(path):
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    code, result = client.request({"op": "get",
                                   "path": path})
    if code == ReturnCode.OK:
        return json.loads(result)
    else:
        print(result)
        raise ValueError("failed: %s" % ReturnCode.to_string(code))


def put_inode_object(p, remove):
    # mode = os.stat(p)
    # "inode": mode.st_ino,
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    code, result = client.request({"op": "put",
                                   "path": p,
                                   "remove": remove})


def wait_for_states(paths, states):
    # inodes = [os.stat(p).st_ino for p in paths]
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    while True:
        code, result = client.request({"op": "is_in_state",
                                       "paths": paths,
                                       "states": states})
        if json.loads(result).get('is_in_state'):
            break
        time.sleep(1)


def dmls(argv=sys.argv[1:]):
    parser = ArgumentParser(description='')
    parser.add_argument('files', type=str, nargs='*')
    args, unknown = parser.parse_known_args([a for a in argv
                                             if a not in ['-h', '--help']])
    ensure_daemon_is_running()
    cwd = os.getcwd()
    curr_dir = cwd
    files = args.files
    argv = argv + ['--time-style', 'long-iso']
    p = subprocess.Popen([LS_PATH] + argv,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    (out, err) = p.communicate()
    if sys.version_info[0] > 2:
        out = out.decode()
        err = err.decode()
    lines = out.split('\n')
    if len(lines) > 0:
        for line in lines:
            if line.endswith(':') and line[0:-1] in files:
                curr_dir = os.path.join(cwd, line[0:-1])
                print(line)
            elif line == '':
                print("")
            else:
                _dmls_process_line(line, curr_dir)
    sys.stderr.write(err)
    code = p.returncode
    sys.exit(code)


def _dmls_process_line(line, basedir):
    fmt = '{0} {1} {2:<12} {3:<12} {4:>12} {5} {6} {7}'
    cols = line.split()
    if len(cols) == 8:
        p = os.path.join(basedir, cols[-1])
        obj = ls_inode_object(p)
        s = cols[0:6] + ["(%s)" % obj.get('state')] + [cols[7]]
        print(fmt.format(*tuple(s)))
    else:
        print(line)


def dmput(argv=sys.argv[1:]):
    parser = ArgumentParser(description='')
    parser.add_argument('files', type=str, nargs='*')
    parser.add_argument("-r", action='store_true', dest='remove',
                        help="remove file locally")
    parser.add_argument("-w", action='store_true', dest='wait',
                        help="wait until all files have been copied")
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    paths = [os.path.abspath(f) for f in args.files]
    for f in paths:
        put_inode_object(f, args.remove)
    if args.wait:
        wait_for_states(paths, ['DUL', 'OFL'])


def dmget(argv=sys.argv[1:]):
    parser = ArgumentParser(description='')
    parser.add_argument('files', type=str, nargs='*')
    parser.add_argument("-q", action='store_true', dest='quit',
                        help="recalls migrated file")
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    paths = [os.path.abspath(f) for f in args.files]
    for p in paths:
        get_inode_object(p)
    if not args.quit:
        wait_for_states(paths, ['DUL', 'REG'])


def dmattr_format_attr(obj):
    # import pprint
    # pprint.pprint(obj)
    line = ''
    for f in DMATTR_FIELDS:
        if line:
            line += ' '
        line += str(obj.get(f))
    print(line)


def dmattr_long_format_attr(obj):
    fmt = "{0: >%d} : {1}" % max([len(f) for f in DMATTR_FIELDS])
    for f in DMATTR_FIELDS:
        print(fmt.format(f, str(obj.get(f))))
    print("")


def dmattr(argv=sys.argv[1:]):
    parser = ArgumentParser(description='')
    parser.add_argument('files', type=str, nargs='*')
    parser.add_argument("-l", action='store_true', dest='long',
                        help="long format")
    args = parser.parse_args(argv)
    if args.long:
        formatter = dmattr_long_format_attr
    else:
        formatter = dmattr_format_attr
    ensure_daemon_is_running()
    for f in args.files:
        obj = ls_inode_object(os.path.abspath(f))
        obj['path'] = f
        formatter(obj)
