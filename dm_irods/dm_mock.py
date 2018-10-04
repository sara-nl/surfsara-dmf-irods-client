import sys
import os
import subprocess
import json
import time
from argparse import ArgumentParser
from dm_mock_server import DmMockServer
from socket_server import ServerApp
from socket_server import Client
from socket_server import ReturnCode


LS_PATH = '/bin/ls'


def ensure_daemon_is_running():
    app = ServerApp(DmMockServer,
                    socket_file=DmMockServer.get_socket_file(),
                    verbose=False)
    app.start()


def ls_inode_object(path):
    mode = os.stat(path)
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    code, result = client.request({"op": "ls",
                                   "inode": mode.st_ino})
    if code == ReturnCode.OK:
        return json.loads(result)
    else:
        print(result)
        raise ValueError("failed: %s" % ReturnCode.to_string(code))


def get_inode_object(path):
    mode = os.stat(path)
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    code, result = client.request({"op": "get",
                                   "inode": mode.st_ino})
    if code == ReturnCode.OK:
        return json.loads(result)
    else:
        print(result)
        raise ValueError("failed: %s" % ReturnCode.to_string(code))


def put_inode_object(p, remove):
    mode = os.stat(p)
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    code, result = client.request({"op": "put",
                                   "inode": mode.st_ino,
                                   "remove": remove})


def wait_for_states(paths, states):
    inodes = [os.stat(p).st_ino for p in paths]
    socket_file = DmMockServer.get_socket_file()
    client = Client(socket_file)
    while True:
        code, result = client.request({"op": "is_in_state",
                                       "inodes": inodes,
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
    args, unknown = parser.parse_known_args([a for a in argv
                                             if a not in ['-h', '--help']])
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
    args, unknown = parser.parse_known_args([a for a in argv
                                             if a not in ['-h', '--help']])
    ensure_daemon_is_running()
    paths = [os.path.abspath(f) for f in args.files]
    for p in paths:
        get_inode_object(p)
    if not args.quit:
        wait_for_states(paths, ['DUL', 'REG'])
