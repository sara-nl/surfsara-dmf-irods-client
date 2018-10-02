import sys
import os
import re
import time
import atexit
import logging
import json
import traceback
import base64
import getpass
import datetime
from argparse import ArgumentParser
from server import DmIRodsServer
from table import Table
from table import get_term_size
from cprint import print_error
from cprint import terminal_erase
from cprint import terminal_home
from config import DmIRodsConfig
from socket_server import ServerApp
from socket_server import Client
from socket_server import ReturnCode


WATCH_DEALY = 2


def print_request_error(code, result):
    if code != ReturnCode.OK:
        try:
            print('Return Code %d (%s)' % (code, ReturnCode.to_string(code)))
            obj = json.loads(result)
            print('Exception %s raised' % obj.get('exception', '?'))
            print('Message: %s' % obj.get('msg', '?'))
            print('Traceback: %s' % obj.get('traceback', '?'))
            print_error(obj.get('msg', '?'), box=True)
        except Exception:
            print_error(result, box=True)


def ensure_daemon_is_running():
    app = ServerApp(DmIRodsServer,
                    socket_file=DmIRodsServer.get_socket_file(),
                    verbose=False)
    config = DmIRodsConfig(logger=app.logger)
    if not config.is_configured:
        app.stop()
    config.ensure_configured()
    try:
        app.start()
        client = Client(DmIRodsServer.get_socket_file())
        code, result = client.request({"password_configured": True})
        if code != ReturnCode.OK:
            print_request_error(code, result)
            sys.exit(8)
        if not json.loads(result).get('password_configured', False):
            pw = getpass.getpass('irods password for user %s: ' %
                                 config.config.get('irods_user_name', ''))
            code, result = client.request({"set_password":
                                           base64.b64encode(pw)})
            if code != ReturnCode.OK:
                print_request_error(code, result)
                sys.exit(8)
    except Exception as e:
        print(traceback.format_exc())
        print_error(str(e), box=True)
        sys.exit(8)


def init_logger():
    logger = logging.getLogger('dm_iclient')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger


def dm_iconfig(argv=sys.argv[1:]):
    parser = ArgumentParser(description='Configure iRODS_DMF_client')
    env_file_group = parser.add_argument_group('iRODS env file')
    env_file_group.add_argument("--irods_env_file",
                                type=str,
                                help="irods env file")
    env_file_group.add_argument("--irods_authentication_file",
                                type=str,
                                help="irods authentication file")
    cfg_group = parser.add_argument_group('iRODS configuration')
    cfg_group.add_argument('--irods_zone_name', type=str)
    cfg_group.add_argument('--irods_host', type=str)
    cfg_group.add_argument('--irods_port', type=int)
    cfg_group.add_argument('--irods_user_name', type=int)
    cfg_group.add_argument('--timeout', type=int,
                           help='timeout (in seconds, default 10)')
    cfg_group.add_argument('--resource', type=str,
                           help='iRODS resource (default arcRescSURF01)')
    cfg_server_group = parser.add_argument_group('DM-iRODS config')
    cfg_server_group.add_argument('--houskeeping',
                                  help=('remove old jobs after this time ' +
                                        '(hours, default=24)'),
                                  type=int)

    args = parser.parse_args(argv)
    conflict = []
    excl_list = ['irods_zone_name',
                 'irods_host',
                 'irods_port',
                 'irods_user_name']
    env_file_based = None
    if args.irods_env_file is not None:
        env_file_based = True
        for excl in excl_list:
            if getattr(args, excl):
                conflict.append('--irods_env_file <-> --%s' % excl)
    if args.irods_authentication_file is not None:
        env_file_based = True
        for excl in excl_list:
            if getattr(args, excl):
                conflict.append('--irods_authentication_file <-> --%s' % excl)
    if len(conflict) > 0:
        print_error('conflicting arguments:\n' +
                    '\n'.join(conflict), box=True)
        sys.exit(8)
    for excl in excl_list:
        if getattr(args, excl):
            env_file_based = False
    config = DmIRodsConfig(logger=init_logger())
    config.ensure_configured(force=True,
                             env_file_based=env_file_based,
                             config={k: getattr(args, k)
                                     for k in ['irods_zone_name',
                                               'irods_host',
                                               'irods_port',
                                               'irods_user_name',
                                               'irods_env_file',
                                               'irods_authentication_file']
                                     if getattr(args, k) is not None})


def dm_iget(argv=sys.argv[1:]):
    parser = ArgumentParser(description='Get files from archive.')
    parser.add_argument('files', type=str, nargs='+', help='files')
    parser.add_argument('--dir', type=str, default=os.getcwd(),
                        help='target directory (default cwd)')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    lst = []
    for f in args.files:
        local_file = os.path.join(args.dir, os.path.basename(f))
        code, result = client.request({"get": f,
                                       "local_file": local_file})
        if code != ReturnCode.OK:
            print_request_error(code, result)
            sys.exit(8)
        lst += [json.loads(result)]
    fmt = '{0: <20}{1: <40}'
    print(fmt.format("STATUS", "FILE"))
    for item in lst:
        print(fmt.format(item.get('msg', ''),
                         item.get('file', '')))


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


def dm_iinfo(argv=sys.argv[1:]):
    def fmt_time(timestamp):
        time_fmt = '%Y-%m-%d %H:%M:%S'
        return datetime.datetime.fromtimestamp(timestamp).strftime(time_fmt)

    fields = [{'group': 'Transfer'},
              {'field': 'retries'},
              {'field': 'status'},
              {'field': 'time_created', 'fmt': fmt_time},
              {'field': 'transferred'},
              {'field': 'mode'},
              {'group': 'Local File'},
              {'field': 'local_file'},
              {'field': 'local_atime', 'fmt': fmt_time},
              {'field': 'local_ctime', 'fmt': fmt_time},
              {'field': 'local_size'},
              {'field': 'checksum'},
              {'group': 'Remote Object'},
              {'field': 'remote_file'},
              {'field': 'remote_size'},
              {'field': 'remote_create_time', 'fmt': fmt_time},
              {'field': 'remote_modify_time', 'fmt': fmt_time},
              {'field': 'remote_checksum'},
              {'field': 'collection'},
              {'field': 'object'},
              {'field': 'remote_owner_name'},
              {'field': 'remote_owner_zone'},
              {'field': 'remote_replica_number'},
              {'field': 'remote_replica_status'},
              {'group': 'Remote Meta Data'},
              {'fieldre': 'meta_.*'}]

    parser = ArgumentParser(description='Get details for object.')
    parser.add_argument('file', type=str, help='object')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    code, result = client.request({"info": args.file})
    if code != ReturnCode.OK:
        print_request_error(code, result)
        sys.exit(8)
    obj = json.loads(result)
    fmt = '{0: <%d}{1}' % (max([len(v) for v in obj.keys()]) + 2)
    for entry in fields:
        if 'group' in entry:
            print "--------------------------"
            print entry.get('group')
            print "--------------------------"
        elif 'field' in entry:
            f = entry.get('field')
            value = obj.get(f, None)
            if value is not None:
                if 'fmt' in entry:
                    value = entry['fmt'](value)
                print(fmt.format(f + ':', value))
        elif 'fieldre' in entry:
            expr = re.compile(entry.get('fieldre'))
            for f, value in {k: v
                             for k, v in obj.items()
                             if expr.match(k)}.items():
                print(fmt.format(f + ':', value))
