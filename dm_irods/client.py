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
from cprint import format_bold
from cprint import format_status
from cprint import format_error
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
    """
    Interactive configuration of the client.
    Attempts to extract information from ~/.irods/irods_environment.json
    if available.
    """
    parser = ArgumentParser(description='Configure iRODS_DMF_client')
    irods_env = os.path.expanduser("~/.irods/irods_environment.json")
    if os.path.isfile(irods_env):
        with open(irods_env) as fp:
            config = json.load(fp)
    else:
        config = {}
    cfg_group = parser.add_argument_group('iRODS configuration')
    cfg_group.add_argument('--irods_zone_name',
                           type=str,
                           default=config.get('irods_zone_name', None))
    cfg_group.add_argument('--irods_host',
                           type=str,
                           default=config.get('irods_host', None))
    cfg_group.add_argument('--irods_port',
                           type=int,
                           default=config.get('irods_port', None))
    cfg_group.add_argument('--irods_user_name',
                           type=str,
                           default=config.get('irods_user_name', None))
    cfg_group.add_argument('--irods_is_resource_server', action="store_true",
                           help=("Connected directly to resource server\n" +
                                 "(using microservice msiGetDmfObject to " +
                                 "retrieve DMF state,\n" +
                                 "otherwise GetDmfObject wrapper " +
                                 "rule is used)"))
    cfg_group.add_argument('--connection_timeout', type=int,
                           help='timeout (in seconds, default 10)')
    cfg_group.add_argument('--stop_timeout', type=int,
                           help=('stop daemon automatically after being idle' +
                                 '(in minutes, default 10, 0 = never stop)'))
    cfg_group.add_argument('--resource_name', type=str,
                           help='iRODS resource (default arcRescSURF01)')
    cfg_server_group = parser.add_argument_group('DM-iRODS config')
    cfg_server_group.add_argument('--housekeeping',
                                  help=('remove old jobs after this time ' +
                                        '(hours, default=24)'),
                                  type=int)

    args = parser.parse_args(argv)
    config = DmIRodsConfig(logger=init_logger())
    config.ensure_configured(force=True,
                             config={k: getattr(args, k)
                                     for k in ['irods_zone_name',
                                               'irods_host',
                                               'irods_port',
                                               'irods_user_name',
                                               'irods_is_resource_server',
                                               'housekeeping',
                                               'resource_name',
                                               'connection_timeout',
                                               'stop_timeout']
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


def dm_icomplete(argv=sys.argv[1:]):
    app = ServerApp(DmIRodsServer,
                    socket_file=DmIRodsServer.get_socket_file(),
                    verbose=False)
    config = DmIRodsConfig(logger=app.logger)
    if not config.is_configured:
        return
    if app.status().status == 'NOT RUNNING':
        return
    if len(argv) > 1:
        prefix = argv[1]
    else:
        prefix = ''
    client = Client(DmIRodsServer.get_socket_file())
    for code, result in client.request_all({"completion_list": prefix}):
        if code != ReturnCode.OK:
            print_request_error(code, result)
            sys.exit(8)
        print(result)


def dm_iinfo(argv=sys.argv[1:]):
    def fmt_time(timestamp):
        time_fmt = '%Y-%m-%d %H:%M:%S'
        return datetime.datetime.fromtimestamp(timestamp).strftime(time_fmt)

    def format_progress(progress):
        return progress

    def print_value(maxlen, f, value, entry={}):
        colorizer = entry.get('colorizer', None)
        if 'fmt' in entry:
            value = entry['fmt'](value)
        if not isinstance(value, str) and not isinstance(value, unicode):
            value = str(value)
        f = format_bold(('{0: <%d}' % maxlen).format(f))
        sep = ': '
        for line in value.split('\n'):
            if colorizer is not None:
                line = colorizer(line)
            print(f + sep + line)
            f = ('{0: <%d}' % maxlen).format('')
            sep = '  '

    def count_groups(fields, obj):
        current_group = ''
        ret = {'': 0}
        for entry in fields:
            if 'group' in entry:
                current_group = entry.get('group')
                ret[current_group] = 0
            elif 'field' in entry:
                f = entry.get('field')
                if obj.get(f, None) is not None:
                    ret[current_group] += 1
            elif 'fieldre' in entry:
                expr = re.compile(entry.get('fieldre'))
                ret[current_group] += sum([1 if expr.match(k) else 0
                                           for k in obj.keys()])
        return ret

    fields = [{'group': 'Transfer'},
              {'field': 'retries'},
              {'field': 'status', 'colorizer': format_status},
              {'field': 'progress', 'colorizer': format_progress},
              {'field': 'errmsg', 'colorizer': format_error},
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
              {'group': 'DMF Data'},
              {'fieldre': 'DMF_.*'}]
    parser = ArgumentParser(description='Get details for object.')
    parser.add_argument('file',
                        type=str,
                        help='object')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    code, result = client.request({"info": args.file})
    if code != ReturnCode.OK:
        print_request_error(code, result)
        sys.exit(8)
    obj = json.loads(result)
    if not obj:
        return
    maxlen = max([len(v) for v in obj.keys()]) + 2
    groups = count_groups(fields, obj)
    current_group = ''
    for entry in fields:
        if 'group' in entry:
            current_group = entry.get('group')
            if groups.get(current_group, 0) > 0:
                print("--------------------------")
                print(current_group)
                print("--------------------------")
        elif 'field' in entry:
            if groups.get(current_group, 0) > 0:
                f = entry.get('field')
                value = obj.get(f, None)
                if value is not None:
                    print_value(maxlen, f, value, entry)
        elif 'fieldre' in entry:
            if groups.get(current_group, 0) > 0:
                expr = re.compile(entry.get('fieldre'))
                for f, value in {k: v
                                 for k, v in obj.items()
                                 if expr.match(k)}.items():
                    print_value(maxlen, f, value, entry)
