import sys
import os
import logging
import json
import traceback
import base64
import getpass
from argparse import ArgumentParser
from server import DmIRodsServer
from table import Table
from cprint import print_error
from config import DmIRodsConfig
sys.path.insert(0,
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "py-socket-server"))
from socket_server import ServerApp
from socket_server import Client
from socket_server import ReturnCode


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
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    table = Table()
    for code, result in client.request_all({"list": True,
                                            "all": True}):
        if code != ReturnCode.OK:
            print_request_error(code, result)
            sys.exit(8)
        table.print_row(json.loads(result))
