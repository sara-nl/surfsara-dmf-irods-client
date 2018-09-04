import sys
import os
import logging
import json
import time
import datetime
from argparse import ArgumentParser
from server import DmIRodsServer
sys.path.insert(0,
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "py-socket-server"))
from socket_server import ServerApp
from socket_server import Client
from socket_server import ReturnCode


def ensure_daemon_is_running():
    app = ServerApp(DmIRodsServer,
                    socket_file=DmIRodsServer.get_socket_file(),
                    verbose=False)
    app.start()


def init_logger():
    logger = logging.getLogger('dm_iclient')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    return logger


def dm_iget(argv=sys.argv[1:]):
    parser = ArgumentParser(description='Get files from archive.')
    parser.add_argument('files', type=str, nargs='+', help='files')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    lst = []
    for f in args.files:
        code, result = client.request({"get": f})
        if code != ReturnCode.OK:
            raise ValueError("failed: %s" % ReturnCode.to_string(code))
        lst += [json.loads(result)]
    fmt = '{0: <20}{1: <40}'
    print(fmt.format("STATUS", "FILE"))
    for item in lst:
        print(fmt.format(item.get('msg', ''),
                         item.get('file', '')))


def dm_ilist(argv=sys.argv[1:]):
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    code, result = client.request({"list": True})
    if code != ReturnCode.OK:
        print(result)
        raise ValueError("failed: %s" % ReturnCode.to_string(code))
    print_table(json.loads(result))


def print_table(data):
    fmt = '{0: <11}{1: <20}{2: <40}'
    print(fmt.format("STATUS",
                     "TIME",
                     "FILE"))
    lst = data.get('tickets', [])
    time_fmt = '%Y-%m-%d %H:%M:%S'
    for item in lst:
        tim = float(item.get('time_created', int(time.time())))
        dtg = datetime.datetime.fromtimestamp(tim)
        import pprint
        pprint.pprint(item)
        print(fmt.format(item.get('status', ''),
                         dtg.strftime(time_fmt),
                         item.get('filename', '')))
