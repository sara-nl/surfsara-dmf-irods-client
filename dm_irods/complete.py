import sys
from .cprint import print_request_error
from .config import DmIRodsConfig
from .socket_server.server_app import ServerApp
from .socket_server.server import ReturnCode
from .socket_server.client import Client
from .server import DmIRodsServer


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


if __name__ == "__main__":
    dm_icomplete()
