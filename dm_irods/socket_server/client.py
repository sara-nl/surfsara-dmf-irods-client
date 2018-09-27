import logging
import socket
import time
import json
from util import send_message
from util import recv_message
from util import ReturnCode


class Client(object):
    def __init__(self, socket_file,
                 conn_trials=10,
                 reconnect_timeout=1,
                 logger=logging.getLogger("Client")):
        self.socket_file = socket_file
        self.conn_trials = conn_trials
        self.reconnect_timeout = reconnect_timeout
        self.logger = logger

    def request(self, msg):
        if isinstance(msg, dict):
            msg = json.dumps(msg)
        sock = self.connect()
        send_message(sock, msg)
        code, response = recv_message(sock)
        sock.close()
        return (code, response)

    def request_all(self, msg):
        if isinstance(msg, dict):
            msg = json.dumps(msg)
        sock = self.connect()
        send_message(sock, msg, ReturnCode.YIELD)
        code = ReturnCode.OK
        while code == ReturnCode.OK:
            code, response = recv_message(sock)
            if code != ReturnCode.EOF:
                yield code, response
        sock.close()

    def connect(self):
        trials = self.conn_trials
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        while trials > 0:
            trials -= 1
            try:
                sock.connect(self.socket_file)
                return sock
            except Exception:
                if trials == 0:
                    self.logger.error("failed to connect to socket %s",
                                      self.socket_file)
                    raise
                else:
                    self.logger.warning("failed to connect to socket %s " +
                                        "(trying again %d/%d)",
                                        self.socket_file,
                                        self.conn_trials - trials,
                                        self.conn_trials)
                    time.sleep(self.reconnect_timeout)
