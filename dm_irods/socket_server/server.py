import logging
import threading
import os
import socket
import time
import json
import traceback
from util import send_message
from util import recv_message
from util import ReturnCode


class CustomArguments(object):
    def get_cli_arguments(self):
        return []

    def before_argument_parsing(self, server_app, parser):
        pass

    def after_argument_parsing(self, server_app, parser, args):
        pass

    def before_start(self, server_app):
        pass

    def after_start(self, server_app):
        pass


class Server(object):
    """
    A basic server that accepts request via unix sockets
    """
    @classmethod
    def get_system_name(cls):
        return cls.__name__

    @staticmethod
    def get_custom_arguments():
        return CustomArguments()

    def __init__(self, socket_file, tick_sec=1,
                 is_daemon=True,
                 logger=logging.getLogger("Server"),
                 args=None):
        self.socket_file = socket_file
        self.tick_sec = tick_sec
        self.socket = None
        self.logger = logger
        self.active = True
        self.listener_thread = threading.Thread(name='listener',
                                                target=self.listener,
                                                args=())
        self.listener_thread.setDaemon(is_daemon)

    def run(self):
        while self.active:
            time.sleep(self.tick_sec)
            self.tick()
        self.tear_down()
        self.logger.info("stopped")

    def tick(self):
        pass

    def tear_down(self):
        pass

    def start_listener(self):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if os.path.exists(self.socket_file):
            self.logger.info("remove old socket file %s", self.socket_file)
            os.remove(self.socket_file)
        self.logger.info("bind %s", self.socket_file)
        self.socket.bind(self.socket_file)
        self.listener_thread.start()

    def stop(self, signum=0, frame=None):
        self.logger.info("stop requested")
        self.active = False

    def listener(self):
        self.logger.info("listen")
        self.socket.listen(1)
        while True:
            self.logger.debug('waiting for a connection')
            conn, addr = self.socket.accept()
            self.logger.debug('accepted')
            try:
                code, data = recv_message(conn)
                self.logger.debug('recvall %s', data)
                if self.active:
                    if code == ReturnCode.YIELD:
                        self.handle_request_all(conn, code, data)
                    else:
                        self.handle_request(conn, code, data)
                else:
                    msg = 'Server stopped'
                    send_message(conn, msg, ReturnCode.STOPPED)
            except Exception as e:
                self._send_error(conn, traceback.format_exc(), e)
            finally:
                conn.close()

    def handle_request(self, conn, code, data):
        try:
            code, ret = self.process(code, data)
            if isinstance(ret, dict):
                ret = json.dumps(ret)
            send_message(conn, ret, code)
        except Exception as e:
            self._send_error(conn, traceback.format_exc(), e)

    def handle_request_all(self, conn, code, data):
        try:
            for code, ret in self.process_all(code, data):
                if isinstance(ret, dict):
                    ret = json.dumps(ret)
                send_message(conn, ret, code)
            send_message(conn, "EOF", ReturnCode.EOF)
        except Exception as e:
            self._send_error(conn, traceback.format_exc(), e)

    def process(self, code, data):
        raise NotImplementedError('process not implemented')

    def process_all(self, code, data):
        raise NotImplementedError('process all not implemented')

    def _send_error(self, conn, tb, e):
        strmsg = str(e)
        if strmsg == 'None':
            strmsg = e.__class__.__name__
        msg = {'exception': e.__class__.__name__,
               'msg': strmsg,
               'traceback': tb}
        send_message(conn, json.dumps(msg), ReturnCode.ERROR)
