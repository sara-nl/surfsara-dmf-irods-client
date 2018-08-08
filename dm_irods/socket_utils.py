import struct
import socket
import time
import logging

CONN_TRIALS = 10
RECONNECT_TIMEOUT = 1


def recvall(socket, count):
    ret = b''
    while count:
        data = socket.recv(count)
        if not data:
            raise EOFError()
        ret += data
        count -= len(data)
    return ret


def send_message(socket, data):
    length = len(data)
    socket.sendall(struct.pack('!I', length))
    socket.sendall(data)


def recv_message(socket):
    lengthbuf = recvall(socket, 4)
    length, = struct.unpack('!I', lengthbuf)
    return recvall(socket, length)


def connect(socket_file, logger=logging.getLogger('dm_iclient')):
    trials = CONN_TRIALS
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    while trials > 0:
        trials -= 1
        try:
            sock.connect(socket_file)
            return sock
        except:
            if trials == 0:
                logger.error("failed to connect to socket %s", socket_file)
                raise
            else:
                logger.warning("failed to connect to socket %s " +
                               "(trying again %d/%d)",
                               socket_file,
                               CONN_TRIALS - trials,
                               CONN_TRIALS)
                time.sleep(RECONNECT_TIMEOUT)
