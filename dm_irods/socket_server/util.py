import struct


class ReturnCode(object):
    OK = 0
    ERROR = 1
    UNDEFINED = 2
    STOPPED = 3
    YIELD = 4
    EOF = 5

    code2str = {0: "OK",
                1: "ERROR",
                2: "UNDEFINED",
                3: "STOPPED",
                4: "YIELD",
                5: "EOF"}

    @staticmethod
    def to_string(code):
        return ReturnCode.code2str.get(code, "UNKNOWN")


def recvall(socket, count):
    ret = b''
    while count:
        data = socket.recv(count)
        if not data:
            raise EOFError()
        ret += data
        count -= len(data)
    return ret


def send_message(socket, data, code=ReturnCode.OK):
    length = len(data)
    socket.sendall(struct.pack('!II', length, code))
    socket.sendall(data)


def recv_message(socket):
    lengthbuf = recvall(socket, 8)
    length, code = struct.unpack('!II', lengthbuf)
    return (code, recvall(socket, length))
