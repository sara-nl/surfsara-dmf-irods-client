import io
import os
import json
import hashlib
import base64
import logging
import time


def sha256_checksum(filename, block_size=65536):
    def chunks(f, chunksize=io.DEFAULT_BUFFER_SIZE):
        return iter(lambda: f.read(chunksize), b'')

    hasher = hashlib.sha256()
    with open(filename, 'rb') as f:
        for chunk in chunks(f):
            hasher.update(chunk)
    return base64.b64encode(hasher.digest())


class Ticket(object):
    WAITING = 1
    CANCELED = 2
    GETTING = 3
    PUTTING = 4
    DONE = 5
    UNDEF = 6
    ERROR = 7
    RETRY = 8

    NONE = 0
    GET = 1
    PUT = 2

    code2string = {1: "WAITING",
                   2: "CANCELED",
                   3: "GETTING",
                   4: "PUTTING",
                   5: "DONE",
                   6: "UNDEF",
                   7: "ERROR",
                   8: "RETRY"}
    sorted_codes = [WAITING,
                    GETTING,
                    PUTTING,
                    RETRY,
                    CANCELED,
                    ERROR,
                    UNDEF,
                    DONE]
    mode2string = {0: "",
                   1: "GET",
                   2: "PUT"}
    fields = ['local_file',
              'local_atime',
              'local_ctime',
              'local_size',
              'remote_file',
              'remote_size',
              'time_created',
              'retries',
              'checksum',
              'transferred',
              'errmsg']

    def __init__(self,
                 local_file,
                 remote_file,
                 status=WAITING,
                 mode=NONE,
                 time_created=None,
                 retries=3,
                 checksum=None,
                 local_atime=None,
                 local_ctime=None,
                 local_size=None,
                 remote_size=None,
                 errmsg=None,
                 transferred=0):
        self.status = status
        self.mode = mode
        self.local_file = local_file
        self.remote_file = remote_file
        self.checksum = checksum
        self.local_atime = local_atime
        self.local_ctime = local_ctime
        self.local_size = local_size
        self.remote_size = remote_size
        self.transferred = transferred
        self.retries = int(retries)
        self.errmsg = ''
        if time_created is None:
            self.time_created = time.time()
        else:
            self.time_created = float(time_created)
        if mode == Ticket.PUT:
            self.update_local_attributes()

    def is_active(self):
        return (self.status == Ticket.WAITING or
                self.status == Ticket.GETTING or
                self.status == Ticket.PUTTING or
                self.status == Ticket.RETRY)

    def step(self, logger=logging.getLogger("Daemon")):
        logger.info('check %s' % self.to_json())

    @property
    def ticket_file(self):
        return (Ticket.mode_to_string(self.mode) + ":" +
                self.local_file.replace('/', '#') + ".json" +
                self.remote_file.replace('/', '#') + ".json")

    @staticmethod
    def status_to_string(status):
        return Ticket.code2string.get(status, "?")

    @staticmethod
    def mode_to_string(mode):
        return Ticket.mode2string.get(mode, "")

    @staticmethod
    def string_to_status(status):
        for k, v in Ticket.code2string.items():
            if v == status:
                return k
        return None

    @staticmethod
    def string_to_mode(mode):
        for k, v in Ticket.mode2string.items():
            if v == mode:
                return k
        return None

    def retry(self):
        self.transferred = 0
        self.status = Ticket.RETRY

    def update_local_checksum(self):
        self.checksum = sha256_checksum(self.local_file)

    def update_local_attributes(self):
        self.local_atime = os.path.getatime(self.local_file)
        self.local_ctime = os.path.getctime(self.local_file)
        self.local_size = os.path.getsize(self.local_file)

    def to_dict(self):
        ret = {f: getattr(self, f) for f in Ticket.fields}
        ret['status'] = Ticket.status_to_string(self.status)
        ret['mode'] = Ticket.mode_to_string(self.mode)
        return ret

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(obj):
        cobj = {str(k): str(value) if isinstance(value, unicode) else value
                for k, value in obj.items()
                if str(k) in Ticket.fields}
        cobj['status'] = Ticket.string_to_status(obj['status'])
        cobj['mode'] = Ticket.string_to_mode(obj['mode'])
        return Ticket(**cobj)
