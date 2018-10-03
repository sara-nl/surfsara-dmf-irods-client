import io
import os
import logging
import json
import traceback
import time
import hashlib
import base64
from irods_session import iRODS
from config import DmIRodsConfig
from irods.exception import NetworkException
from irods.exception import RULE_FAILED_ERR
from socket_server import Server
from socket_server import ReturnCode


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
              'transferred']

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


class DmIRodsServer(Server):
    # succ codes
    OK = 0
    RESCHEDULED = 1

    # err codes
    ALREADY_REGISTERED = 2
    FAILED = 3

    TICK_INTERVAL = 10
    HOUSEKEEPING_INTERVAL = 3600

    @staticmethod
    def get_socket_file():
        return os.path.join(os.path.expanduser("~"),
                            ".DmIRodsServer", "DmIRodsServer.socket")

    def __init__(self, socket_file, **kwargs):
        kwargs['tick_sec'] = DmIRodsServer.TICK_INTERVAL
        super(DmIRodsServer, self).__init__(DmIRodsServer.get_socket_file(),
                                            **kwargs)
        config = DmIRodsConfig(logger=self.logger)
        config.ensure_configured()
        self.ticket_dir = os.path.join(os.path.expanduser("~"),
                                       ".DmIRodsServer",
                                       "Tickets")
        self.tickets = {}
        self.active_tickets = {}
        if not os.path.exists(self.ticket_dir):
            os.makedirs(self.ticket_dir)
        self.read_tickets()
        self.dm_irods_config = DmIRodsConfig(logger=self.logger)
        self.config = self.dm_irods_config.config
        self.last_housekeeping = time.time()
        self.housekeeping_interval = DmIRodsServer.HOUSEKEEPING_INTERVAL
        if 'irods_env_file' in self.config.get('irods', {}):
            with open(self.config['irods']['irods_env_file']) as f:
                cfg = json.load(f)
        else:
            cfg = self.config['irods']
        self.zone = cfg['irods_zone_name']
        self.user = cfg['irods_user_name']
        self.resource = self.config.get('irods', {}).get('resource_name', '')
        self.stop_timeout = self.config.get('stop_timeout', 0) * 60
        self.heartbeat = time.time()
        if not self.dm_irods_config.is_configured:
            raise RuntimeError('failed to read config from %s' %
                               self.dm_irods_config.config_file)
        # managing remote completion list
        self.completion_list = []
        self.completion_list_updated = 0
        self.completion_list_timeout = 60

    def irods_connection(self):
        return iRODS(logger=self.logger, **self.config['irods'])

    def read_tickets(self):
        for root, dirs, files in os.walk(self.ticket_dir):
            for file in files:
                if file.endswith(".json"):
                    ticket_file = os.path.join(root, file)
                    self.logger.info("reading ticket from file %s",
                                     ticket_file)
                    with open(ticket_file) as f:
                        data = json.load(f)
                        ticket = Ticket.from_json(data)
                        if ticket.status in [Ticket.GETTING, Ticket.PUTTING]:
                            ticket.retry()
                            ticket.retries = 3
                        p = (ticket.local_file, ticket.remote_file)
                        self.tickets[p] = ticket
                        if ticket.is_active():
                            self.active_tickets[p] = ticket
                    if ticket.mode == Ticket.PUT:
                        ticket.update_local_attributes()
                        with open(ticket_file, 'wb') as f:
                            f.write(ticket.to_json())
                    self.logger.info(ticket.to_json())

    def process(self, code, data):
        self.heartbeat = time.time()
        obj = json.loads(data)
        if "get" in obj:
            return self.process_get(obj)
        elif "put" in obj:
            return self.process_put(obj)
        elif "info" in obj:
            return self.process_info(obj)
        elif "password_configured" in obj:
            ret = ('irods_password' in self.config.get('irods', {}) or
                   'irods_authentication_file' in self.config.get('irods', {}))
            return (ReturnCode.OK, json.dumps({'password_configured': ret}))
        elif "set_password" in obj:
            self.config['irods']['irods_password'] = obj["set_password"]
            return (ReturnCode.OK, json.dumps({'password_configured': True}))
        else:
            return (ReturnCode.ERROR,
                    ("invalid command %s" % json.dumps(data)))

    def process_all(self, code, data):
        self.heartbeat = time.time()
        obj = json.loads(data)
        if "list" in obj:
            for code, item in self.process_list(obj):
                yield code, item
        elif "completion_list" in obj:
            for code, item in self.process_completion_list(obj):
                yield code, item
        else:
            yield (ReturnCode.ERROR,
                   ("invalid command %s" % json.dumps(data)))

    def process_get(self, obj):
        remote_file = obj["get"]
        local_file = obj['local_file']
        try:
            remote_file = remote_file.encode().format(zone=self.zone,
                                                      user=self.user)
            local_file = local_file.encode()
        except Exception:
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": "cannot encode unicode"})
        if isinstance(remote_file, str):
            return (ReturnCode.OK,
                    self.register_ticket(local_file, remote_file, Ticket.GET))
        else:
            try:
                s = str(obj["get"])
            except Exception:
                s = "[object]"
            return (ReturnCode.ERROR,
                    {"code": DmIRodsServer.FAILED,
                     "msg": "invalid type: %s" % s})

    def process_put(self, obj):
        remote_file = obj["remote_file"]
        local_file = obj['put']

        try:
            remote_file = remote_file.encode().format(zone=self.zone,
                                                      user=self.user)
            local_file = local_file.encode()
        except Exception:
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": "cannot encode unicode"})
        if isinstance(local_file, str):
            return (ReturnCode.OK,
                    self.register_ticket(local_file, remote_file, Ticket.PUT))
        else:
            try:
                s = str(local_file)
            except Exception:
                s = "[object]"
            return (ReturnCode.ERROR,
                    {"code": DmIRodsServer.FAILED,
                     "msg": "invalid type: %s" % s})

    def process_info(self, obj):
        remote_file = obj['info']
        try:
            remote_file = remote_file.encode()
        except Exception:
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": "cannot encode unicode"})
        with self.irods_connection() as irods:
            obj = os.path.basename(remote_file)
            coll = os.path.dirname(remote_file)
            res = next(irods.list_objects({'object': obj,
                                           'collection': coll}), None)
            if res is None:
                return ReturnCode.OK, json.dumps({})
            else:
                remote_file = os.path.join(res.get('collection', ''),
                                           res.get('object', ''))
                print len(self.tickets.values())
                for ticket in self.tickets.values():
                    if ticket.remote_file == str(remote_file):
                        for k, v in ticket.to_dict().items():
                            if res.get(k, None) is None:
                                res[k] = v
                return ReturnCode.OK, json.dumps(res)

    def process_list(self, obj):
        limit = obj.get('limit', None)
        if limit is None:
            limit = 1000000000
        with self.irods_connection() as irods:
            missing_tickets = {}
            tickets = {}
            ticket_list = self.tickets.values()
            ticket_list.sort(key=lambda x: x.time_created)
            ticket_stat = {k: 0 for k in Ticket.sorted_codes}
            for ticket in ticket_list:
                missing_tickets[ticket.remote_file] = ticket
                tickets[ticket.remote_file] = ticket
                ticket_stat[ticket.status] += 1

            code_list = [code for code
                         in Ticket.sorted_codes if ticket_stat[code] > 0]
            code_list += [0]
            for code in code_list:
                for obj in irods.list_objects():
                    filename = (obj.get('collection', '') +
                                '/' +
                                obj.get('object'))
                    if filename in tickets:
                        if tickets[filename].status == code:
                            tmp = tickets[filename].to_dict()
                            for k, v in tmp.items():
                                if obj.get(k, None) is None:
                                    obj[k] = v
                            if filename in missing_tickets:
                                del missing_tickets[filename]
                            limit -= 1
                            yield ReturnCode.OK, json.dumps(obj)
                    elif code == 0:
                        limit -= 1
                        yield ReturnCode.OK, json.dumps(obj)
                    if limit <= 0:
                        break
            for item in self._process_missing_list(missing_tickets.values(),
                                                   limit):
                yield item

    def _process_missing_list(self, missing_tickets, limit):
        def sort_key(x):
            return Ticket.sorted_codes.index(x.status)

        if limit > 0:
            missing_tickets.sort(key=sort_key, reverse=False)
            for ticket in missing_tickets:
                obj = {"object": os.path.basename(ticket.remote_file),
                       "collection": os.path.dirname(ticket.remote_file),
                       "meta_SURF-DMF": '???'}
                for k, v in ticket.to_dict().items():
                    obj[k] = v
                limit -= 1
                yield ReturnCode.OK, json.dumps(obj)
                if limit <= 0:
                    break

    def process_completion_list(self, obj):
        now = time.time()
        age = now - self.completion_list_updated
        if age > self.completion_list_timeout:
            self.completion_list = []
            self.completion_list_updated = now
            with self.irods_connection() as irods:
                for obj in irods.list_objects():
                    filename = "%s/%s" % (obj.get('collection', ''),
                                          obj.get('object'))
                    self.completion_list.append(filename)
        for filename in self.completion_list:
            yield ReturnCode.OK, filename

    def register_ticket(self, local_file, remote_file, mode):
        p = (local_file, remote_file)
        ticket = self.tickets.get(p, None)
        if ticket is not None:
            if ticket.is_active():
                return {"file": '%s <> %s' % (local_file, remote_file),
                        "ticket": ticket.to_dict(),
                        "code": DmIRodsServer.ALREADY_REGISTERED,
                        "msg": "%s <-> %s already registered" % (local_file,
                                                                 remote_file)}
            else:
                try:
                    ticket = self.create_ticket(local_file, remote_file, mode)
                    return {"file": '%s <> %s' % (local_file, remote_file),
                            "ticket": ticket.to_dict(),
                            "code": DmIRodsServer.RESCHEDULED,
                            "msg": "rescheduled"}
                except Exception as ex:
                    self.logger.error(traceback.format_exc())
                    return {"file": '%s <> %s' % (local_file, remote_file),
                            "ticket": None,
                            "code": DmIRodsServer.FAILED,
                            "msg": str(ex)}
        else:
            try:
                ticket = self.create_ticket(local_file, remote_file, mode)
                return {"file": '%s <> %s' % (local_file, remote_file),
                        "ticket": ticket.to_dict(),
                        "code": DmIRodsServer.OK,
                        "msg": "scheduled"}
            except Exception as ex:
                self.logger.error(traceback.format_exc())
                return {"file": '%s <> %s' % (local_file, remote_file),
                        "ticket": None,
                        "code": DmIRodsServer.FAILED,
                        "msg": str(ex)}

    def create_ticket(self, local_file, remote_file, mode):
        ticket = Ticket(local_file, remote_file, mode=mode)
        p = (local_file, remote_file)
        self.tickets[p] = ticket
        self.active_tickets[p] = ticket
        with open(os.path.join(self.ticket_dir,
                               ticket.ticket_file), "w") as fp:
            fp.write(ticket.to_json())
        return ticket

    def update_ticket(self, local_file, remote_file):
        ticket = self.tickets[(local_file, remote_file)]
        with open(os.path.join(self.ticket_dir,
                               ticket.ticket_file), "w") as fp:
            fp.write(ticket.to_json())

    def delete_ticket(self,  local_file, remote_file):
        p = (local_file, remote_file)
        ticket = self.tickets[p]
        ticket_file = os.path.join(self.ticket_dir,
                                   ticket.ticket_file)
        self.logger.info('remove ticket for %s <->', ticket_file)
        del self.tickets[p]
        if p in self.active_tickets:
            del self.active_tickets[p]
        try:
            os.remove(ticket_file)
        except Exception as e:
            self.logger.warning('cannot remove ticket file %s', ticket_file)
            self.logger.error(e.__class__.__name__)
            self.logger.error(str(e))
            for line in traceback.format_exc().split('\n'):
                self.logger.error(line)

    def tick(self):
        self.housekeeping()
        for p, ticket in self.active_tickets.items():
            if not self.active:
                break
            if ticket.status in [Ticket.WAITING, Ticket.RETRY]:
                self.heartbeat = time.time()
                if ticket.mode == Ticket.GET:
                    self._tick_download(p, ticket)
                else:
                    self._tick_upload(p, ticket)
        if not self.active_tickets and self.stop_timeout > 0:
            last_heartbeat = time.time() - self.heartbeat
            if last_heartbeat > self.stop_timeout:
                self.logger.info('stop daemon due to inactivity')
                self.active = False

    def _tick_download(self, p, ticket):
        self.heartbeat = time.time()
        with self.irods_connection() as irods:
            try:
                self.logger.info('get %s -> %s' % (p[1], p[0]))
                self.tickets[p].status = Ticket.GETTING
                irods.get(ticket)
                self.logger.info('done %s -> %s' % (p[1], p[0]))
                self.tickets[p].status = Ticket.DONE
                del self.active_tickets[p]
                self.update_ticket(p[0], p[1])
            except RULE_FAILED_ERR as e:
                self.logger.debug('failed rule %s', str(e))
            except NetworkException as e:
                self.tickets[p].status == Ticket.RETRY
                if self.tickets[p].retries > 0:
                    self.logger.warning('failed to get %s -> %s,' +
                                        'remaining %d trials',
                                        (p[1], p[0]),
                                        self.tickets[p].retries)
                    self.tickets[p].retries -= 1
                    self.update_ticket(p[0], p[1])
                else:
                    self.logger.error('failed to get %s -> %s', (p[1], p[0]))
                    self._log_exception(e, traceback.format_exc())
                    self.tickets[p].status = Ticket.ERROR
                    del self.active_tickets[p]
                    self.update_ticket(p[0], p[1])
            except Exception as e:
                self.logger.error('failed to get %s -> %s', (p[1], p[0]))
                self._log_exception(e, traceback.format_exc())
                self.tickets[p].status = Ticket.ERROR
                del self.active_tickets[p]
                self.update_ticket(p[0], p[1])
        self.heartbeat = time.time()

    def _tick_upload(self, p, ticket):
        self.heartbeat = time.time()
        with self.irods_connection() as irods:
            try:
                if not os.path.isfile(self.tickets[p].local_file):
                    raise IOError('file %s does not exist' %
                                  self.tickets[p].local_file)
                self.tickets[p].update_local_checksum()
                self.logger.info('chcksum %s:%s',
                                 self.tickets[p].local_file,
                                 self.tickets[p].checksum)
                self.logger.info('put %s -> %s', p[0], p[1])
                self.tickets[p].status = Ticket.PUTTING
                irods.put(ticket)
                self.logger.info('done %s -> %s', p[0], p[1])
                self.tickets[p].status = Ticket.DONE
                del self.active_tickets[p]
                self.update_ticket(p[0], p[1])
            except NetworkException as e:
                self.tickets[p].status == Ticket.RETRY
                if self.tickets[p].retries > 0:
                    self.logger.warning('failed to put %s -> %s,' +
                                        'remaining %d trials',
                                        p[0], p[1], self.tickets[p].retries)
                    self.tickets[p].retries -= 1
                    self.update_ticket(p[0], p[1])
                else:
                    self.logger.error('failed to put %s -> %s', p[0], p[1])
                    self._log_exception(e, traceback.format_exc())
                    self.tickets[p].status = Ticket.ERROR
                    del self.active_tickets[p]
                    self.update_ticket(p[0], p[1])
            except Exception as e:
                self.logger.error('failed to put %s -> %s', p[0], p[1])
                self._log_exception(e, traceback.format_exc())
                self.tickets[p].status = Ticket.ERROR
                del self.active_tickets[p]
                self.update_ticket(p[0], p[1])
        self.heartbeat = time.time()

    def housekeeping(self):
        curr = time.time()
        keep_seconds = self.config.get('housekeeping', 24) * 3600
        if curr - self.last_housekeeping > self.housekeeping_interval:
            self.logger.info('housekeeping')
            try:
                with self.irods_connection() as irods:
                    tickets = {}
                    for ticket in self.tickets.values():
                        tickets[ticket.remote_file] = ticket
                    for obj in irods.list_objects():
                        filename = "%s/%s" % (obj.get('collection', ''),
                                              obj.get('object'))
                        if filename in self.tickets:
                            del tickets[filename]
                    for ticket in tickets.values():
                        age = time.time() - ticket.time_created
                        if age > keep_seconds:
                            self.delete_ticket(ticket.local_file,
                                               ticket.remote_file)
            except Exception as e:
                self.logger.error('housekeeping failed')
                self._log_exception(e, traceback.format_exc())
            self.last_houskeeping = curr

    def _log_exception(self, e, tb):
        self.logger.error(e.__class__.__name__)
        self.logger.error(str(e))
        for line in tb.split('\n'):
            self.logger.error(line)
