import sys
import os
import logging
import json
import traceback
import time
from irods_session import iRODS
from config import DmIRodsConfig
from irods.exception import NetworkException
from irods.exception import RULE_FAILED_ERR
sys.path.insert(0,
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "py-socket-server"))
from socket_server import Server
from socket_server import ReturnCode


class Ticket(object):
    WAITING = 1
    CANCELED = 2
    GETTING = 3
    PUTTING = 4
    DONE = 5
    UNDEF = 6
    ERROR = 7
    RETRY = 8

    code2string = {1: "WAITING",
                   2: "CANCELED",
                   3: "GETTING",
                   4: "PUTTING",
                   5: "DONE",
                   6: "UNDEF",
                   7: "ERROR",
                   8: "RETRY"}

    def __init__(self,
                 filename,
                 status=WAITING,
                 time_created=None,
                 retries=3,
                 cwd=os.getcwd()):
        self.status = status
        self.filename = filename
        self.cwd = cwd
        self.retries = retries
        if time_created is None:
            self.time_created = time.time()
        else:
            self.time_created = float(time_created)

    def is_active(self):
        return (self.status == Ticket.WAITING or
                self.status == Ticket.GETTING or
                self.status == Ticket.PUTTING or
                self.status == Ticket.RETRY)

    def step(self, logger=logging.getLogger("Daemon")):
        logger.info('check %s' % self.to_json())

    @staticmethod
    def status_to_string(status):
        return Ticket.code2string.get(status, "?")

    @staticmethod
    def string_to_status(status):
        for k, v in Ticket.code2string.items():
            if v == status:
                return k
        return None

    def to_dict(self):
        return {"filename": self.filename,
                "status": Ticket.status_to_string(self.status),
                "time_created": self.time_created,
                "cwd": self.cwd,
                "retries": self.retries}

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(obj):
        fields = ['filename',
                  'status',
                  'time_created',
                  'cwd',
                  'retries']
        cobj = {str(k): str(value)
                for k, value in obj.items()
                if str(k) in fields}
        cobj['status'] = Ticket.string_to_status(cobj['status'])
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
        if not self.dm_irods_config.is_configured:
            raise RuntimeError('failed to read config from %s' %
                               self.dm_irods_config.config_file)

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
                        self.tickets[ticket.filename] = ticket
                        if ticket.is_active():
                            self.active_tickets[ticket.filename] = ticket
                        self.logger.info(ticket.to_json())

    def process(self, code, data):
        obj = json.loads(data)
        if "get" in obj:
            obj = json.loads(data)
            return self.process_get(obj)
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
        obj = json.loads(data)
        if "list" in obj:
            for code, item in self.process_list(obj):
                yield code, item
        else:
            yield (ReturnCode.ERROR,
                   ("invalid command %s" % json.dumps(data)))

    def process_get(self, obj):
        fname = obj["get"]
        try:
            fname = fname.encode()
        except Exception:
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": "cannot encode unicode"})
        if isinstance(fname, str):
            return (ReturnCode.OK, self.get_file(fname))
        else:
            try:
                s = str(obj["get"])
            except Exception:
                s = "[object]"
            return (ReturnCode.ERROR,
                    {"code": DmIRodsServer.FAILED,
                     "msg": "invalid type: %s" % s})

    def process_list(self, obj):
        irods = iRODS(logger=self.logger, **self.config['irods'])
        missing_tickets = {}
        for filename, ticket in self.tickets.items():
            if ticket.status != Ticket.DONE:
                missing_tickets[filename] = ticket
        for obj in irods.list_objects():
            filename = obj.get('collection', '') + '/' + obj.get('object')
            if filename in self.tickets:
                if filename in missing_tickets:
                    del missing_tickets[filename]
                for k, v in self.tickets[filename].to_dict().items():
                    obj[k] = v
            yield ReturnCode.OK, json.dumps(obj)
        for filename, ticket in missing_tickets.items():
            obj = {"object": os.path.basename(filename),
                   "collection": os.path.dirname(filename),
                   "meta": {'SURF-DMF': "???"}}
            for k, v in ticket.to_dict().items():
                obj[k] = v
            yield ReturnCode.OK, json.dumps(obj)

    def get_file(self, f):
        if f in self.tickets:
            ticket = self.tickets[f]
            if ticket.is_active():
                return {"file": f,
                        "ticket": ticket.to_dict(),
                        "code": DmIRodsServer.ALREADY_REGISTERED,
                        "msg": "already registered"}
            else:
                try:
                    ticket = self.register_ticket(f)
                    return {"file": f,
                            "ticket": ticket.to_dict(),
                            "code": DmIRodsServer.RESCHEDULED,
                            "msg": "rescheduled"}
                except Exception as ex:
                    self.logger.error(traceback.format_exc())
                    return {"file": f,
                            "ticket": None,
                            "code": DmIRodsServer.FAILED,
                            "msg": str(ex)}
        else:
            try:
                ticket = self.register_ticket(f)
                return {"file": f,
                        "ticket": ticket.to_dict(),
                        "code": DmIRodsServer.OK,
                        "msg": "scheduled"}
            except Exception as ex:
                self.logger.error(traceback.format_exc())
                return {"file": f,
                        "ticket": None,
                        "code": DmIRodsServer.FAILED,
                        "msg": str(ex)}

    def register_ticket(self, f):
        ticket = Ticket(f)
        ticket_file = f.replace('/', '#') + ".json"
        self.tickets[f] = ticket
        self.active_tickets[f] = ticket
        with open(os.path.join(self.ticket_dir,
                               ticket_file), "w") as fp:
            fp.write(ticket.to_json())
        return ticket

    def update_ticket(self, p):
        ticket = self.tickets[p]
        ticket_file = ticket.filename.replace('/', '#') + ".json"
        with open(os.path.join(self.ticket_dir,
                               ticket_file), "w") as fp:
            fp.write(ticket.to_json())

    def delete_ticket(self,  p):
        ticket = self.tickets[p]
        ticket_file = os.path.join(self.ticket_dir,
                                   ticket.filename.replace('/', '#') + ".json")
        self.logger.info('remove ticket for %s', p)
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
                irods = iRODS(logger=self.logger, **self.config['irods'])
                try:
                    self.logger.info('get %s' % p)
                    self.tickets[p].status = Ticket.GETTING
                    irods.get(ticket)
                    self.logger.info('done %s' % p)
                    self.tickets[p].status = Ticket.DONE
                    del self.active_tickets[p]
                    self.update_ticket(p)
                except RULE_FAILED_ERR as e:
                    self.logger.debug('failed rule %s', str(e))
                except NetworkException as e:
                    self.tickets[p].status == Ticket.RETRY
                    if self.tickets[p].retries > 0:
                        self.logger.warning('failed to get %s,' +
                                            'remaining %d trials',
                                            p, self.tickets[p].retries)
                        self.tickets[p].retries -= 1
                        self.update_ticket(p)
                    else:
                        self.logger.error('failed to get %s', p)
                        self._log_exception(e, traceback.format_exc())
                        self.tickets[p].status = Ticket.ERROR
                        del self.active_tickets[p]
                        self.update_ticket(p)
                except Exception as e:
                    self.logger.error('failed to get %s', p)
                    self._log_exception(e, traceback.format_exc())
                    self.tickets[p].status = Ticket.ERROR
                    del self.active_tickets[p]
                    self.update_ticket(p)

    def housekeeping(self):
        curr = time.time()
        keep_seconds = self.config.get('housekeeping', 24) * 3600
        if curr - self.last_housekeeping > self.housekeeping_interval:
            self.logger.info('housekeeping')
            tickets = {p: t for p, t in self.tickets.items()}
            try:
                irods = iRODS(logger=self.logger, **self.config['irods'])
                for obj in irods.list_objects():
                    filename = "%s/%s" % (obj.get('collection', ''),
                                          obj.get('object'))
                    if filename in self.tickets:
                        del tickets[filename]
                for p, ticket in tickets.items():
                    age = time.time() - ticket.time_created
                    if age > keep_seconds:
                        self.delete_ticket(p)
            except Exception as e:
                self.logger.error('housekeeping failed')
                self._log_exception(e, traceback.format_exc())
            self.last_houskeeping = curr

    def _log_exception(self, e, tb):
        self.logger.error(e.__class__.__name__)
        self.logger.error(str(e))
        for line in tb.split('\n'):
            self.logger.error(line)
