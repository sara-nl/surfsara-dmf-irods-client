import sys
import os
import logging
import json
import traceback
import time
sys.path.insert(0,
                os.path.join(
                    os.path.dirname(os.path.dirname(__file__)),
                    "py-socket-server"))
from socket_server import Server
from socket_server import ReturnCode


class Ticket(object):
    WAITING = 1
    CANCELED = 2
    PROCESSING = 3
    DONE = 4
    UNDEF = 5

    def __init__(self, filename, status=WAITING, time_created=None):
        self.status = status
        self.filename = filename
        self.time_created = time_created
        if self.time_created is None:
            self.time_created = time.time()

    def is_active(self):
        return (self.status == Ticket.WAITING or
                self.status == Ticket.PROCESSING)

    def step(self, logger=logging.getLogger("Daemon")):
        logger.info('check %s' % self.to_json())

    @staticmethod
    def status_to_string(status):
        if status == Ticket.WAITING:
            return "WAITING"
        elif status == Ticket.CANCELED:
            return "CANCELED"
        elif status == Ticket.PROCESSING:
            return "PROCESSING"
        elif status == Ticket.DONE:
            return "DONE"
        elif status == Ticket.UNDEF:
            return "UNDEF"

    @staticmethod
    def string_to_status(status):
        if status == "WAITING":
            return Ticket.WAITING
        elif status == "CANCELED":
            return Ticket.CANCELED
        elif status == "PROCESSING":
            return Ticket.PROCESSING
        elif status == "DONE":
            return Ticket.DONE
        elif status == "UNDEF":
            return Ticket.UNDEF

    def to_dict(self):
        return {"filename": self.filename,
                "status": Ticket.status_to_string(self.status),
                "time_created": self.time_created}

    def to_json(self):
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(obj):
        fields = ['filename',
                  'status',
                  'time_created']
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

    @staticmethod
    def get_socket_file():
        return os.path.join(os.path.expanduser("~"),
                            ".DmIRodsServer", "DmIRodsServer.socket")

    def __init__(self, socket_file, **kwargs):
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

    def read_tickets(self):
        for root, dirs, files in os.walk(self.ticket_dir):
            for file in files:
                if file.endswith(".json"):
                    ticket_file = os.path.join(root, file)
                    self.logger.info("reading ticket from file %s" %
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
        elif "list" in obj:
            return (ReturnCode.OK, self.process_list(obj))
        else:
            return (ReturnCode.ERROR,
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
        return {"tickets": [ticket.to_dict()
                            for f, ticket in self.tickets.items()]}

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
