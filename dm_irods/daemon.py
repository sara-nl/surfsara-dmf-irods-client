import socket
import logging
import os
import json
import socket_utils
import threading
import time
import traceback


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


class Daemon(object):
    # succ codes
    OK = 0
    RESCHEDULED = 1

    # err codes
    ALREADY_REGISTERED = 2
    FAILED = 3

    def __init__(self, socket_file, ticket_dir,
                 logger=logging.getLogger("Daemon"), iget_timeout=60):
        self.iget_timeout = iget_timeout
        self.socket_file = socket_file
        self.socket = None
        self.logger = logger
        self.listener_thread = threading.Thread(name='listener',
                                                target=self.listener,
                                                args=())
        self.listener_thread.daemon = True
        self.active = True
        self.ticket_dir = ticket_dir
        self.tickets = {}
        self.active_tickets = {}
        if not os.path.exists(ticket_dir):
            self.logger.info("creating ticket directory %s" % ticket_dir)
            os.makedirs(ticket_dir)
        else:
            self.logger.info("ticket directory %s exists" % ticket_dir)
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

    def run(self):
        while self.active:
            for f, item in self.active_tickets.items():
                self.logger.info('check %s' % item.to_json())
            begin = time.time()
            while self.active and time.time() - begin < self.iget_timeout:
                time.sleep(1)

    def stop(self, signum=0, frame=None):
        self.active = False

    def start_listener(self):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if os.path.exists(self.socket_file):
            self.logger.info("remove old socket file %s", self.socket_file)
            os.remove(self.socket_file)
        self.logger.info("bind %s", self.socket_file)
        self.socket.bind(self.socket_file)
        self.listener_thread.start()

    def listener(self):
        self.logger.info("listen")
        self.socket.listen(1)
        while True:
            self.logger.debug('waiting for a connection')
            conn, addr = self.socket.accept()
            self.logger.debug('accepted')
            try:
                data = socket_utils.recv_message(conn)
                self.logger.debug('recvall %s', data)
                ret = self.process(data)
                if isinstance(ret, dict):
                    ret = json.dumps(ret)
                socket_utils.send_message(conn, ret)
            finally:
                conn.close()

    def process(self, data):
        obj = json.loads(data)
        if "get" in obj:
            obj = json.loads(data)
            return self.process_get(obj)
        elif "list" in obj:
            return self.process_list(obj)
        else:
            return {"code": Daemon.FAILED,
                    "msg": "invalid command %s" % json.dumps(data)}

    def process_list(self, obj):
        return {"tickets": [ticket.to_dict()
                            for f, ticket in self.tickets.items()]}

    def process_get(self, obj):
        fname = obj["get"]
        try:
            fname = fname.encode()
        except:
            return {"code": Daemon.FAILED,
                    "msg": "cannot encode unicode"}
        if isinstance(fname, str):
            return self.get_file(fname)
        else:
            try:
                s = str(obj["get"])
            except:
                s = "[object]"
            return {"code": Daemon.FAILED,
                    "msg": "invalid type: %s" % s}

    def get_file(self, f):
        if f in self.tickets:
            ticket = self.tickets[f]
            if ticket.is_active():
                return {"file": f,
                        "ticket": ticket.to_dict(),
                        "code": Daemon.ALREADY_REGISTERED,
                        "msg": "already registered"}
            else:
                try:
                    ticket = self.register_ticket(f)
                    return {"file": f,
                            "ticket": ticket.to_dict(),
                            "code": Daemon.RESCHEDULED,
                            "msg": "rescheduled"}
                except Exception as ex:
                    self.logger.error(traceback.format_exc())
                    return {"file": f,
                            "ticket": None,
                            "code": Daemon.FAILED,
                            "msg": str(ex)}
        else:
            try:
                ticket = self.register_ticket(f)
                return {"file": f,
                        "ticket": ticket.to_dict(),
                        "code": Daemon.OK,
                        "msg": "scheduled"}
            except Exception as ex:
                self.logger.error(traceback.format_exc())
                return {"file": f,
                        "ticket": None,
                        "code": Daemon.FAILED,
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
