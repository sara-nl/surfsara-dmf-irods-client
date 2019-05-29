import os
import sys
import json
import time
import traceback
from .irods_session import iRODS
from .irods_session import GetDmfObject
from .config import DmIRodsConfig
from irods.exception import NetworkException
from irods.exception import RULE_FAILED_ERR
from .socket_server.server import Server
from .socket_server.server_app import ServerApp
from .socket_server.util import ReturnCode
from .ticket import Ticket
from .cprint import print_error


class DmIRodsServer(Server):
    # succ codes
    OK = 0
    RESCHEDULED = 1

    # err codes
    ALREADY_REGISTERED = 2
    FAILED = 3

    TICK_INTERVAL = 10
    HOUSEKEEPING_INTERVAL = 3600
    # HOUSEKEEPING_INTERVAL = 10

    LIST_BUFF_SIZE = 10

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

        # ticket configuration
        self.ticket_dir = os.path.join(os.path.expanduser("~"),
                                       ".DmIRodsServer",
                                       "Tickets")
        self.tickets = {}
        self.active_tickets = {}

        if not os.path.exists(self.ticket_dir):
            os.makedirs(self.ticket_dir)
        self.read_tickets()

        # check if system is configured config
        self.dm_irods_config = DmIRodsConfig(logger=self.logger)
        if not self.dm_irods_config.is_configured:
            raise RuntimeError('failed to read config from %s' %
                               self.dm_irods_config.config_file)
        self.config = self.dm_irods_config.config

        # houskeeping
        self.last_housekeeping = time.time()
        self.housekeeping_interval = DmIRodsServer.HOUSEKEEPING_INTERVAL

        # read irods configuiration for irods env
        if 'irods_env_file' in self.config.get('irods', {}):
            with open(self.config['irods']['irods_env_file']) as f:
                cfg = json.load(f)
        else:
            cfg = self.config

        # setup zone, user and resource
        self.zone = cfg['irods_zone_name']
        self.user = cfg['irods_user_name']
        self.default_path = "/{zone}/home/{user}".format(zone=self.zone,
                                                         user=self.user)
        self.resource = self.config.get('irods', {}).get('resource_name', '')

        # configure
        self.stop_timeout = self.config.get('stop_timeout', 0) * 60
        self.heartbeat = time.time()

        # managing remote completion list
        self.completion_list = []
        self.completion_list_updated = 0
        self.completion_list_timeout = 60

    def irods_connection(self):
        """
        Create iRODS session object
        """
        return iRODS(self.dm_irods_config.config_file,
                     self.dm_irods_config.irods_auth_file,
                     connection_timeout=self.config.get('connection_timeout',
                                                        None),
                     resource_name=self.config.get('resource_name',
                                                   None),
                     is_resource_server=self.config.get('is_resource_server',
                                                        None),
                     logger=self.logger)

    def read_tickets(self):
        for root, dirs, files in os.walk(self.ticket_dir):
            for file in files:
                if file.endswith(".json"):
                    self.read_ticket_from_file(os.path.join(root, file))

    def read_ticket_from_file(self, ticket_file):
        self.logger.info("reading ticket from file %s", ticket_file)
        try:
            with open(ticket_file, "r") as f:
                data = json.load(f)
            ticket = Ticket.from_json(data)
            if ticket.status in [Ticket.GETTING, Ticket.PUTTING]:
                ticket.retry()
                ticket.retries = 3
            p = (ticket.local_file, ticket.remote_file)
            self.tickets[p] = ticket
            if ticket.is_active():
                self.active_tickets[p] = ticket
        except Exception as ex:
            self.logger.error("failed to read from file %s:", ticket_file)
            self.logger.error("failed to read from file %s:", str(ex))
            raise

        if ticket.mode == Ticket.PUT:
            ticket.update_local_attributes()
            with open(ticket_file, 'w') as f:
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
            if sys.version_info[0] == 2:
                remote_file = remote_file.encode()
                local_file = local_file.encode()
            remote_file = remote_file.format(zone=self.zone,
                                             user=self.user)
            if not os.path.isabs(remote_file):
                remote_file = os.path.join(self.default_path, remote_file)
        except Exception as e:
            msg = "cannot encode unicode: {}".format(str(e))
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": msg,
                                       "exception": e.__class__.__name__,
                                       "traceback": traceback.format_exc()})
        if isinstance(remote_file, str):
            return (ReturnCode.OK,
                    self.register_ticket(local_file, remote_file, Ticket.GET))
        else:
            try:
                s = str(obj["get"])
            except Exception as e:
                s = "[object]"
                return (ReturnCode.ERROR,
                        {"code": DmIRodsServer.FAILED,
                         "msg": "invalid type: %s" % s,
                         "exception": e.__class__.__name__,
                         "traceback": traceback.format_exc()})
            return (ReturnCode.ERROR,
                    {"code": DmIRodsServer.FAILED,
                     "msg": "invalid type: %s" % s})

    def process_put(self, obj):
        remote_file = obj["remote_file"]
        local_file = obj['put']

        try:
            if sys.version_info[0] == 2:
                remote_file = remote_file.encode()
                local_file = local_file.encode()
            remote_file = remote_file.format(zone=self.zone,
                                             user=self.user)
        except Exception as e:
            msg = "cannot encode unicode: {}".format(str(e))
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": msg,
                                       "exception": e.__class__.__name__,
                                       "traceback": traceback.format_exc()})
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
            if sys.version_info[0] == 2:
                remote_file = remote_file.encode()
        except Exception as e:
            msg = "cannot encode unicode: {}".format(str(e))
            return (ReturnCode.ERROR, {"code": DmIRodsServer.FAILED,
                                       "msg": msg,
                                       "exception": e.__class__.__name__,
                                       "traceback": traceback.format_exc()})

        filters = {'object': os.path.basename(remote_file),
                   'collection': os.path.dirname(remote_file)}
        for code, item in self.process_list_dict({'limit': 1,
                                                  'filter': filters}):
            if item.get('remote_file') == remote_file:
                return ReturnCode.OK, json.dumps(item)
        return ReturnCode.OK, {}

    def process_list_dict(self, obj):
        def check_locally_deleted(item):
            # check if file has been deleted:
            if (item.get('local_size', None) is None and
                item.get('local_file', None) is not None):
                item['local_file'] = 'DELETED:' + item['local_file']

        limit = obj.get('limit', None)
        flt = obj.get('filter', {})
        if limit is None:
            limit = 1000000000
            arglimit = -1
        else:
            arglimit = limit * 2

        with self.irods_connection() as irods:
            rule = GetDmfObject(irods)
            tickets_done = {}
            for item in rule.process_all(self.list_tickets(flt)):
                remote_file = item.get('remote_file')
                tickets_done[remote_file] = True
                limit -= 1
                check_locally_deleted(item)
                yield ReturnCode.OK, item
                if limit == 0:
                    break

        # then check if there are objects without tickets
        if limit > 0 and not flt.get('active', False):
            with self.irods_connection() as irods:
                rule = GetDmfObject(irods)
                lst_func = self.list_objects
                for item in rule.process_all(lst_func(tickets_done,
                                                      limit=arglimit)):
                    limit -= 1
                    check_locally_deleted(item)
                    yield ReturnCode.OK, item
                    if limit == 0:
                        break

    def process_list(self, obj):
        for s, item in self.process_list_dict(obj):
            yield s, json.dumps(item)

    def list_tickets(self, flt={}):
        def sort_key(x):
            return Ticket.sorted_codes.index(x.status)

        def filter_ticket(x):
            if flt.get('active', False):
                return x.is_active()
            else:
                return True

        ticket_list = sorted([t for t in self.tickets.values()
                              if filter_ticket(t)],
                             key=lambda x: x.time_created)
        ticket_list = sorted(ticket_list,
                             key=sort_key, reverse=False)
        for ticket in ticket_list:
            item = ticket.to_dict()
            remote_file = item.get('remote_file')
            item['collection'] = os.path.dirname(remote_file)
            item['object'] = os.path.basename(remote_file)
            yield item

    def list_objects(self, tickets_done={}, filters={}, limit=-1):
        """
        Return a generator over all objects of the resource.
        ticket_done is a ignore list
        """
        with self.irods_connection() as irods:
            for item in irods.list_objects(filters=filters, limit=limit):
                if item['remote_file'] not in tickets_done:
                    yield item

    def process_completion_list(self, obj):
        now = time.time()
        age = now - self.completion_list_updated
        prefix = obj.get('completion_list', '')
        if age > self.completion_list_timeout:
            self.completion_list = []
            self.completion_list_updated = now
            with self.irods_connection() as irods:
                for obj in irods.list_objects():
                    filename = "%s/%s" % (obj.get('collection', ''),
                                          obj.get('object'))
                    self.completion_list.append(filename)
        for filename in self.completion_list:
            if filename.startswith(prefix):
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
        tjson = ticket.to_json()
        tfile = os.path.join(self.ticket_dir, ticket.ticket_file)
        self.tickets[p] = ticket
        self.active_tickets[p] = ticket
        with open(tfile, "w") as fp:
            fp.write(tjson)
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
        keys = list(self.active_tickets.keys())
        for p in keys:
            ticket = self.active_tickets[p]
            if not self.active:
                break
            if ticket.status in [Ticket.UNMIG, Ticket.WAITING, Ticket.RETRY]:
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
                self.logger.info('done %s -> %s (%d s)',
                                 p[1],
                                 p[0],
                                 ticket.transfer_time)
                self.tickets[p].status = Ticket.DONE
                del self.active_tickets[p]
                self.update_ticket(p[0], p[1])
            except RULE_FAILED_ERR as e:
                # state unmigrate
                self.active_tickets[p].unmig()
                self.logger.debug('failed rule %s', str(e))
            except NetworkException as e:
                fmt = 'failed to get {remote} -> {local}'
                self._transfer_network_handling(p, e, fmt)
            except Exception as e:
                fmt = 'failed to get {remote} -> {local}'
                self._transfer_exception_handling(p, e, fmt)
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
                self.logger.info('done %s -> %s (%f s)',
                                 p[0],
                                 p[1],
                                 ticket.transfer_time)
                self.tickets[p].status = Ticket.DONE
                del self.active_tickets[p]
                self.update_ticket(p[0], p[1])
            except NetworkException as e:
                fmt = 'failed to put {local} -> {remote}'
                self._transfer_network_handling(p, e, fmt)
            except Exception as e:
                fmt = 'failed to put {local} -> {remote}'
                self._transfer_exception_handling(p, e, fmt)
        self.heartbeat = time.time()

    def _transfer_network_handling(self, p, excep, fmt):
        self.tickets[p].status == Ticket.RETRY
        errmsg = fmt.format(local=p[1], remote=p[0]) + ':'
        errmsg += '\n' + self._exception2string(excep, traceback.format_exc())
        if self.tickets[p].retries > 0:
            self.logger.warning(errmsg)
            self.logger.warning('remaining %d trials', self.tickets[p].retries)
            errmsg += '\nremaining %d trials' % self.tickets[p].retries
            self.tickets[p].retry()
            self.tickets[p].errmsg = errmsg
            self.tickets[p].retries -= 1
            self.update_ticket(p[0], p[1])
        else:
            self.logger.error(errmsg)
            self._log_exception(excep, traceback.format_exc())
            self.tickets[p].status = Ticket.ERROR
            self.tickets[p].errmsg = errmsg
            del self.active_tickets[p]
            self.update_ticket(p[0], p[1])

    def _transfer_exception_handling(self, p, excep, fmt):
        errmsg = fmt.format(local=p[1], remote=p[0]) + ':'
        errmsg += '\n' + self._exception2string(excep, traceback.format_exc())
        self.logger.error(errmsg)
        self._log_exception(excep, traceback.format_exc())
        self.tickets[p].status = Ticket.ERROR
        self.tickets[p].errmsg = errmsg
        del self.active_tickets[p]
        self.update_ticket(p[0], p[1])

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

    def _exception2string(self, e, tb):
        msg = e.__class__.__name__ + ': ' + str(e)
        msg += '\n' + tb
        return msg

    def _log_exception(self, e, tb):
        self.logger.error(e.__class__.__name__)
        self.logger.error(str(e))
        for line in tb.split('\n'):
            self.logger.error(line)


def ensure_daemon_is_running():
    app = ServerApp(DmIRodsServer,
                    socket_file=DmIRodsServer.get_socket_file(),
                    verbose=False)
    config = DmIRodsConfig(logger=app.logger)
    if not config.is_configured:
        app.stop()
    config.ensure_configured()
    try:
        app.start()
    except Exception as e:
        print(traceback.format_exc())
        print_error(str(e), box=True)
        sys.exit(8)


def dm_idaemon(argv=sys.argv[1:]):
    app = ServerApp(DmIRodsServer,
                    module='dm_irods.server',
                    socket_file=DmIRodsServer.get_socket_file())
    app.main(argv)


if __name__ == "__main__":
    dm_idaemon()
