import os
import json
import time
import sys
from .socket_server.server import Server
from .socket_server.server import ReturnCode
from .socket_server.server_app import ServerApp


class DmMockServer(Server):
    @staticmethod
    def get_socket_file():
        return os.path.join(os.path.expanduser("~"),
                            ".DmMockServer", "DmMockServer.socket")

    @staticmethod
    def get_dm_data_dir():
        return os.path.join(os.path.expanduser("~"),
                            ".DmMockServer", "data")

    def __init__(self, socket_file, **kwargs):
        kwargs['tick_sec'] = 10
        super(DmMockServer, self).__init__(DmMockServer.get_socket_file(),
                                           **kwargs)
        self.inodes = {}
        dirname = DmMockServer.get_dm_data_dir()
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        self.read_data()
        self.default_mig_time = 10
        self.default_unmig_time = 10
        if sys.version_info[0] == 2:
            self.fhandle = os.urandom(32).encode('hex')
        else:
            self.fhandle = os.urandom(32).hex()
        self.default_owner = 45953

    def read_data(self):
        for root, dirs, files in os.walk(DmMockServer.get_dm_data_dir()):
            for f in files:
                if f.endswith(".json"):
                    ticket_file = os.path.join(root, f)
                    with open(ticket_file) as f:
                        data = json.load(f)
                        self.inodes[data['inode']] = data

    def check_delay(self, inode):
        if 'change_time' in inode and 'change_duration' in inode:
            now = int(time.time())
            time_diff = now - inode['change_time']
            if time_diff > inode['change_duration']:
                return True
            else:
                return False
        else:
            return True

    def tick(self):
        for k, inode in self.inodes.items():
            if inode['state'] == 'MIG':
                if self.check_delay(inode):
                    if inode.get('remove', False):
                        inode['state'] = 'OFL'
                    else:
                        inode['state'] = 'DUL'
                    self.update_inode(inode)
            elif inode['state'] == 'UNM':
                if self.check_delay(inode):
                    inode['state'] = 'DUL'
                    self.update_inode(inode)

    def process(self, code, data):
        obj = json.loads(data)
        if obj.get('op') == 'ls':
            return (ReturnCode.OK, self.ls_inode(obj.get('path')))
        elif obj.get('op') == 'get':
            return (ReturnCode.OK, self.get_inode(obj.get('path')))
        elif obj.get('op') == 'put':
            return (ReturnCode.OK, self.put_inode(obj.get('path'),
                                                  obj.get('remove', False)))
        elif obj.get('op') == 'is_in_state':
            return (ReturnCode.OK,
                    self.is_in_state(obj.get('paths'),
                                     obj.get('states')))

    def ls_inode(self, path):
        inode = os.stat(path).st_ino
        data_path = os.path.join(DmMockServer.get_dm_data_dir(),
                                 "%d.json" % inode)
        if inode in self.inodes:
            return self.inodes[inode]
        else:
            return {'state': 'REG',
                    'inode': inode,
                    '_path': path,
                    '_filename': data_path,
                    'bfid': 0,
                    'fhandle': self.fhandle,
                    'flags': 0,
                    'nregn': 0,
                    'owner': self.default_owner,
                    'projid': 0,
                    'sitetag': 0,
                    'size': 0,
                    'space': 0}

    def get_inode(self, path):
        inode = os.stat(path).st_ino
        obj = self.ls_inode(inode)
        if obj.get('state') == 'MIG':
            obj['remove'] = False
            self.update_inode(obj)
        elif obj.get('state') == 'OFL':
            obj['state'] = 'UNM'
            obj['change_duration'] = self.default_unmig_time
            self.update_inode(obj)
        return obj

    def put_inode(self, path, remove):
        obj = self.ls_inode(path)
        if obj['state'] == 'REG':
            obj['state'] = 'MIG'
            obj['remove'] = remove
            obj['change_duration'] = self.default_mig_time
            self.generate_bfid(obj)
            self.update_inode(obj)
        elif obj['state'] == 'DUL' and remove:
            obj['state'] = 'OFL'
            self.update_inode(obj)
        return obj

    def generate_bfid(self, obj):
        obj['bfid'] = os.urandom(32).encode('hex')
        obj['emask'] = 17000
        obj['fhandle'] = self.fhandle
        obj['flags'] = 0
        obj['owner'] = self.default_owner
        obj['nregn'] = 1
        obj['projid'] = 0
        obj['sitetag'] = 0
        obj['size'] = os.stat(obj['_path']).st_size
        obj['space'] = 0

    def is_in_state(self, paths, states):
        inodes = [os.stat(path).st_ino for path in paths]
        states = [str(s) for s in states]
        for inode in [self.inodes[ind]
                      for ind in inodes
                      if ind in self.inodes]:
            if not inode['state'] in states:
                return {'is_in_state': False}
        return {'is_in_state': True}

    def update_inode(self, obj):
        obj['change_time'] = int(time.time())
        self.inodes[obj['inode']] = obj
        data_path = os.path.join(DmMockServer.get_dm_data_dir(),
                                 "%d.json" % obj['inode'])
        with open(data_path, 'w') as fp:
            json.dump(obj, fp)


if __name__ == "__main__":
    app = ServerApp(DmMockServer,
                    module='dm_irods.dm_mock_server',
                    socket_file=DmMockServer.get_socket_file())
    app.main()
