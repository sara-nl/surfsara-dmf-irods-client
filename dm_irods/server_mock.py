import io
import os
import re
import json
import logging
import shutil
import hashlib
import base64
from .server import DmIRodsServer


class iRODSMockSession(object):
    def __init__(self, zone, username):
        self.zone = zone
        self.username = username


class iRODSMock(object):
    def __init__(self,
                 server,
                 logger=logging.getLogger("DmIRodsServer"),
                 connection_timeout=10,
                 resource_name='arcRescSURF01',
                 **kwargs):
        if 'irods_password' in kwargs:
            pw = base64.b64decode(kwargs['irods_password'])
            kwargs['irods_password'] = pw
        self.logger = logger
        self.kwargs = kwargs
        self.connection_timeout = connection_timeout
        self.resource_name = resource_name
        self.logger = logger
        self.server = server

    def __enter__(self):
        self.session = iRODSMockSession(self.server.zone,
                                        self.server.user)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def sha256_checksum(self, filename, block_size=65536):
        def chunks(f, chunksize=io.DEFAULT_BUFFER_SIZE):
            return iter(lambda: f.read(chunksize), b'')

        sha256 = hashlib.sha256()
        with open(filename, 'rb') as f:
            for chunk in chunks(f, block_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def list_objects(self):
        for root, dir, files in os.walk(self.server.mockdir):
            for item in files:
                if re.match('^__.*.json', item):
                    with open(os.path.join(root, item), 'r') as f:
                        meta_data = json.load(f)
                        state = meta_data.get('state', '???')
                        collection = os.path.dirname(meta_data['file'])
                        obj = os.path.basename(meta_data['file'])
                        yield {'collection': collection,
                               'object': obj,
                               'resource_value': self.server.resource,
                               "meta_SURF-DMF": state}

    def get(self, ticket):
        local_file = ticket.local_file
        remote_file = ticket.remote_file.format(zone=self.server.zone,
                                                user=self.server.user)
        data_file = os.path.join(self.server.mockdir,
                                 remote_file.replace('/', '#'))
        meta_data_file = os.path.join(self.server.mockdir,
                                      '__' +
                                      remote_file.replace('/', '#') +
                                      '.json')
        if os.path.isfile(meta_data_file):
            with open(meta_data_file, 'r') as f:
                meta_data = json.load(f)
        else:
            raise IOError('could not find meta data file: %s' % meta_data_file)

        self.logger.info('copy %s -> %s', data_file, local_file)
        shutil.copy(data_file, local_file)
        if 'checksum' in meta_data:
            if meta_data['checksum'] != self.sha256_checksum(local_file):
                raise ValueError('checksum  test failed')

    def put(self, ticket):
        local_file = ticket.local_file
        remote_file = ticket.remote_file.format(zone=self.server.zone,
                                                user=self.server.user)
        data_file = os.path.join(self.server.mockdir,
                                 remote_file.replace('/', '#'))
        meta_data_file = os.path.join(self.server.mockdir,
                                      '__' +
                                      remote_file.replace('/', '#') +
                                      '.json')
        self.logger.info('copy %s -> %s', local_file, data_file)
        chcksum = self.sha256_checksum(ticket.local_file)
        self.logger.info('checksum %s', chcksum)
        shutil.copy(local_file, data_file)
        mode = os.stat(data_file)

        if os.path.isfile(meta_data_file):
            with open(meta_data_file, 'r') as f:
                meta_data = json.load(f)
        else:
            meta_data = {'state': 'MIG',
                         'inode': mode.st_ino,
                         'file': remote_file}
        if meta_data['state'] == 'REG':
            meta_data['state'] = 'MIG'
        meta_data['checksum'] = chcksum
        with open(meta_data_file, 'w') as f:
            json.dump(meta_data, f)


class DmIRodsServerMock(DmIRodsServer):
    @classmethod
    def get_system_name(cls):
        return "DmIRodsServer"

    def __init__(self, socket_file, **kwargs):
        super(DmIRodsServerMock, self).__init__(socket_file, **kwargs)
        self.mockdir = os.path.join(os.path.dirname(socket_file), "mock_data")
        if not os.path.exists(self.mockdir):
            self.logger.info("creating directory %s", self.mockdir)
            os.makedirs(self.mockdir)

    def irods_connection(self):
        return iRODSMock(self,
                         logger=self.logger, **self.config['irods'])
