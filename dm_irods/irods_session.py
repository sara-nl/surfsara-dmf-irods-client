import io
import logging
import base64
import hashlib
import datetime
from irods.session import iRODSSession
from irods.models import Collection
from irods.models import DataObject
from irods.models import DataObjectMeta
from irods.models import Resource
from irods.column import Like
from irods import keywords as kw


PUT_BLOCK_SIZE = 1024
GET_BLOCK_SIZE = 1024


class iRODS(object):
    def __init__(self,
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

    def __enter__(self):
        self.session = iRODSSession(connection_timeout=self.connection_timeout,
                                    **self.kwargs)
        self.session.connection_timeout = self.connection_timeout
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.cleanup()

    def sha256_checksum(self, filename, block_size=65536):
        def chunks(f, chunksize=io.DEFAULT_BUFFER_SIZE):
            return iter(lambda: f.read(chunksize), b'')

        hasher = hashlib.sha256()
        with open(filename, 'rb') as f:
            for chunk in chunks(f):
                hasher.update(chunk)
        return base64.b64encode(hasher.digest())

    def list_objects(self, filters={}, limit=-1):
        session = self.session
        fields = {
            'collection': Collection.name,
            'object': DataObject.name,
            'resource_value': Resource.name,
            'remote_replica_number': DataObject.replica_number,
            'remote_version': DataObject.version,
            'remote_type': DataObject.type,
            'remote_size': DataObject.size,
            'remote_owner_name': DataObject.owner_name,
            'remote_owner_zone': DataObject.owner_zone,
            'remote_replica_status': DataObject.replica_status,
            'remote_status': DataObject.status,
            'remote_checksum': DataObject.checksum,
            'remote_expiry': DataObject.expiry,
            'remote_create_time': DataObject.create_time,
            'remote_modify_time': DataObject.modify_time
        }
        query = session.query(*fields.values())
        query = query.filter(Resource.name == self.resource_name)
        for k, value in filters.items():
            query = query.filter(fields[k] == value)
        query = query.order_by(Collection.name, DataObject.name)
        if limit != -1:
            query = query.limit(limit)
        for item in query.all():
            mquery = session.query(DataObjectMeta.name,
                                   DataObjectMeta.value)
            mquery = mquery.filter(Collection.name == item[Collection.name])
            mquery = mquery.filter(DataObject.name == item[DataObject.name])
            mquery = mquery.filter(Like(DataObjectMeta.name, "SURF-%"))
            res = {}
            for m in mquery.all():
                res['meta_' + m[DataObjectMeta.name]] = m[DataObjectMeta.value]
            for k, v in fields.items():
                val = item[v]
                if isinstance(val, datetime.datetime):
                    val = (val - datetime.datetime(1970, 1, 1)).total_seconds()
                res[k] = val
            yield res

    def get(self, ticket):
        self.logger.info('iget %s -> %s',
                         ticket.remote_file, ticket.local_file)
        remote_file = ticket.remote_file
        obj = self.session.data_objects.get(remote_file)
        ticket.transferred = 0
        mb = 1024 * 1024
        with obj.open('r') as f:
            with open(ticket.local_file, 'wb') as fo:
                while True:
                    chunk = f.read(GET_BLOCK_SIZE)
                    if chunk:
                        ticket.transferred += len(chunk)
                        if ticket.transferred % (1024 * 1024 * 10) == 0:
                            self.logger.info('retrieved %3.1f MB from %s',
                                             (ticket.transferred / mb),
                                             remote_file)
                        fo.write(chunk)
                    else:
                        self.logger.info('retrieved file %s',
                                         ticket.local_file)
                        break
        ticket.update_local_checksum()
        self.checksum(ticket, remote_file)

    def put(self, ticket):
        target = ticket.remote_file
        self.logger.info('iput %s -> %s', ticket.local_file, target)
        self.session.default_resource = self.resource_name
        ticket.update_local_checksum()
        ticket.transferred = 0
        mb = 1024 * 1024
        self.logger.info('checksum %s', ticket.checksum)
        with open(ticket.local_file, 'r') as fin:
            options = {kw.REG_CHKSUM_KW: ''}
            with self.session.data_objects.open(target, 'w',
                                                **options) as fout:
                while True:
                    chunk = fin.read(PUT_BLOCK_SIZE)
                    if chunk:
                        ticket.transferred += len(chunk)
                        if ticket.transferred % (1024 * 1024 * 10) == 0:
                            self.logger.info('sent %3.1f MB',
                                             (ticket.transferred / mb))
                        fout.write(chunk)
                    else:
                        self.logger.info('sent file %s', ticket.local_file)
                        break
        ticket.update_local_attributes()
        self.checksum(ticket, target)

    def checksum(self, ticket, remote_file):
        obj = self.session.data_objects.get(remote_file)
        if obj.checksum is not None:
            chcksum = ticket.checksum
            if obj.checksum != "sha2:{checksum}".format(checksum=chcksum):
                self.logger.error('obj.checksum  %s', obj.checksum)
                self.logger.error('file checksum %s', chcksum)
                raise ValueError('checksum test failed')
