import logging
import base64
from irods.session import iRODSSession
from irods.models import Collection
from irods.models import DataObject
from irods.models import DataObjectMeta
from irods.models import Resource
from irods.column import Like
import irods.keywords as kw


class iRODS(object):
    def __init__(self,
                 logger=logging.getLogger("DmIRodsServer"),
                 connection_timeout=10,
                 resource_name='arcRescSURF01',
                 **kwargs):
        if 'irods_password' in kwargs:
            pw = base64.b64decode(kwargs['irods_password'])
            kwargs['irods_password'] = pw
        self.session = iRODSSession(connection_timeout=connection_timeout,
                                    **kwargs)
        self.session.connection_timeout = connection_timeout
        self.resource_name = resource_name
        self.logger = logger

    def list_objects(self):
        session = self.session
        query = session.query(Collection.name,
                              DataObject.name,
                              Resource.name)
        query = query.filter(Resource.name == self.resource_name)
        query = query.order_by(Collection.name, DataObject.name)
        for item in query.all():
            meta = {}
            mquery = session.query(DataObjectMeta.name,
                                   DataObjectMeta.value)
            mquery = mquery.filter(Collection.name == item[Collection.name])
            mquery = mquery.filter(DataObject.name == item[DataObject.name])
            mquery = mquery.filter(Like(DataObjectMeta.name, "SURF-%"))
            for m in mquery.all():
                meta[m[DataObjectMeta.name]] = m[DataObjectMeta.value]
            yield {'collection': item[Collection.name],
                   'object': item[DataObject.name],
                   'resource_value': item[Resource.name],
                   'meta': meta}

    def get(self, ticket):
        chunk_size = 1024
        self.logger.info('iget %s -> %s',
                         ticket.remote_file, ticket.local_file)
        obj = self.session.data_objects.get(ticket.remote_file)
        with obj.open('r') as f:
            with open(ticket.local_file, 'wb') as fo:
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    fo.write(data)

    def put(self, ticket):
        target = ticket.remote_file.format(zone=self.session.zone,
                                           user=self.session.username)
        self.logger.info('iput %s -> %s', ticket.local_file, target)
        self.session.default_resource = self.resource_name
        with open(ticket.local_file, 'r') as fin:
            with self.session.data_objects.open(target, 'w') as fout:
                while True:
                      chunk = fin.read(1024)
                      if chunk:
                          fout.write(chunk)
                      else:
                          break
