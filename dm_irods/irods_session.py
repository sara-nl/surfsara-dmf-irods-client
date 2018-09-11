import os
import logging
import base64
from irods.session import iRODSSession
from irods.models import Collection
from irods.models import DataObject
from irods.models import DataObjectMeta
from irods.models import Resource
from irods.column import Like


class iRODS(object):
    def __init__(self,
                 logger=logging.getLogger("DmIRodsServer"),
                 connection_timeout=10,
                 resource_name='arcRescSURF01',
                 **kwargs):
        if 'irods_password' in kwargs:
            pw = base64.b64decode(kwargs['irods_password'])
            kwargs['irods_password'] = pw
        self.session = iRODSSession(**kwargs)
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
        target = os.path.join(ticket.cwd, os.path.basename(ticket.filename))
        self.logger.info('iget %s -> %s', ticket.filename, target)
        obj = self.session.data_objects.get(ticket.filename)
        with obj.open('r') as f:
            with open(target, 'wb') as fo:
                while True:
                    data = f.read(chunk_size)
                    if not data:
                        break
                    fo.write(data)
