import io
import os
import logging
import base64
import hashlib
import datetime
import json
from irods.session import iRODSSession
from irods.models import Collection
from irods.models import DataObject
from irods.models import Resource
from irods.rule import Rule
from irods import keywords as kw


PUT_BLOCK_SIZE = 1024
GET_BLOCK_SIZE = 1024


class iRODS(object):
    """
    Wrapper class for iRODS session with additional
    functions.
    """
    def __init__(self,
                 irods_config_file,
                 irods_auth_file,
                 logger=logging.getLogger("DmIRodsServer"),
                 connection_timeout=None,
                 resource_name=None,
                 is_resource_server=None):
        if connection_timeout is None:
            connection_timeout = 10
        if resource_name is None:
            resource_name = 'arcRescSURF01'
        if is_resource_server is None:
            is_resource_server = False
        if logger is None:
            logger = logging.getLogger("DmIRodsServer"),
        self.is_resource_server = is_resource_server
        self.logger = logger
        self.connection_timeout = connection_timeout
        self.resource_name = resource_name
        self.logger = logger
        self.irods_config_file = irods_config_file
        self.irods_auth_file = irods_auth_file

    def __enter__(self):
        auth_file = self.irods_auth_file
        env_file = self.irods_config_file
        self.session = iRODSSession(irods_env_file=env_file,
                                    irods_authentication_file=auth_file)
        self.session.connection_timeout = self.connection_timeout
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.cleanup()

    def get_rule_return_value(self, res, index):
        """
        Extract return value from iRODS microservice
        """
        return str(res.MsParam_PI[index].inOutStruct.myStr)

    def sha256_checksum(self, filename, block_size=65536):
        """
        Compute checksum for the contents of a file
        """
        def chunks(f, chunksize=io.DEFAULT_BUFFER_SIZE):
            return iter(lambda: f.read(chunksize), b'')

        hasher = hashlib.sha256()
        with open(filename, 'rb') as f:
            for chunk in chunks(f):
                hasher.update(chunk)
        return base64.b64encode(hasher.digest())

    def get_objects(self, lst):
        """
        Get the DMF state for a list of objects.

        lst -- A list of objects (absolute iRODS paths),
               e.g. ['/irods/path/to/object1', '/irods/path/to/object2']

        Returns:

        A list of dictionaries with mixed DMF and iRODS information.

        Example:
        [{'DMF_bfid': '0',
          ...
          'DMF_path': '/path/to/dmf/object1',
          'DMF_state': 'REG',
          ...
          'collection': u'/irods/path/to',
          'object': 'object1',
          ...},
        ...]
        """
        def transform(obj):
            ret = {'collection': os.path.dirname(obj.get('objPath', '')),
                   'object': os.path.basename(obj.get('objPath', '')),
                   'remote_file': str(obj.get('objPath', '')),
                   'resource_value': str(obj.get('rescName')),
                   'remote_replica_number': obj.get('replNum', 0),
                   'remote_version': str(obj.get('version', '')),
                   'remote_type': str(obj.get('dataType', '')),
                   'remote_size': obj.get('dataSize', 0),
                   'remote_owner_name': str(obj.get('dataOwnerName', '')),
                   'remote_owner_zone': str(obj.get('dataOwnerZone', '')),
                   'remote_replica_status': obj.get('replStatus', 1),
                   'remote_status': str(obj.get('statusString', '')),
                   'remote_checksum': str(obj.get('chksum', '')),
                   'remote_expiry': str(obj.get('dataExpiry', '0')),
                   'remote_create_time': int(obj.get('dataCreate', 0)),
                   'remote_modify_time': int(obj.get('dataModify', 0))}
            for k, v in obj.items():
                if k.startswith('DMF_'):
                    ret[str(k)] = str(v)
            return ret

        json_lst = json.dumps(lst)
        if self.is_resource_server:
            msiName = "msiGetDmfObject"
        else:
            msiName = "GetDmfObject"
            # need to install the rule on the iCAT server:
            # GetDmfObject(*lst, *res) {
            #  *res = ""
            #  remote("<irods5>","") {
            #   msiGetDmfObject(*lst, *res);
            #  }
            # }
        rule_code = ("getDmf {\n" +
                     " *res=\"\";\n" +
                     " " + msiName + "(*lst, *res);\n" +
                     "}\n")
        encoded_list = json_lst.replace('"', '\\"')
        params = {"*lst": '"{0}"'.format(encoded_list)}
        myrule = Rule(self.session,
                      body=rule_code,
                      params=params,
                      output="*res")
        try:
            res = myrule.execute()
        except Exception as e:
            self.logger.error(str(e))
            for line in rule_code.split('\n'):
                self.logger.error(line)
            self.logger.error('*lst: "{0}"'.format(encoded_list))
            raise
        objs = json.loads(self.get_rule_return_value(res, 0))
        return [transform(obj) for obj in objs]

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
            res = {}
            for k, v in fields.items():
                val = item[v]
                if isinstance(val, datetime.datetime):
                    val = (val - datetime.datetime(1970, 1, 1)).total_seconds()
                res[k] = val
            res['remote_file'] = os.path.join(res['collection'],
                                              res['object'])
            yield res

    def get(self, ticket):
        self.logger.info('iget %s -> %s',
                         ticket.remote_file, ticket.local_file)
        remote_file = ticket.remote_file
        obj = self.session.data_objects.get(remote_file)
        ticket.transferred = 0
        ticket.remote_size = obj.size
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
        """
        Compare the checksum of local file and object in iRODS
        Raise ValueError if checksums don't match
        """
        obj = self.session.data_objects.get(remote_file)
        if obj.checksum is not None:
            chcksum = ticket.checksum
            if obj.checksum != "sha2:{checksum}".format(checksum=chcksum):
                self.logger.error('obj.checksum  %s', obj.checksum)
                self.logger.error('file checksum %s', chcksum)
                raise ValueError('checksum test failed')
