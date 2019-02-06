import sys
import datetime
import re
import json
from argparse import ArgumentParser
from .server import ensure_daemon_is_running
from .server import DmIRodsServer
from .cprint import format_bold
from .cprint import format_status
from .cprint import format_error
from .cprint import print_request_error
from .socket_server.client import Client
from .socket_server.server import ReturnCode


def dm_iinfo(argv=sys.argv[1:]):
    def fmt_time(timestamp):
        time_fmt = '%Y-%m-%d %H:%M:%S'
        return datetime.datetime.fromtimestamp(timestamp).strftime(time_fmt)

    def format_progress(progress):
        return progress

    def print_value(maxlen, f, value, entry={}):
        colorizer = entry.get('colorizer', None)
        if 'fmt' in entry:
            value = entry['fmt'](value)
        if sys.version_info[0] == 2:
            if not isinstance(value, str) and not isinstance(value, unicode):
                value = str(value)
        else:
            if not isinstance(value, str):
                value = str(value)
        f = format_bold(('{0: <%d}' % maxlen).format(f))
        sep = ': '
        for line in value.split('\n'):
            if colorizer is not None:
                line = colorizer(line)
            print(f + sep + line)
            f = ('{0: <%d}' % maxlen).format('')
            sep = '  '

    def count_groups(fields, obj):
        current_group = ''
        ret = {'': 0}
        for entry in fields:
            if 'group' in entry:
                current_group = entry.get('group')
                ret[current_group] = 0
            elif 'field' in entry:
                f = entry.get('field')
                if obj.get(f, None) is not None:
                    ret[current_group] += 1
            elif 'fieldre' in entry:
                expr = re.compile(entry.get('fieldre'))
                ret[current_group] += sum([1 if expr.match(k) else 0
                                           for k in obj.keys()])
        return ret

    fields = [{'group': 'Transfer'},
              {'field': 'retries'},
              {'field': 'status', 'colorizer': format_status},
              {'field': 'progress', 'colorizer': format_progress},
              {'field': 'errmsg', 'colorizer': format_error},
              {'field': 'time_created', 'fmt': fmt_time},
              {'field': 'transferred'},
              {'field': 'mode'},
              {'group': 'Local File'},
              {'field': 'local_file'},
              {'field': 'local_atime', 'fmt': fmt_time},
              {'field': 'local_ctime', 'fmt': fmt_time},
              {'field': 'local_size'},
              {'field': 'checksum'},
              {'group': 'Remote Object'},
              {'field': 'remote_file'},
              {'field': 'remote_size'},
              {'field': 'remote_create_time', 'fmt': fmt_time},
              {'field': 'remote_modify_time', 'fmt': fmt_time},
              {'field': 'remote_checksum'},
              {'field': 'collection'},
              {'field': 'object'},
              {'field': 'remote_owner_name'},
              {'field': 'remote_owner_zone'},
              {'field': 'remote_replica_number'},
              {'field': 'remote_replica_status'},
              {'group': 'DMF Data'},
              {'fieldre': 'DMF_.*'}]
    parser = ArgumentParser(description='Get details for object.')
    parser.add_argument('file',
                        type=str,
                        help='object')
    args = parser.parse_args(argv)
    ensure_daemon_is_running()
    client = Client(DmIRodsServer.get_socket_file())
    code, result = client.request({"info": args.file})
    if code != ReturnCode.OK:
        print_request_error(code, result)
        sys.exit(8)
    obj = json.loads(result)
    if not obj:
        return
    maxlen = max([len(v) for v in obj.keys()]) + 2
    groups = count_groups(fields, obj)
    current_group = ''
    for entry in fields:
        if 'group' in entry:
            current_group = entry.get('group')
            if groups.get(current_group, 0) > 0:
                print("--------------------------")
                print(current_group)
                print("--------------------------")
        elif 'field' in entry:
            if groups.get(current_group, 0) > 0:
                f = entry.get('field')
                value = obj.get(f, None)
                if value is not None:
                    print_value(maxlen, f, value, entry)
        elif 'fieldre' in entry:
            if groups.get(current_group, 0) > 0:
                expr = re.compile(entry.get('fieldre'))
                for f, value in {k: v
                                 for k, v in obj.items()
                                 if expr.match(k)}.items():
                    print_value(maxlen, f, value, entry)


if __name__ == "__main__":
    dm_iinfo()
