import datetime
import sys
import os
import fcntl
import termios
import struct
from cprint import format_bold
from cprint import format_status


def get_term_size():
    env = os.environ

    def ioctl_GWINSZ(fd):
        try:
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
                                                 '1234'))
        except Exception:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except Exception:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))
    return int(cr[1]), int(cr[0])


def split_format_item(item):
    item = item.split(':')
    if len(item) == 1:
        return {'header': item[0].upper()}
    else:
        return {'header': item[0].upper(), 'width': int(item[1])}


class Table(object):
    def __init__(self, format='dmf,time,status,mod,file,local_file'):
        self.fields = [split_format_item(item)
                       for item in format.split(',')]
        self.term_size = get_term_size()
        self.header_written = False
        field_config = {
            'DMF': {'field': 'DMF_state',
                    'width': 4},
            'TIME': {'field': 'time',
                     'width':  20,
                     'formatter': self.format_time},
            'STATUS': {'field': 'status',
                       'width':  15,
                       'formatter': self.format_status},
            'MOD': {'field': 'mode',
                    'width':  4},
            'FILE': {'field': 'file',
                     'min_width': 20,
                     'formatter': self.format_file},
            'LOCAL_FILE': {'field': 'local_file',
                           'min_width': 20,
                           'formatter': self.format_local_file}
        }
        for item in self.fields:
            overlay = field_config.get(item['header'], {})
            for k, v in overlay.items():
                if k not in item:
                    item[k] = v
        total_width = sum([f.get('width', 0)
                           for f in self.fields])
        missing_columns = sum([0 if 'width' in f else 1
                               for f in self.fields])
        rest_width = self.term_size[0] - total_width
        if missing_columns > 0:
            col_width = rest_width / missing_columns
        else:
            col_width = 0
        for f in self.fields:
            if 'width' not in f:
                f['width'] = max(f.get('min_width', 2), col_width)
            f['fmt'] = ('{0: <%d}' % f.get('width'))

    def format_file(self, field, obj):
        filename = obj.get('collection', '') + '/' + obj.get('object')
        width = field.get('width')
        if len(filename) >= width:
            n = width - 4
            filename = '...' + filename[-n:]
        return field.get('fmt').format(filename)

    def format_local_file(self, field, obj):
        filename = obj.get('local_file', '')
        width = field.get('width')
        if len(filename) >= width:
            n = width - 4
            filename = '...' + filename[-n:]
        return field.get('fmt').format(filename)

    def format_time(self, field, obj):
        time_fmt = '%Y-%m-%d %H:%M:%S'
        if 'time_created' in obj:
            tim = float(obj.get('time_created'))
            dtg = datetime.datetime.fromtimestamp(tim).strftime(time_fmt)
        else:
            dtg = ''
        return field.get('fmt').format(dtg)

    def format_status(self, field, obj):
        def format_percentage(txt, current, total):
            if total > 0:
                ret = " % 2.0f%% " % (100 * current / total)
            else:
                ret = " 100% "
            return txt[:-len(ret)] + ret

        status = obj.get('status', '')
        mode = obj.get('mode')
        txt = field.get('fmt').format(status)
        if mode == 'PUT' and 'transferred' in obj:
            if obj.get('local_size', None) is not None:
                txt = format_percentage(txt,
                                        int(obj.get('transferred', 0)),
                                        int(obj.get('local_size')))
        elif mode == 'GET' and 'transferred' in obj:
            if obj.get('remote_size', None) is not None:
                txt = format_percentage(txt,
                                        int(obj.get('transferred', 0)),
                                        int(obj.get('remote_size')))
        return format_status(status, txt)

    def print_row(self, obj):
        if not self.header_written:
            self.print_header()
        for f in self.fields:
            fname = f.get('field')
            if 'formatter' in f:
                value = f.get('formatter')(f, obj)
            else:
                value = f.get('fmt').format(obj.get(fname, ''))
            sys.stdout.write(value)
        sys.stdout.write("\n")
        sys.stdout.flush()

    def slice_filename(self, obj):
        filename = obj.get('collection', '') + '/' + obj.get('object')
        if len(filename) >= self.file_width:
            n = self.file_width - 4
            filename = '...' + filename[-n:]
        return filename

    def print_header(self):
        self.header_written = True
        hl = ''
        for f in self.fields:
            hl += format_bold(f.get('fmt').format(f.get('header',
                                                        f.get('field'))))
        print(hl)
