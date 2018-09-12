import datetime
from cprint import format_error
from cprint import format_processing
from cprint import format_warning
from cprint import format_done
from cprint import format_bold


class Table(object):
    filewidth = 40
    fmt = '{DMF: <4}{FILE: <' + str(filewidth) + \
          '}{TIME: <20}{STATUS: <11}{MOD: <4}'
    time_fmt = '%Y-%m-%d %H:%M:%S'
    status_formatter = {"WAITING": format_bold,
                        "CANCELED": format_warning,
                        "GETTING": format_processing,
                        "PUTTING": format_processing,
                        "DONE": format_done,
                        "UNDEF": format_error,
                        "ERROR": format_error,
                        "RETRY": format_warning}

    def __init__(self):
        self.header_written = False

    def print_row(self, obj):
        if not self.header_written:
            self.print_header()
        if 'time_created' in obj:
            tim = float(obj.get('time_created'))
            dtg = datetime.datetime.fromtimestamp(tim).strftime(Table.time_fmt)
        else:
            dtg = ''
        status = obj.get('status', '')
        if status in Table.status_formatter:
            status = Table.status_formatter[status](status)
        print(Table.fmt.format(STATUS=status,
                               DMF=obj.get('meta', {}). get('SURF-DMF', ''),
                               TIME=dtg,
                               FILE=self.slice_filename(obj),
                               MOD=obj.get('mode', '')))

    def slice_filename(self, obj):
        filename = obj.get('collection', '') + '/' + obj.get('object')
        if len(filename) >= Table.filewidth:
            n = Table.filewidth - 4
            filename = '...' + filename[-n:]
        return filename

    def print_header(self):
        self.header_written = True
        print(Table.fmt.format(STATUS='STATUS',
                               DMF='DMF',
                               TIME="TIME",
                               FILE="FILE",
                               MOD="MOD"))
