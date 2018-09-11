import datetime


class Table(object):
    filewidth = 40
    fmt = '{DMF: <4}{FILE: <' + str(filewidth) + '}{TIME: <20}{STATUS: <11}'
    time_fmt = '%Y-%m-%d %H:%M:%S'

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
        print(Table.fmt.format(STATUS=obj.get('status', ''),
                               DMF=obj.get('meta', {}). get('SURF-DMF', ''),
                               TIME=dtg,
                               FILE=self.slice_filename(obj)))

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
                               FILE="FILE"))
