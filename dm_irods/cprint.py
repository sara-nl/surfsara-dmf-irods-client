import json
import sys
from .socket_server.server import ReturnCode
try:
    from termcolor import colored
    with_color = True
except Exception:
    with_color = False


def format_box(st):
    maxlen = max([len(line) for line in st.split('\n')])
    out = "+--" + "-" * maxlen + "--+\n"
    for line in st.split('\n'):
        out += "|  " + line + ' ' * (maxlen - len(line)) + "  |\n"
    out += "+--" + "-" * maxlen + "--+\n"
    return out


def format_error(st, box=False):
    if box:
        st = format_box(st)
    if with_color:
        return colored(st, color='red', attrs=['bold'])
    else:
        return st


def format_warning(st, box=False):
    if box:
        st = format_box(st)
    if with_color:
        return colored(st, color='red', attrs=['dark'])
    else:
        return st


def format_processing(st, box=False):
    if box:
        st = format_box(st)
    if with_color:
        return colored(st, color='blue')
    else:
        return st


def format_done(st, box=False):
    if box:
        st = format_box(st)
    if with_color:
        return colored(st, color='green', attrs=['bold'])
    else:
        return st


def format_bold(st):
    if with_color:
        return colored(st, attrs=['bold'])
    else:
        return st


def format_status(status, txt=None):
    status_formatter = {"WAITING": format_bold,
                        "CANCELED": format_warning,
                        "GETTING": format_processing,
                        "PUTTING": format_processing,
                        "DONE": format_done,
                        "UNDEF": format_error,
                        "UNMIG": format_error,
                        "ERROR": format_error,
                        "RETRY": format_warning}
    if txt is None:
        txt = status
    if status in status_formatter:
        return status_formatter[status](txt)
    else:
        return txt


def print_error(st, box=False):
    print(format_error(st, box=box))


def terminal_erase():
    if sys.version_info[0] == 2:
        print('\u001Bc'.decode('unicode_escape'))
    else:
        print(b'\u001Bc'.decode('unicode_escape'))


def terminal_home():
    if sys.version_info[0] == 2:
        print('\u001B[H'.decode('unicode_escape'))
    else:
        print(b'\u001B[H'.decode('unicode_escape'))


def print_request_error(code, result):
    if code != ReturnCode.OK:
        try:
            print('Return Code %d (%s)' % (code, ReturnCode.to_string(code)))
            obj = json.loads(result)
            print('Exception %s raised' % obj.get('exception', '?'))
            print('Message: %s' % obj.get('msg', '?'))
            print('Traceback: %s' % obj.get('traceback', '?'))
            print_error(obj.get('msg', '?'), box=True)
        except Exception:
            print_error(result, box=True)
