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


def print_error(st, box=False):
    print(format_error(st, box=box))
