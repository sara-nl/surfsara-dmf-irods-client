try:
    from termcolor import colored
    with_color = True
except Exception:
    with_color = False


def format_error(st):
    maxlen = max([len(line) for line in st.split('\n')])
    out = "+--" + "-" * maxlen + "--+\n"
    for line in st.split('\n'):
        out += "|  " + line + ' ' * (maxlen - len(line)) + "  |\n"
    out += "+--" + "-" * maxlen + "--+\n"
    if with_color:
        return colored(out, color='red', attrs=['bold'])
    else:
        return out


def print_error(st):
    print(format_error(st))


def format_bold(st):
    if with_color:
        return colored(st, attrs=['bold'])
    else:
        return st
