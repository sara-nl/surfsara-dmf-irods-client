import os
import signal
import logging
import subprocess


def get_config_dir():
    home = os.path.expanduser("~")
    return os.path.join(home, ".dm_irods")


def get_ticket_dir():
    return os.path.join(get_config_dir(), "tickets")


def get_dm_inodes_dir():
    return os.path.join(get_config_dir(), "inodes")


def get_pid_file():
    config_dir = get_config_dir()
    return os.path.join(config_dir, "dm_idaemon.pid")


def get_socket_file():
    config_dir = get_config_dir()
    return os.path.join(config_dir, "dm_idaemon.socket")


def get_daemon_log_file():
    config_dir = get_config_dir()
    return os.path.join(config_dir, "dm_idaemon.log")


def ensure_daemon_is_running(logfile=get_daemon_log_file(),
                             logger=logging.getLogger('dm_idaemon')):
    if logfile is None:
        logfile = get_daemon_log_file()
    pid_file = get_pid_file()
    need_start = False
    try:
        file = open(pid_file, "r")
        pid = int(file.read())
        try:
            os.kill(pid, 0)
        except:
            logger.warning("pid file %s exists " +
                           "but process is not running (pid=%d)" %
                           (pid_file, pid))
            need_start = True
    except:
        logger.info("pid file %s does not exist" % pid_file)
        need_start = True
    if need_start:
        progr = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                             'dm_idaemon')
        pid = subprocess.Popen(["/usr/bin/env", "python",
                                progr, "-l", logfile]).pid
        logger.info("started daemon with pid %d" % pid)
    else:
        logger.info("daemin already running (%s %d)" % (pid_file, pid))


def ensure_daemon_is_stopped(logger=logging.getLogger('dm_idaemon')):
    pid_file = get_pid_file()
    try:
        file = open(pid_file, "r")
    except:
        logger.info("pid file %s does not exist" % pid_file)
        return
    try:
        pid = int(file.read())
    except:
        logger.error("invalid pid in file %s" % pid_file)
        raise
    if pid is not None:
        logger.info("stopping deamon (%s %d)" % (pid_file, pid))
        os.kill(pid, signal.SIGINT)


def get_daemon_status(logger=logging.getLogger('dm_idaemon')):
    pid_file = get_pid_file()
    try:
        file = open(pid_file, "r")
    except:
        return "NOT RUNNING"
    try:
        pid = int(file.read())
    except:
        logger.error("invalid pid in file %s" % pid_file)
        raise
    if pid is not None:
        try:
            os.kill(pid, 0)
            return "RUNNING"
        except:
            return "NOT RUNNING"


def ensure_config_path_exists(config_dir=get_config_dir(),
                              logger=logging.getLogger('dm_idaemon')):
    if not os.path.exists(config_dir):
        logger.info("creating directory %s", config_dir)
        os.makedirs(config_dir)
    else:
        logger.info("directory %s exists", config_dir)
