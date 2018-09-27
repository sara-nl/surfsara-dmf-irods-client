#!/usr/bin/env python
import logging
import os
import sys
import atexit
import signal
import time
import imp
import inspect
import subprocess
import json
from argparse import ArgumentParser


class ProgramStatus(object):
    def __init__(self, status, state_change=None):
        self.status = status
        self.last_state_change = state_change

    def __str__(self):
        if self.last_state_change:
            gmt = time.gmtime(self.last_state_change)
            return "%s %s" % (self.status,
                              time.strftime("%Y-%m-%d %H:%M:%S", gmt))
        else:
            return self.status


class ServerApp(object):
    def __init__(self,
                 klass,
                 pid_file=None,
                 socket_file=None,
                 log_file=None,
                 work_dir=None,
                 logger=None,
                 verbose=True,
                 custom_args=None,
                 **kwargs):
        self.klass = klass
        self.system_name = klass.get_system_name()
        if work_dir is None:
            self.work_dir = self._get_default_workdir()
        else:
            self.work_dir = work_dir
        self.pid_file = self._get_default_file_name(pid_file, "pid")
        self.socket_file = self._get_default_file_name(socket_file, "socket")
        self.log_file = self._get_default_file_name(log_file, "log")
        self._logger = logger
        self.kwargs = kwargs
        self.python_prefix = ["/usr/bin/env", "python"]
        self.verbose = verbose
        if custom_args is None:
            self.custom_args = klass.get_custom_arguments()
        else:
            self.custom_args = custom_args

    @property
    def logger(self):
        if self._logger is None:
            self._logger = self._init_logger()
        return self._logger

    def _init_logger(self, name=None, logfile=None):
        if name is None:
            name = self.system_name
        logger = logging.getLogger(name)
        log_fmt = '%(asctime)s %(levelname)-8s ' + \
                  '%(pathname)s:%(lineno)d %(message)s'
        # log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_fmt)
        if logfile is None:
            ch = logging.StreamHandler()
        else:
            dirname = os.path.dirname(logfile)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            ch = logging.FileHandler(logfile)
        ch.setFormatter(formatter)
        if self.verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)
        logger.addHandler(ch)
        return logger

    def _get_default_workdir(self):
        return os.path.join(os.path.expanduser("~"),
                            "." + self.system_name)

    def _get_default_file_name(self, original, suffix):
        if original is None:
            return os.path.join(self.work_dir,
                                self.system_name + "." + suffix)
        else:
            return original

    def parse_args(self, argv=sys.argv[1:], descr='Daemon'):
        parser = ArgumentParser(description=descr)
        parser.add_argument("-l", "--log", type=str,
                            help="log to file")
        parser.add_argument("-s", "--socket", type=str,
                            help="socket file")
        parser.add_argument("-p", "--pid", type=str,
                            help="pid file")
        parser.add_argument("-w", "--workdir", type=str,
                            help=("working directory (default %s)" %
                                  self._get_default_workdir()))
        parser.add_argument("operation", type=str, nargs='?',
                            help="start|stop|status|restart|log (optional)")
        self.custom_args.before_argument_parsing(self, parser)
        args = parser.parse_args(argv)
        self.custom_args.after_argument_parsing(self, parser, args)
        if args.log is not None:
            self.log_file = args.log
        if args.socket is not None:
            self.socket_file = args.socket
        if args.pid is not None:
            self.pid_file = args.pid
        return args

    def main(self, argv=sys.argv[1:], descr=None):
        if descr is None:
            descr = self.system_name
        args = self.parse_args(argv, descr)
        if args.operation == "start":
            self.start()
        elif args.operation == "stop":
            self.stop()
        elif args.operation == "restart":
            self.restart()
        elif args.operation == "status":
            print(self.status())
        elif args.operation == "log":
            self.log()
        else:
            if self._logger is None:
                self._logger = self._init_logger(logfile=args.log)
            elif isinstance(self._logger, str):
                self._logger = self._init_logger(name=self._logger,
                                                 logfile=args.log)
            self.run(args)

    def start(self):
        need_start = False
        try:
            with open(self.pid_file, "r") as f:
                pid = json.load(f).get('pid')
            try:
                os.kill(pid, 0)
            except Exception:
                self.logger.warning("pid file %s exists " +
                                    "but process is not running (pid=%d)" %
                                    (self.pid_file, pid))
                need_start = True
        except Exception:
            self.logger.info("pid file %s does not exist" % self.pid_file)
            need_start = True
        if need_start:
            module = inspect.getmodule(self.klass)
            filename, ext = os.path.splitext(module.__file__)
            cmd = self.python_prefix + [__file__,
                                        os.path.dirname(module.__file__),
                                        os.path.basename(filename),
                                        self.klass.__name__,
                                        '--socket', self.socket_file,
                                        '--pid', self.pid_file,
                                        '--log', self.log_file]
            cmd += self.custom_args.get_cli_arguments()
            self.custom_args.before_start(self)
            self.logger.info('start %s', ' '.join(cmd))
            p = subprocess.Popen(cmd)
            self.wait_for_process(p)
            self.custom_args.after_start(self)
            self.logger.info("started daemon with pid %d" % p.pid)
        else:
            self.logger.info("daemon already running (%s %d)" %
                             (self.pid_file, pid))

    def wait_for_process(self, p, max_trials=10):
        n = 0
        while n < max_trials:
            if p.poll() is not None:
                raise RuntimeError('failed to start process')
            n += 1
            try:
                msg = "check if process %d is ready (trial %d / %d)"
                self.logger.info(msg, p.pid, n, max_trials)
                with open(self.pid_file, "r") as f:
                    _pid = json.load(f).get('pid')
                    if _pid == p.pid:
                        return True
                    else:
                        msg = "pid in file %s (%d) does not match pid %p"
                        raise RuntimeError(msg % (self.pid_file, _pid, p.pid))
            except Exception as e:
                if n >= max_trials:
                    self.logger.error(str(e))
                    raise
                else:
                    self.logger.info(str(e))
            time.sleep(10)

    def stop(self, max_trials=10):
        try:
            file = open(self.pid_file, "r")
        except Exception:
            self.logger.info("pid file %s does not exist" % self.pid_file)
            return
        try:
            pid = json.load(file).get('pid')
        except Exception:
            self.logger.error("invalid pid in file %s" % self.pid_file)
            raise
        if pid is not None:
            self.logger.info("stopping deamon (%s %d)" % (self.pid_file,
                                                          pid))
            os.kill(pid, signal.SIGINT)
            n = 0
            while n < max_trials:
                n += 1
                if not os.path.isfile(self.pid_file):
                    return True
                time.sleep(10)
            raise RuntimeError('failed to stop process %d' % pid)

    def restart(self):
        self.stop()
        self.start()

    def status(self):
        try:
            file = open(self.pid_file, "r")
        except Exception:
            return ProgramStatus("NOT RUNNING")
        try:
            pid = json.load(file).get('pid')
        except Exception:
            self.logger.error("invalid pid in file %s" % self.pid_file)
            raise
        if pid is not None:
            try:
                os.kill(pid, 0)
                changed = os.path.getmtime(self.pid_file)
                return ProgramStatus("RUNNING", state_change=changed)
            except Exception:
                return ProgramStatus("NOT RUNNING")

    def log(self):
        print self.log_file
        cmd = ['/usr/bin/tail', '-f', '-n', '10', self.log_file]
        print ' '.join(cmd)
        p = subprocess.Popen(cmd,
                             bufsize=1,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        while True:
            output = p.stdout.readline()
            if output == '' and p.poll() is not None:
                break
            print(output.strip())
        code = p.poll()
        return code

    def run(self, args):
        self.create_pid_file()
        atexit.register(ServerApp.rm_file,
                        fname=self.pid_file,
                        logger=self.logger)
        atexit.register(ServerApp.rm_file,
                        fname=self.socket_file,
                        logger=self.logger)
        daemon = self.klass(socket_file=self.socket_file,
                            logger=self.logger,
                            args=args,
                            **self.kwargs)
        signal.signal(signal.SIGINT, daemon.stop)
        daemon.start_listener()
        daemon.run()

    def create_pid_file(self):
        dirname = os.path.dirname(self.pid_file)
        if not os.path.exists(dirname):
            self.logger.info("creating directory %s", dirname)
            os.makedirs(dirname)
        obj = {'pid': os.getpid(),
               'socket_file': self.socket_file,
               'log_file': self.log_file}
        self.logger.info("creating pid file %s pid=%d",
                         self.pid_file, os.getpid())
        with open(self.pid_file, "w") as fp:
            json.dump(obj, fp)

    @staticmethod
    def rm_file(fname, logger):
        try:
            logger.info('remove %s' % fname)
            os.remove(fname)
        except Exception as ex:
            logger.error("could not remove file %s (%s)" %
                         (fname, str(ex)))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit('server_app.py requires at least 4 arguments')
    module_path = sys.argv[1]
    module_name = sys.argv[2]
    klass_name = sys.argv[3]
    fp, pathname, description = imp.find_module(module_name, [module_path])
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    sys.path.insert(0, module_path)
    mod = imp.load_module(module_name, fp, pathname, description)
    klass = getattr(mod, klass_name)
    app = ServerApp(klass)
    app.main(sys.argv[4:])
