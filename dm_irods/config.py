import os
import sys
import json
import logging
from argparse import ArgumentParser
import irods.password_obfuscation as password_obfuscation
from getpass import getpass
from .cprint import format_bold
from .logger import init_logger


if sys.version_info[0] == 3:
    raw_input = input


def question_boolean(question, default_value=None):
    """
    Reads the answer for a boolean question from the keyboard.
    Returns the answer of the question (True=Yes, False=No)
    """

    if default_value is True:
        ans = ' (Y/n) '
    elif default_value is False:
        ans = ' (y/N) '
    else:
        ans = ' (y/n) '
    while True:
        reply = str(raw_input(question +
                              format_bold(ans) +
                              ': ')).lower().strip()
        if default_value is not None and reply == '':
            return default_value
        elif len(reply) > 0:
            if reply[0] == 'y':
                return True
            if reply[0] == 'n':
                return False


def question(question, default_value='', required=True, return_type=str):
    """
    Ask a question and reads a string from stdin.
    """
    # todo: autocomplete
    if default_value:
        question += ' [' + str(default_value) + ']'
    question += ': '
    while True:
        ret = str(raw_input(question)).strip()
        if ret:
            if return_type == str:
                return ret
            else:
                try:
                    ret = return_type(ret)
                    return ret
                except Exception:
                    pass
        else:
            if default_value:
                return default_value


class DmIRodsConfig(object):
    """
    Configuration of the dmf-irods-client.
    Load configuration from ~/.DmIRodServer/config.json
    """

    def __init__(self, logger=logging.getLogger('dm_iclient')):
        home_dir = os.path.expanduser("~")
        self.logger = logger
        self.config = {}
        self.config_dir = os.path.join(home_dir, ".DmIRodsServer")
        self.config_file = os.path.join(self.config_dir, "config.json")
        self.completion_file = os.path.join(self.config_dir, "completion.sh")
        self.irods_auth_file = os.path.join(self.config_dir, ".irodsA")
        if os.path.isfile(self.config_file):
            self.is_configured = True
            self.logger.info('read config file %s', self.config_file)
            with open(self.config_file) as f:
                for k, v in json.load(f).items():
                    if sys.version_info[0] == 2 and isinstance(v, unicode):
                        self.config[str(k)] = str(v)
                    else:
                        self.config[str(k)] = v
        else:
            self.is_configured = False

    def ensure_configured(self, force=False, config={}):
        """
        If the file ~/.DmIRodServer/config.json exists and force is False,
        nothing is changed and True is returned.
        Otherwise the configuration file is created from config.
        Missing information is filled from answers to inter
        """
        def _get_timeout(c):
            irods_config = self.config.get('irods', {})
            default_value = c.get('connection_timeout',
                                  irods_config.get('connection_timeout',
                                                   10))
            return question('iRODS connection timeout (seconds) ',
                            default_value=default_value,
                            return_type=int)

        def _get_resource_name(c):
            irods_config = self.config.get('irods', {})
            default_value = c.get('resource_name',
                                  irods_config.get('resource_name',
                                                   'arcRescSURF01'))
            return question('iRODS resource',
                            default_value=default_value,
                            return_type=str)

        def _get_housekeeping(c):
            default_value = c.get('housekeeping',
                                  self.config.get('housekeeping', 24))
            return question('remove old jobs after n hours',
                            default_value=default_value,
                            return_type=int)

        def _get_stop_timeout(c):
            default_value = c.get('stop_timeout',
                                  self.config.get('stop_timeout', 30))
            return question('stop daemon after being n minutes idle ' +
                            '(never stop: n=0)',
                            default_value=default_value,
                            return_type=int)

        def _get_resource_server(c):
            value = c.get('irods_is_resource_server', False)
            q = 'direct connection to resource (DMF) server'
            return question_boolean(q, default_value=value)

        if self.is_configured and not force:
            return True
        else:
            default_host = config.get('irods_host', None)
            default_port = config.get('irods_port', 1247)
            default_zone = config.get('irods_zone_name', None)
            default_user = config.get('irods_user_name', None)
            cfg = {'irods_host': question('iRODS config: Host',
                                          default_value=default_host),
                   'irods_port': question('iRODS config: Port',
                                          default_value=default_port,
                                          return_type=int),
                   'irods_zone_name': question('iRODS config: Zone',
                                               default_value=default_zone),
                   'irods_user_name': question('iRODS config: User name',
                                               default_value=default_user),
                   'is_resource_server': _get_resource_server(config),
                   'connection_timeout': _get_timeout(config),
                   'resource_name': _get_resource_name(config),
                   'housekeeping': _get_housekeeping(config),
                   'stop_timeout': _get_stop_timeout(config)}
            dirname = os.path.dirname(self.config_file)
            if not os.path.exists(dirname):
                self.logger.info('mkdir %s', dirname)
                os.makedirs(dirname)
            self.logger.info('writing config to %s', self.config_file)
            self.configure_password(cfg
                                    .get('irods_user_name', None))
            for line in json.dumps(cfg, indent=4).split("\n"):
                self.logger.info(line)
            with open(self.config_file, "w") as fp:
                json.dump(cfg, fp, indent=4)
            self.configure_completion_file()
            return False

    def configure_completion_file(self):
        self.logger.info('configure completion file %s',
                         self.completion_file)
        self.logger.info("type 'source %s' to enable tab completion",
                         self.completion_file)
        with open(self.completion_file, "w") as fp:
            fp.write("complete -C dm_icomplete " +
                     "dm_iget dm_iinfo\n")

    def configure_password(self, user_name):
        pw = getpass("irods password for user {0}:".format(user_name))
        with open(self.irods_auth_file, "wb") as fp:
            fp.write(password_obfuscation.encode(pw).encode())


def dm_iconfig(argv=sys.argv[1:]):
    """
    Interactive configuration of the client.
    Attempts to extract information from ~/.irods/irods_environment.json
    if available.
    """
    parser = ArgumentParser(description='Configure iRODS_DMF_client')
    irods_env = os.path.expanduser("~/.irods/irods_environment.json")
    if os.path.isfile(irods_env):
        with open(irods_env) as fp:
            config = json.load(fp)
    else:
        config = {}
    cfg_group = parser.add_argument_group('iRODS configuration')
    cfg_group.add_argument('--irods_zone_name',
                           type=str,
                           default=config.get('irods_zone_name', None))
    cfg_group.add_argument('--irods_host',
                           type=str,
                           default=config.get('irods_host', None))
    cfg_group.add_argument('--irods_port',
                           type=int,
                           default=config.get('irods_port', None))
    cfg_group.add_argument('--irods_user_name',
                           type=str,
                           default=config.get('irods_user_name', None))
    cfg_group.add_argument('--irods_is_resource_server', action="store_true",
                           help=("Connected directly to resource server\n" +
                                 "(using microservice msiGetDmfObject to " +
                                 "retrieve DMF state,\n" +
                                 "otherwise GetDmfObject wrapper " +
                                 "rule is used)"))
    cfg_group.add_argument('--connection_timeout', type=int,
                           help='timeout (in seconds, default 10)')
    cfg_group.add_argument('--stop_timeout', type=int,
                           help=('stop daemon automatically after being idle' +
                                 '(in minutes, default 10, 0 = never stop)'))
    cfg_group.add_argument('--resource_name', type=str,
                           help='iRODS resource (default arcRescSURF01)')
    cfg_server_group = parser.add_argument_group('DM-iRODS config')
    cfg_server_group.add_argument('--housekeeping',
                                  help=('remove old jobs after this time ' +
                                        '(hours, default=24)'),
                                  type=int)

    args = parser.parse_args(argv)
    config = DmIRodsConfig(logger=init_logger())
    config.ensure_configured(force=True,
                             config={k: getattr(args, k)
                                     for k in ['irods_zone_name',
                                               'irods_host',
                                               'irods_port',
                                               'irods_user_name',
                                               'irods_is_resource_server',
                                               'housekeeping',
                                               'resource_name',
                                               'connection_timeout',
                                               'stop_timeout']
                                     if getattr(args, k) is not None})


if __name__ == "__main__":
    dm_iconfig()
