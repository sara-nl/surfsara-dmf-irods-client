import os
import json
import logging
from cprint import format_bold


def question_boolean(question, default_value=None):
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
    def __init__(self, logger=logging.getLogger('dm_iclient')):
        self.logger = logger
        self.config = {}
        self.config_file = os.path.join(os.path.expanduser("~"),
                                        ".DmIRodsServer",
                                        "config.json")
        self.completion_file = os.path.join(os.path.expanduser("~"),
                                            ".DmIRodsServer",
                                            "completion.sh")
        if os.path.isfile(self.config_file):
            self.is_configured = True
            self.logger.info('read config file %s', self.config_file)
            with open(self.config_file) as f:
                for k, v in json.load(f).items():
                    if isinstance(v, unicode):
                        self.config[str(k)] = str(v)
                    else:
                        self.config[str(k)] = v
        else:
            self.is_configured = False

    def ensure_configured(self, force=False, env_file_based=None, config={}):
        def _get_timeout(config):
            irods_config = self.config.get('irods', {})
            default_value = config.get('connection_timeout',
                                       irods_config.get('connection_timeout',
                                                        10))
            return question('iRODS connection timeout (seconds) ',
                            default_value=default_value,
                            return_type=int)

        def _get_resource_name(config):
            irods_config = self.config.get('irods', {})
            default_value = config.get('resource_name',
                                       irods_config.get('resource_name',
                                                        'arcRescSURF01'))
            return question('iRODS resource',
                            default_value=default_value,
                            return_type=str)

        def _get_housekeeping(config):
            default_value = config.get('housekeeping',
                                       self.config.get('housekeeping', 24))
            return question('remove old jobs after n hours',
                            default_value=default_value,
                            return_type=int)

        def _get_stop_timeout(config):
            default_value = config.get('stop_timeout',
                                       self.config.get('stop_timeout', 30))
            return question('stop daemon after being n minutes idle ' +
                            '(never stop: n=0)',
                            default_value=default_value,
                            return_type=int)

        if self.is_configured and not force:
            return True
        else:
            if env_file_based is None:
                prompt = 'Using irods_environment.json based config? '
                env_file_based = question_boolean(prompt, default_value=True)
            cfg = {}
            if env_file_based:
                cfg['irods'] = self.configure_env_file(config)
            else:
                cfg['irods'] = self.configure_entries(config)
            cfg['irods']['connection_timeout'] = _get_timeout(config)
            cfg['irods']['resource_name'] = _get_resource_name(config)
            cfg['housekeeping'] = _get_housekeeping(config)
            cfg['stop_timeout'] = _get_stop_timeout(config)
            dirname = os.path.dirname(self.config_file)
            if not os.path.exists(dirname):
                self.logger.info('mkdir %s', dirname)
                os.makedirs(dirname)
            self.logger.info('writing config to %s', self.config_file)
            for line in json.dumps(cfg, indent=4).split("\n"):
                self.logger.info(line)
            with open(self.config_file, "wr") as fp:
                fp.write(json.dumps(cfg, indent=4))
            self.configure_completion_file()

    def configure_completion_file(self):
        self.logger.info('configure completion file %s',
                         self.completion_file)
        self.logger.info("type 'source %s' to enable tab completion",
                         self.completion_file)
        with open(self.completion_file, "w") as fp:
            fp.write("complete -C dm_icomplete dm_iget dm_iinfo\n")
                                            
    def configure_env_file(self, config):
        def_env_file = os.path.join(os.path.expanduser("~"),
                                    ".irods",
                                    "irods_environment.json")
        def_auth_file = os.path.join(os.path.expanduser("~"),
                                     ".irods",
                                     ".irodsA")
        env_file = config.get('irods_env_file',
                              self.config.get('irods_env_file',
                                              def_env_file))
        env_file = question('Path to iRODS env path',
                            default_value=env_file)
        auth_file = config.get('irods_authentication_file',
                               self.config.get('irods_authentication_file',
                                               def_auth_file))
        auth_file = question('Path to iRODS authentication path',
                             default_value=auth_file)
        return {'irods_env_file': env_file,
                'irods_authentication_file': auth_file}

    def configure_entries(self, config):
        def_env_file = os.path.join(os.path.expanduser("~"),
                                    ".irods",
                                    "irods_environment.json")
        def_config = {}
        if os.path.isfile(def_env_file):
            with open(def_env_file) as f:
                for k, v in json.load(f).items():
                    if isinstance(v, unicode):
                        def_config[str(k)] = str(v)
                    else:
                        def_config[str(k)] = v
        if 'irods_port' not in def_config:
            def_config['irods_port'] = 1247
        fields = ['irods_host',
                  'irods_port',
                  'irods_user_name',
                  'irods_zone_name']
        ret = {}
        for k in fields:
            if k == 'irods_port':
                return_type = int
            else:
                return_type = str
            value = question('iRODS config: %s' % k,
                             default_value=config.get(k,
                                                      def_config.get(k,
                                                                     None)),
                             return_type=return_type)
            ret[k] = value
        return ret
