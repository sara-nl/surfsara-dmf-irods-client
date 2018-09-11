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
        if self.is_configured and not force:
            return True
        else:
            if env_file_based is None:
                prompt = 'Using irods_environment.json based config? '
                env_file_based = question_boolean(prompt, default_value=True)
            if env_file_based:
                cfg = self.configure_env_file(config)
            else:
                cfg = self.configure_entries(config)
            def_timeout = config.get('connection_timeout', 10)
            prompt = 'iRODS connection timeout (seconds) '
            cfg['connection_timeout'] = question(prompt,
                                                 default_value=def_timeout,
                                                 return_type=int)
            def_res = config.get('resource_name', 'arcRescSURF01')
            cfg['resource_name'] = question('iRODS resource',
                                            default_value=def_res)
            dirname = os.path.dirname(self.config_file)
            if not os.path.exists(dirname):
                self.logger.info('mkdir %s', dirname)
                os.makedirs(dirname)
            self.logger.info('writing config to %s', self.config_file)
            for line in json.dumps(cfg, 4).split("\n"):
                self.logger.info(line)
            with open(self.config_file, "wr") as fp:
                fp.write(json.dumps(cfg, 4))

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
        if 'irods_port' not in config:
            config['irods_port'] = 1247
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
                             default_value=config.get(k, None),
                             return_type=return_type)
            ret[k] = value
        return ret
