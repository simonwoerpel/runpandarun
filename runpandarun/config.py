import os
import re
import yaml


def parse_config(data, tag='!ENV'):
    """
    https://medium.com/swlh/python-yaml-configuration-with-environment-variables-parsing-77930f4273ac

    Load a yaml configuration file and resolve any environment variables
    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}.
    E.g.:

    database:
        host: !ENV ${HOST}
        port: !ENV ${PORT}
    app:
        log_path: !ENV '/var/${LOG_PATH}'
        something_else: !ENV '${AWESOME_ENV_VAR}/var/${A_SECOND_AWESOME_VAR}'

    :param str data: the yaml data itself as a stream
    :param str tag: the tag to look for
    :return: the dict configuration
    :rtype: dict[str, T]
    """
    # pattern for global vars: look for ${word}
    pattern = re.compile(r'.*?\${(\w+)}.*?')
    loader = yaml.SafeLoader

    # the tag will be used to mark where to start searching for the pattern
    # e.g. somekey: !ENV somestring${MYENVVAR}blah blah blah
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader, node):
        """
        Extracts the environment variable from the node's value
        :param yaml.Loader loader: the yaml loader
        :param node: the current node in the yaml
        :return: the parsed string that contains the value of the environment
        variable
        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    f'${{{g}}}', os.environ.get(g, '')
                )
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    if os.path.isfile(data):
        with open(data) as f:
            data = f.read()

    return yaml.load(data, Loader=loader)


class Config:
    """simple wrapper to handle config dicts conveniently"""
    def __init__(self, config):
        if isinstance(config, str):
            config = parse_config(config)
        self._config = config

    def __getattr__(self, attr):
        return self._config.get(attr)

    def __getitem__(self, item):
        return self._config.get(item)

    def __get__(self, attr, default=None):
        return self._config.get(attr, default or {})

    def __contains__(self, item):
        return item in self._config

    def __iteritems__(self):
        return self._config.items()

    def get(self, attr, default=None):
        return self.__get__(attr, default)

    def to_dict(self):
        return self._config

    def update(self, data):
        return self.__class__({**self._config, **data})
