from .exceptions import ConfigError
from .util import ensure_singlekey_dict, safe_eval


def paginate_offset(get_request, offset_param, get_offset_value, offset=0):
    res = get_request(**{offset_param: offset})
    new_offset = offset + get_offset_value(res)
    if new_offset > offset:
        yield res
        yield from paginate_offset(get_request, offset_param, get_offset_value, new_offset)


def paginate(get_request, config):
    method, config = ensure_singlekey_dict(config)
    if method is None:
        raise ConfigError(f'Please make sure {config} is properly configured as single-key dict!')
    if method != 'offset':
        raise ConfigError(f'Other pagination method than `{method}` currently not registered')
    yield from paginate_offset(get_request, config['param'], safe_eval(config['get_offset_value']))
