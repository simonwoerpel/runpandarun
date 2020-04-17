import banal
import os
import sys
import hashlib
import pandas as pd
import numpy as np

from multiprocessing import Pool, cpu_count
from slugify import slugify as _slugify


CPUS = cpu_count()


def get_chunks(iterable, n=CPUS):
    """
    split up an iterable into n chunks
    """
    total = len(iterable)
    if total < n:
        return [[i] for i in iterable]
    chunk_size = int(total / n)
    chunks = []
    for i in range(n):
        if not i + 1 == n:
            chunks.append(iterable[i * chunk_size:(i + 1) * chunk_size])
        else:
            chunks.append(iterable[i * chunk_size:])
    return chunks


def parallelize(func, iterable, *args):
    """
    parallelize `func` applied to n chunks of `iterable`
    with optional `args`

    return: flattened generator of `func` returns
    """
    try:
        len(iterable)
    except TypeError:
        iterable = tuple(iterable)

    chunks = get_chunks(iterable, CPUS)

    if args:
        _args = ([a] * CPUS for a in args)
        with Pool(processes=CPUS) as P:
            res = P.starmap(func, zip(chunks, *_args))
    else:
        with Pool(processes=CPUS) as P:
            res = P.map(func, chunks)

    return (i for r in res for i in r)


def slugify(value, to_lower=True, separator='_'):
    return _slugify(value, to_lower=to_lower, separator=separator)


def time_to_json(value):
    try:
        return value.isoformat()
    except AttributeError:
        return


def get_files(directory, condition=lambda x: True):
    """
    return tuples of (filename, path) for files in given `directory`
    that match `condition` (default: all) incl. subdirectories
    """
    return [(os.path.splitext(f)[0], os.path.join(d, f)) for d, _, fnames in os.walk(directory)
            for f in fnames if condition(os.path.join(d, f))]


# https://docs.djangoproject.com/en/2.2/ref/utils/#module-django.utils.functional
class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.

    A cached property can be made out of an existing method:
    (e.g. ``url = cached_property(get_absolute_url)``).
    On Python < 3.6, the optional ``name`` argument must be provided, e.g.
    ``url = cached_property(get_absolute_url, name='url')``.
    """
    name = None

    @staticmethod
    def func(instance):
        raise TypeError(
            'Cannot use cached_property instance without calling '
            '__set_name__() on it.'
        )

    @staticmethod
    def _is_mangled(name):
        return name.startswith('__') and not name.endswith('__')

    def __init__(self, func, name=None):
        if sys.version_info >= (3, 6):
            self.real_func = func
        else:
            func_name = func.__name__
            name = name or func_name
            if not (isinstance(name, str) and name.isidentifier()):
                raise ValueError(
                    "%r can't be used as the name of a cached_property." % name,
                )
            if self._is_mangled(name):
                raise ValueError(
                    'cached_property does not work with mangled methods on '
                    'Python < 3.6 without the appropriate `name` argument. See '
                    'https://docs.djangoproject.com/en/2.2/ref/utils/'
                    '#cached-property-mangled-name',
                )
            self.name = name
            self.func = func
        self.__doc__ = getattr(func, '__doc__')

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
            self.func = self.real_func
        elif name != self.name:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                "(%r and %r)." % (self.name, name)
            )

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res


# inspired from https://github.com/alephdata/servicelayer/blob/master/servicelayer/cache.py#L46
def make_key(*criteria, hash=None, clean=False):
    """Make a string key out of many criteria."""
    if hash is not None:
        if hash is True:
            hash = 'md5'
        m = getattr(hashlib, hash)()
    parts = []
    for criterion in criteria:
        if criterion is None:
            continue
        criterion = str(criterion)
        if clean:
            criterion = slugify(criterion.strip())
        parts.append(criterion)
    key = ':'.join(parts)
    if hash is None:
        return key
    m.update(key.encode('utf-8'))
    return m.hexdigest()


def safe_eval(value):
    return eval(value, {'__builtins__': {
        'pd': pd,
        'np': np,
        'str': str,
        'int': int,
        'float': float,
        'dict': dict,
        'list': list,
        'tuple': tuple,
        'None': None,
        'True': True,
        'False': False,
        'len': len,
        'hasattr': hasattr,
        'getattr': getattr,
        'isinstance': isinstance
    }})


def ensure_singlekey_dict(data):
    # validate that data is a dict with only 1 key and return key, data[key]
    if banal.is_mapping(data):
        if len(data) == 1:
            return list(data.items())[0]
    return None, None
