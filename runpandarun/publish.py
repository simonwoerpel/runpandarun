import os
from urllib.parse import urljoin

from .util import ensure_directory


def filesystem_publish(dataset, df, config, **kwargs):
    """publish dataset to local filesystem and return filepath"""
    config = config.update(kwargs)
    directory = os.path.abspath(os.path.join(config.public_root, dataset.name))
    public_root = ensure_directory(directory)
    format_ = config.format or dataset._storage.format
    fname = '%s.%s' % (config.name or 'data', format_)
    fp = os.path.join(public_root, fname)
    if os.path.isfile(fp) and not config.overwrite:
        raise FileExistsError(f'public file `{fp}` already exists for dataset `{dataset}`')
    dump = getattr(df, f'to_{format_}')
    if config.pd_args:
        dump(fp, **config.pd_args)
    else:
        dump(fp)
    if config.include_source:
        source_fp = os.path.join(public_root, 'source.%s' % dataset._storage.format)
        with open(source_fp, 'w') as f:
            f.write(dataset._storage.get_source())

    if config.base_url:
        return urljoin(config.base_url, '/'.join((dataset.name, fname)))
    return fp
