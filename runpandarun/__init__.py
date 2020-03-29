import os
import sys
from types import ModuleType

from .store import Datastore


# for easy entry in scripts. works if env `CONFIG` is set properly.
# use like
#
#   from runpandarun.settings import storage
#   from runpandarun.datasets import my_dataset
if os.getenv('CONFIG'):
    _store = Datastore()
    datasets = ModuleType('datasets')
    for dataset in _store:
        setattr(datasets, dataset.name, dataset)

    sys.modules[f'{__name__}.datasets'] = datasets
    sys.modules[f'{__name__}.settings'] = _store.config
