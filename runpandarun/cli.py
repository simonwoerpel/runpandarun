import os
import argparse
import logging
import sys

from .store import Datastore
from .exceptions import ConfigError


log = logging.getLogger(__name__)


def update(args):
    for dataset in args.store:
        should_update = not len(args.datasets) or (dataset.name in args.datasets)
        if should_update:
            log.info(f'Updating `{dataset}` from `{dataset.url}` ...')
            try:
                dataset.update()
                log.info(f'Updated `{dataset}`.')
            except Exception as e:
                log.error(f'{e.__class__.__name__}: {e}')


def print_(args):
    df = args.store[args.dataset].get_df()
    df.to_csv(sys.stdout)


def publish(args):
    for dataset in args.store:
        should_publish = not args.datasets or (dataset.name in args.datasets)
        if should_publish:
            log.info(f'Publishing `{dataset}` ...')
            try:
                for res in dataset.publish():
                    log.info(f'Published `{dataset}` to `{res}`.')
            except Exception as e:
                log.error(f'{e.__class__.__name__}: {e}')


ENTRIES = {
    'update': update,
    'print': print_,
    'publish': publish
}


COMMANDS = {
    'update': {
        'datasets': {
            'help': '1 or more datasets to update, leave empty for all',
            'nargs': '*',
            'default': None
        },
        '--config': {
            'help': 'Path to yaml config for datastore spec. If empty, we will try to read from env vars'
        }
    },
    'print': {
        'dataset': {
            'help': 'name of dataset to print to stdout',
        },
        '--config': {
            'help': 'Path to yaml config for datastore spec. If empty, we will try to read from env vars'
        }
    },
    'publish': {
        'datasets': {
            'help': '1 or more datasets to publish, leave empty for all',
            'nargs': '*',
            'default': None
        },
        '--config': {
            'help': 'Path to yaml config for datastore spec. If empty, we will try to read from env vars'
        }
    }
}


def main():
    parser = argparse.ArgumentParser(prog='runpandarun')
    parser.add_argument('--loglevel', default='INFO')
    subparsers = parser.add_subparsers(help='commands help: run `runpandarun <command> -h`')
    for name, opts in COMMANDS.items():
        subparser = subparsers.add_parser(name)
        subparser.set_defaults(func=name)
        for flag, args in opts.items():
            subparser.add_argument(flag, **args)

    args = parser.parse_args()

    if getattr(args, 'config', None) is None:
        args.config = os.getenv('CONFIG')
        if args.config is None:
            raise ConfigError('Please specify config path via `--config` or env var `CONFIG`')

    logging.basicConfig(stream=sys.stderr, level=getattr(logging, args.loglevel))

    if args.func in ENTRIES:
        args.store = Datastore(args.config)
        ENTRIES[args.func](args)
    else:
        raise Exception('`%s` is not a valid command.' % args.func)
