import os
import argparse
import logging
import sys

from .store import Datastore
from .exceptions import ConfigError


def update(args):
    if args.datasets:
        for dataset in args.store:
            if dataset.name in args.datasets:
                dataset.update()
    else:
        args.store.update()


def print_(args):
    df = args.store[args.dataset].df
    df.to_csv(sys.stdout)


def publish(args):
    for dataset in args.store:
        should_publish = not args.datasets or (dataset.name in args.datasets)
        should_publish and dataset.publish()


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
