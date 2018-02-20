import configparser
import io
import os

from . import platform
from .utils import LazyPool, NoPool


class Config(object):
    def __init__(self, name=None, platform=None, conf=None):
        self.name = name
        self.platform = platform
        self.conf = conf

    def read(self, main_conf_file, args):
        self.name, _ = os.path.splitext(main_conf_file)
        self.platform = self.platform_config(args)
        self.conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.conf.read_file(open(main_conf_file, 'r'))
        if 'subconf' in self.conf:
            for (key, subconf) in self.conf['subconf'].items():
                self.conf.read_file(open(subconf, 'r'))

    def get_path(self, section, key):
        return self.conf['paths.{}'.format(section)][key]

    def platform_config(self, args):
        try:
            with open('current_platform', 'r') as fobj:
                platform_name = fobj.readline().strip()
        except FileNotFoundError:
            raise Exception(
                'Expecting to find a file named "current_platform" '
                'in the working directory. It should contain a string, '
                'which when plugged into platform_{}.ini points to a '
                'valid platform config.')
        conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        conf.read_file(open('platform_{}.ini'.format(platform_name), 'r'))

        platform_class = conf['platform']['platform']
        platf = platform.classes[platform_class](
            platform_name, conf)

        if 'multiprocessing' in conf and not args.no_fork:
            self.pool = LazyPool(
                processes=conf['multiprocessing'].get(
                    'cores', None),
                chunksize=conf['multiprocessing'].get(
                    'chunksize', 1000)
                )
        else:
            self.pool = NoPool()

        return platf

    def __getitem__(self, key):
        return self.conf[key]

# use interpolation in configparser
# '${FILE:corpus}.${resection:some}'
# can also include dollar-free, which can be formatted from command line args
# '${resection:some}.mb{minibatch}'


class GridConfig(object):
    def __init__(self, grid_conf_file, args):
        pass    # FIXME

    def get_overrides(self):
        # FIXME dummy
        return [{'grid:param1': 1, 'grid:param2': 2},
                {'grid:param1': 2, 'grid:param2': 3}]

    @staticmethod
    def apply_override(base_conf, overrides):
        conf_string = io.StringIO()
        base_conf.conf.write(conf_string)
        # We must reset the buffer ready for reading.
        conf_string.seek(0)
        new_conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        new_conf.read_file(conf_string)
        # apply overrides
        for sec_key, val in overrides.items():
            sec, key = sec_key.split(':')
            new_conf[sec][key] = str(val)
        return Config(name=base_conf.name,
                      platform=base_conf.platform,
                      conf=new_conf)
