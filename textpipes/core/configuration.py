import configparser
import os

from . import platform
from .utils import LazyPool, NoPool


class Config(object):
    def __init__(self, main_conf_file, args):
        self.name, _ = os.path.splitext(main_conf_file)
        self.platform = self.platform_config(args)
        self.conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.conf.read_file(open(main_conf_file, 'r'))
        if 'subconf' in self.conf:
            for (key, subconf) in self.conf['subconf'].items():
                self.conf.read_file(open(subconf, 'r'))

    def get_path(self, section, key):
        try:
            return self.conf['paths.{}'.format(section)][key]
        except KeyError:
            raise Exception('Undefined path {}:{}'.format(section, key))

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
