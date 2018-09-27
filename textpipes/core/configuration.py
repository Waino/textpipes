import configparser
import io
import itertools
import os

from . import platform
from .utils import LazyPool, NoPool


class Config(object):
    def __init__(self, name=None, platform=None, conf=None):
        self.name = name
        self.platform = platform
        self.conf = conf
        self.current_autolog_path = None

    def read(self, main_conf_file, args):
        self.name, _ = os.path.splitext(main_conf_file)
        self.platform = self.platform_config(args)
        self.conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.conf.read_file(open(main_conf_file, 'r'))
        if 'subconf' in self.conf:
            for (key, subconf) in self.conf['subconf'].items():
                lines = open(subconf, 'r')
                if 'subconf.template' in self.conf:
                    if key in self.conf['subconf.template']:
                        for pair in self.conf['subconf.template'][key].split(';'):
                            pattern, repl = pair.split('=')
                            lines = [line.replace(pattern, repl) for line in lines]
                self.conf.read_file(lines)
        self.force = args.force

    def get_path(self, section, key):
        try:
            return self.conf['paths.{}'.format(section)][key]
        except KeyError:
            # --check expects KeyError
            raise KeyError('Undefined path {}:{}'.format(section, key))

    def platform_config(self, args):
        if args.platform is not None:
            platform_name = args.platform
        else:
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

    def autolog_for_jobid(self, job_id, sec_key):
        self.current_autolog_path = self.platform.autolog_for_jobid(
            job_id, self, sec_key)
        return self.current_autolog_path

    def __getitem__(self, key):
        return self.conf[key]

# use interpolation in configparser
# '${FILE:corpus}.${resection:some}'
# can also include dollar-free, which can be formatted from command line args
# '${resection:some}.mb{minibatch}'


class GridConfig(object):
    def __init__(self, grid_conf_file, args):
        self.conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.conf.read_file(open(grid_conf_file, 'r'))

    def get_overrides(self, main_conf):
        params = self.conf['grid']['optimize'].split()
        if 'override' in self.conf['grid']:
            center_overrides = self.conf['grid']['override'].split()
            params += [x for x in center_overrides if x not in params]
        else:
            center_overrides = []
        sec_keys = [self.conf['grid.keys'][param]
                    for param in params]
        ranges = [self._get_range(param,
                                  main_conf,
                                  override_center=param in center_overrides)
                  for param in params]
        result = []
        for point in itertools.product(*ranges):
            result.append({key: val
                           for (key, val)
                           in zip(sec_keys, point)})
        return result

    def _get_range(self, param, main_conf, override_center=False):
        (sec, key) = self.conf['grid.keys'][param].split(':')
        if override_center:
            center_value = self.conf['grid.overrides'][param]
        else:
            center_value = main_conf.conf[sec][key]
        whole_range = self.conf['grid.values'][param].split()
        radius = self.conf['grid.radius'].getint(param)
        # FIXME: raise when not found
        center = whole_range.index(center_value)
        low = max(0, center - radius)
        high = min(len(whole_range) - 1, center + radius)
        return whole_range[low:(high + 1)]

    @staticmethod
    def apply_override(base_conf, overrides):
        # this trick is necessary, because ConfigParser
        # doesn't support deepcopy
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
