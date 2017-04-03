import configparser

from . import platform

class Config(object):
    def __init__(self, main_conf_file):
        self.platform = self.platform_config()
        self.conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.conf.read_file(open(main_conf_file, 'r'))
        if 'subconf' in self.conf:
            for (key, subconf) in self.conf['subconf'].items():
                self.conf.read_file(open(subconf, 'r'))

    def get_path(self, section, key):
        return self.conf['paths.{}'.format(section)][key]

    def platform_config(self):
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
        return platf
        

# use interpolation in configparser
# '${FILE:corpus}.${resection:some}'
# can also include dollar-free, which can be formatted from command line args
# '${resection:some}.mb{minibatch}'
