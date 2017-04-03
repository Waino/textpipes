import configparser

class Config(object):
    def __init__(self, main_conf_file):
        self.platform = None
        self.conf = self.platform_config()
        self.conf.read_file(open(main_conf_file, 'r'))
        if 'subconf' in self.conf:
            for (key, subconf) in self.conf['subconf'].items():
                self.conf.read_file(open(subconf, 'r'))

    def get_path(self, section, key):
        return self.conf['paths.{}'.format(section)][key]

    def platform_config(self):
        try:
            with open('current_platform', 'r') as fobj:
                self.platform = fobj.readline().strip()
        except FileNotFoundError:
            raise Exception(
                'Expecting to find a file named "current_platform" '
                'in the working directory. It should contain a string, '
                'which when plugged into platform_{}.ini points to a '
                'valid platform config.')
        conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        conf.read_file(open('platform_{}.ini'.format(self.platform), 'r'))
        return conf
        

# clunky: can use dict notation
# '{FILE[corpus]}.{resection[some]}.{resection[wtf]}.{resection[lulz]}.gz'.format(**conf)
# alternative: use interpolation in configparser
# '${FILE:corpus}.${resection:some}'
# can also include dollar-free, which can be formatted from command line args
# '${resection:some}.mb{minibatch}'
