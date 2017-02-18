import configparser

class Config(object):
    def __init__(self, main_conf_file):
        self.conf = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.conf.read_file(open(main_conf_file, 'r'))
        # FIXME: for all keys in subconf section, read and union

    def get_path(self, section, key):
        return self.conf['paths.{}'.format(section)][key]

# clunky: can use dict notation
# '{FILE[corpus]}.{resection[some]}.{resection[wtf]}.{resection[lulz]}.gz'.format(**conf)
# alternative: use interpolation in configparser
# '${FILE:corpus}.${resection:some}'
# can also include dollar-free, which can be formatted from command line args
# '${resection:some}.mb{minibatch}'
