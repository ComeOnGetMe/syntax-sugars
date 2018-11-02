from configparser import ConfigParser


class BITConfigParser(ConfigParser):
    def __init__(self, *args, **kwargs):
        super(BITConfigParser, self).__init__(*args, **kwargs)

    def gettuple(self, section, option, delimiter=',', var_type=str, **kwargs):
        temp = self.get(section, option, **kwargs)
        return map(var_type, tuple(temp.split(delimiter)))

    def update(self, another):
        for section in another.sections():
            if section not in self.sections():
                continue
            for option in another.options(section):
                value = another.get(section, option)
                self.set(section, option, value)
