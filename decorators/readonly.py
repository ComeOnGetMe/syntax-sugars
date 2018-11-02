def readonly_property_factory(name, docstring=None, delimiter='/'):
    def __getattr(self, _att_name):
        att_names = _att_name.split(delimiter)
        obj = self
        for _name in att_names:
            obj = getattr(obj, _name)
        return obj

    def getter(self):
        return __getattr(self, name)
    return property(getter, doc=docstring)


def readonly_protected(cls):
    old_init = cls.__init__

    def init(self, *args, **kwargs):
        old_init(self, *args, **kwargs)
        for item in self.__dict__:
            if item.startswith('_') and not item.startswith('__'):
                setattr(cls, item[1:], readonly_property_factory(item))

    cls.__init__ = init
    return cls


if __name__ == '__main__':
    @readonly_protected
    class A(object):
        def __init__(self):
            self._a = 1

    a = A()
    print a.a
    a.a = 1
