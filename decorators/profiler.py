import time
from collections import defaultdict
from functools import update_wrapper


class Profiler(object):
    def __init__(self):
        self.records = defaultdict(list)
        self.obj = None

    def __call__(self, obj):
        self.obj = obj
        update_wrapper(self, obj)

    def __getattr__(self, item):
        def wrapped(*args, **kwargs):
            t = time.time()
            ret = getattr(self.obj, item)(*args, **kwargs)
            t = time.time() - t
            self.records[item].append(t)
            return ret
        return wrapped

    def print_res(self):
        print self.records


def profile(cls):
    records = defaultdict(list)
    # old_getattr = getattr(cls, '__getattr__', None)

    def __getattr(self, item):
        _item = getattr(self, item)

        def wrapped(*args, **kwargs):
            t = time.time()
            ret = _item(*args, **kwargs)
            t = time.time() - t
            records[item].append(t)
            return ret
        return wrapped()
    cls.__getattr__ = __getattr

    def print_res(*args, **kwargs):
        print records

    cls.print_res = print_res
    return cls


if __name__ == '__main__':
    @profile
    class A(object):
        def __init__(self):
            self.a = 1

        def sleep(self):
            time.sleep(1)
            return 'a'

    a = A()
    a.sleep()
    a.print_res()
