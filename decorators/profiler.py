import time
from collections import defaultdict
from functools import update_wrapper


def profiler(cls):
    cls.__profile_records = defaultdict(list)
    old_getattr = cls.__getattribute__

    def timed_attr(self, item):
        _item = old_getattr(self, item)
        if callable(_item):
            def wrapper(*args, **kwargs):
                t = time.time()
                ret = _item(*args, **kwargs)
                t = time.time() - t
                cls.__profile_records[item].append(t)
                return ret
            update_wrapper(wrapper, _item)
            return wrapper
        else:
            return _item
    cls.__getattribute__ = timed_attr

    def print_records(self):
        for key, val in self.__profile_records.iteritems():
            N = len(val)
            total = sum(val)
            totalsq = sum(x ** 2 for x in val)
            print "{}: # of calls {}, mean time {}, variance {}".format(key, N, total/N, totalsq/N - (total/N)**2)

    cls.print_res = print_records
    return cls


if __name__ == '__main__':
    @profiler
    class A(object):
        def __init__(self):
            self.a = 1

        def sleep(self, x):
            time.sleep(x)
            return 'a'

    a = A()
    a.sleep(1)
    a.print_res()
    b = A()
    b.sleep(2)
    b.print_res()
