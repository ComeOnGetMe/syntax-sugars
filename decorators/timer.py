import time
from functools import partial, update_wrapper


class BaseTimer(object):
    def __init__(self):
        self._start = 0
        self._started = False
        self._last_lap_end = 0

    def _check_started(self):
        assert self._start > 0, 'Need to start first!'

    def start(self):
        if self._started:
            print('Timer is already running!')
        else:
            self._start = time.time()
            self._started = True
            self._last_lap_end = self._start

    def lap(self, print_format="{time}"):
        lap_end = time.time()
        self._check_started()
        print(print_format.format(time=lap_end - self._last_lap_end))
        self._last_lap_end = lap_end

    def stop(self, print_format="{time}"):
        end = time.time()
        self._check_started()
        print(print_format.format(time=end - self._start))
        self._started = False
        self._last_lap_end = 0


class Timer(BaseTimer):
    def __init__(self, func=None):
        super(Timer, self).__init__()
        self.func = None
        if callable(func):
            self.func = func
            update_wrapper(self, func)

    # Used for decorator
    def __call__(self, *args):
        self.start()
        ret = self.func(*args)
        self.stop()
        return ret

    def __get__(self, obj, objtype):
        return partial(self.__call__, obj)

    # Used for context manager
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


if __name__ == '__main__':
    from time import sleep
    import math

    """ basic usage """
    t = BaseTimer()
    t.start()
    sleep(0.5)
    t.lap("This lap: {time}")
    sleep(0.6)
    t.stop("Total: {time}")

    """ Context usage """
    with Timer():
        sleep(math.pi * .1)

    """ Decorator """
    @Timer
    def test():
        sleep(0.12345)
        return
    test()

    class A:
        @Timer
        def test(self, x):
            sleep(x)

    A().test(math.e * .1)
