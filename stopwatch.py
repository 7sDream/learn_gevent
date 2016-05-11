import time


class stop_watch(object):
    def __init__(self, value=0.0, func=time.perf_counter):
        self._elapsed = value
        self._func = func
        self._start = None

    def start(self):
        if self._start is not None:
            raise RuntimeError('Already started')
        self._start = self._func()

    def stop(self):
        if self._start is None:
            raise RuntimeError('Not started')
        end = self._func()
        self._elapsed += end - self._start
        self._start = None

    def reset(self):
        self._elapsed = 0.0

    @property
    def running(self):
        return self._start is not None

    @property
    def elapsed(self):
        if self.running:
            end = self._func()
            return end - self._start + self._elapsed
        else:
            return self._elapsed

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
