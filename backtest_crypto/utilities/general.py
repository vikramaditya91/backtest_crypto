import multiprocessing
import logging

import dill

logger = logging.getLogger(__name__)

class Singleton (type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MultiProcessPool:
    def __init__(self):
        multiprocessing.set_start_method('spawn')
        self.pool = multiprocessing.Pool(multiprocessing.cpu_count())
        self.tasks = []

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.pool.close()

    @staticmethod
    def run_dill_encoded(payload):
        func, args = dill.loads(payload)
        return func(*args)

    def apply_async_with_dill(self,
                              func,
                              args):
        payload = dill.dumps((func, args))
        return self.pool.apply_async(self.run_dill_encoded, (payload,))


class InsufficientHistory(ValueError):
    pass

