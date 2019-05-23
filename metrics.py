import os
import rocksdb
import redis
import subprocess
import shortuuid
import time
import numpy
from random import shuffle


def get_random_keys(n):
    print("Generating keys ...")
    x = [shortuuid.uuid() for i in range(n)]
    print("End generation")
    return x


class MetricBase(object):
    """
        Base classs

    """
    def __init__(self):
        # One million
        N = 1000000
        self.keys = range(N)
        self.times_writes = []
        self.times_reads = []

    def run(self, tries=10):
        self.times_writes = []
        self.times_reads = []
        print("Num tries:", tries)
        for i in range(tries):
            self.reset(i)
            self.times_writes.append(self.write())
            self.times_reads.append(self.read())
        print(numpy.average(self.times_writes))
        print(numpy.average(self.times_reads))


class RedisDB(MetricBase):
    def __init__(self):
        super().__init__()
        self.reset()

    def reset(self):
        self.db = redis.Redis(host='localhost', port=6379, db=0)
        self.db.flushall()

    def write(self):
        start = time.time()
        for key in self.keys:
            self.db.set(key, key)
        end = time.time()
        return end - start

    def read(self):
        start = time.time()
        for key in self.keys:
            self.db.get(key)
        end = time.time()
        return end - start


class RocksDB(MetricBase):
    def __init__(self):
        subprocess.check_output(['rm','-rf', 'puts-*.db'])
        super().__init__()

    def reset(self, iter):
        print('iter', iter)
        self.db = rocksdb.DB(
            "puts-%d.db" % iter, rocksdb.Options(create_if_missing=True))

    def write(self):
        start = time.time()
        for key in self.keys:
            b_key = str(key).encode('utf-8')
            self.db.put(b_key, b_key)
        end = time.time()
        return end - start

    def read(self):
        start = time.time()
        for key in self.keys:
            b_key = str(key).encode('utf-8')
            self.db.get(b_key)
        end = time.time()
        return end - start


if __name__ == '__main__':

    print('===== rocks DB =====')
    rocks = RocksDB()
    rocks.run()

    print('===== redis DB =====')
    redis_metric = RedisDB()
    redis_metric.run()
