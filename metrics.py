import rocksdb
import redis
import subprocess
import shortuuid
import time
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
        N = 5 * 10000
        self.keys = range(N)

    def run(self):
        tries = 1
        times_writes = []
        times_reads = []
        for i in range(tries):
            self.reset()
            self.write()
            # times_writes.append()
            # times_reads.append(self.read())
        print(times_writes)
        print(times_reads)


class Redis(MetricBase):
    def __init__(self, db):
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
        super().__init__()
        self.reset()

    def reset(self):
        subprocess.check_output(['rm','-rf', 'puts.db'])
        self.db = rocksdb.DB(
            "puts.db", rocksdb.Options(create_if_missing=True))

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
    rocks = RocksDB()
    rocks.run()
    # rocks.write()
    # rocks.read()
