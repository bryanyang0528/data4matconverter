from __future__ import print_function, division

from pyspark.sql import SparkSession


class Converter(object):

    def __init__(self, src_format, src_path, **kwargs):
        self.spark = SparkSession.builder.getOrCreate()
        self._df = self.read(src_format=src_format, path=src_path, **kwargs)

    @property
    def df(self):
        return self._df

    def read(self, src_format, path, **kwargs):
        self._df = self.spark.read.format(src_format).load(path=path, **kwargs)
        return self

    def write(self, dest_format, path, **kwargs):
        self._df.write.format(dest_format).save(path=path, **kwargs)
        return self
