from __future__ import print_function, division

from pyspark.sql import SparkSession


class Converter(object):

    def __init__(self, src_format=None, src_path=None, **kwargs):
        self.spark = SparkSession.builder.getOrCreate()
        self._df = None
        if src_format and src_path:
            self.read(src_format=src_format, path=src_path, **kwargs)

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, value):
        self._df = value

    def read(self, src_format, path, **kwargs):
        self.df = self.spark.read.format(src_format).load(path=path, **kwargs)
        return self

    def write(self, format, path, **kwargs):
        if self.df:
            self.df.write.format(format).save(path=path, **kwargs)
        return self
