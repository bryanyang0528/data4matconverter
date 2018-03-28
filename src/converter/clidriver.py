from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys
import argparse
import logging
from converter import Converter

logger = logging.getLogger(__name__)


def main(argv):
    CLIDriver(argv)


class CLIDriver(object):
    def __init__(self, argv):
        parser = argparse.ArgumentParser(description='Welcome to Data4matConverter.',
                                         usage='''convert <command> [<args>].
                        ''')

        if len(argv) == 1:
            parser.print_help()
            sys.exit(1)

        def exit(result):
            sys.exit(1 - result)

        parser.add_argument(
            '--src_path',
            required=True,
            type=str,
            metavar='file/path/fileA.csv',
            help='path of data source'
        )

        parser.add_argument(
            '--src_format',
            required=True,
            type=str,
            metavar='csv',
            help='format of the data source'
        )

        parser.add_argument(
            '--dest_path',
            required=True,
            type=str,
            metavar='file/path/fileA.csv',
            help='path of data source'
        )

        parser.add_argument(
            '--dest_format',
            required=True,
            type=str,
            metavar='csv',
            help='format of the data source'
        )

        parser.add_argument(
            '--src_property',
            nargs='+',
            required=False
        )

        parser.add_argument(
            '--dest_property',
            nargs='+',
            required=False
        )

        parser.set_defaults(
            func=self.run
        )

        args = parser.parse_args(argv[1:])
        inputs = vars(args)

        src_properties = inputs.pop('src_property', None)
        if src_properties:
            inputs['src_properties'] = (self.parse_properties(src_properties))

        dest_properties = inputs.pop('dest_property', None)
        if dest_properties:
            inputs['dest_properties'] = (self.parse_properties(dest_properties))

        func = inputs.pop('func')
        exit(func(**inputs))

    @staticmethod
    def parse_properties(properties):
        inputs = {}
        for p in properties:
            k, v = p.split('=')
            inputs[k] = v
        return inputs

    @staticmethod
    def run(**kwargs):
        logger.info(kwargs)
        src_path = kwargs.pop('src_path')
        src_format = kwargs.pop('src_format')
        src_properties = kwargs.pop('src_properties', None)
        dest_path = kwargs.pop('dest_path')
        dest_format = kwargs.pop('dest_format')
        dest_properties = kwargs.pop('dest_properties', None)

        if src_properties:
            converter = Converter(src_format=src_format, src_path=src_path, **src_properties)
        else:
            converter = Converter(src_format=src_format, src_path=src_path)

        converter.df.show()

        if dest_properties:
            converter.write(format=dest_format, path=dest_path, **dest_properties)
        else:
            converter.write(format=dest_format, path=dest_path)

        converter.spark.stop()
        return True
