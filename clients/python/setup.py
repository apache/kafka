#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.core import setup
 
setup(
    name='kafka-python-client',
    version='0.6',
    description='This library implements a Kafka client',
    author='LinkedIn.com',
    url='https://github.com/kafka-dev/kafka',
    package_dir={'': '.'},
    py_modules=[
        'kafka',
    ],
)
