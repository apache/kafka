# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import sys
from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand

version = ''
with open('kafkatest/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        print self.pytest_args
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(name="kafkatest",
      version=version,
      description="Apache Kafka System Tests",
      author="Apache Kafka",
      platforms=["any"], 
      license="apache2.0",
      packages=find_packages(),
      include_package_data=True,
      install_requires=["ducktape==0.6.0", "requests>=2.5.0"],
      tests_require=["pytest", "mock"],
      cmdclass={'test': PyTest},
      )
