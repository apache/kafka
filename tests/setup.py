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
from setuptools import find_packages, setup, Command

version = ''
with open('kafkatest/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)


class PyTest(Command):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        self.pytest_args = []

    def finalize_options(self):
        self.test_args = []
        self.test_suite = True

    def run(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        print(self.pytest_args)
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

# Note: when changing the version of ducktape, also revise tests/docker/Dockerfile
setup(name="kafkatest",
      version=version,
      description="Apache Kafka System Tests",
      author="Apache Kafka",
      platforms=["any"],
      license="apache2.0",
      packages=find_packages(),
      include_package_data=True,
      install_requires=["ducktape==0.12.0", "requests==2.31.0", "psutil==5.7.2", "pytest==8.3.3", "mock==5.1.0"],
      cmdclass={'test': PyTest},
      zip_safe=False
      )
