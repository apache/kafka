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

from .path_resolver import PathResolver
from kafkatest.version.version import TRUNK

import importlib
import os

from distutils.version import LooseVersion



# TODO - describe how overriding can work
"""This module contains tools for resolving the location of directories and scripts.


"""

DEFAULT_SCRATCH_ROOT = "/mnt"
DEFAULT_KAFKA_INSTALL_ROOT = "/opt"
KAFKA_PATH_RESOLVER_KEY = "kafka-path-resolver"
DEFAULT_KAFKA_PATH_RESOLVER = "kafkatest.directory_layout.kafka_path.KafkaSystemTestPathResolver"


def create_path_resolver(context, project="kafka"):
    """Factory for generating a path resolver class

    This will first check for a fully qualified path resolver classname in context.globals.

    If present, construct a new instance, else default to KafkaSystemTestPathResolver
    """
    assert project is not None

    if KAFKA_PATH_RESOLVER_KEY in context.globals:
        resolver_fully_qualified_classname = context.globals[KAFKA_PATH_RESOLVER_KEY]
    else:
        resolver_fully_qualified_classname = DEFAULT_KAFKA_PATH_RESOLVER

    # Using the fully qualified classname, import the resolver class
    (module_name, resolver_class_name) = resolver_fully_qualified_classname.rsplit('.', 1)
    cluster_mod = importlib.import_module(module_name)
    path_resolver_class = getattr(cluster_mod, resolver_class_name)
    path_resolver = path_resolver_class(context, project)

    return path_resolver


class KafkaSystemTestPathResolver(PathResolver):
    """Path resolver for Kafka system tests which assumes the following layout:

        /opt/kafka-trunk        # Current version of kafka under test
        /opt/kafka-0.9.0.1      # Example of an older version of kafka installed from tarball
        /opt/kafka-<version>    # Other previous versions of kafka
        ...
    """
    def __init__(self, context, project="kafka"):
        super(KafkaSystemTestPathResolver, self).__init__(context, project)
        assert project is not None
        self.project = project

    def _version(self, project=None, node_or_version=None):
        """Get the version attached to the given node, if present.
        Special-case for kafka - default version here is TRUNK
        """

        if isinstance(node_or_version, LooseVersion):
            return node_or_version

        # Assume it's a node
        node = node_or_version
        if node is not None and hasattr(node, "version"):
            return node.version
        elif project == "kafka":
            # default version for kafka is "trunk"
            return TRUNK
        else:
            return None

    def home(self, project=None, node_or_version=None):
        project = project or self.project
        version = self._version(project, node_or_version)

        home_dir = project
        if version is not None:
            home_dir += "-%s" % str(version)

        return os.path.join(DEFAULT_KAFKA_INSTALL_ROOT, home_dir)

    def bin(self, project=None, node_or_version=None):
        project = project or self.project
        version = self._version(project, node_or_version)

        return os.path.join(self.home(project, version), "bin")

    def script(self, script_name, project=None, node_or_version=None):
        project = project or self.project
        version = self._version(project, node_or_version)

        return os.path.join(self.bin(project, version), script_name)

    def jar(self, project=None, node_or_version=None):
        raise NotImplementedError("KafkaSystemTestPathResolver currently does not support the jar method")

    def scratch_space(self, service_instance):
        return os.path.join(DEFAULT_SCRATCH_ROOT, service_instance.name)
