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

from kafkatest.directory_layout.path_resolver import PathResolver
from kafkatest.version.version import get_version, KafkaVersion, TRUNK

import importlib
import os


# TODO - describe how overriding can work
"""This module contains tools for resolving the location of directories and scripts.


"""

DEFAULT_SCRATCH_ROOT = "/mnt"
DEFAULT_KAFKA_INSTALL_ROOT = "/opt"
KAFKA_PATH_RESOLVER_KEY = "kafka-path-resolver"
DEFAULT_KAFKA_PATH_RESOLVER = "kafkatest.directory_layout.kafka_path.KafkaSystemTestPathResolver"


JARS = {
    "trunk": {
        "core": ["core/build/*/*.jar"],
        "tools": ["tools/build/libs/kafka-tools*.jar", "tools/build/dependant-libs*/*.jar"]
    }
}


"""
core jars

def core_jar_paths(self, node, lib_dir_name):
    lib_dir = "%s/core/build/%s" % home, lib_dir_name)
    jars = node.account.ssh_capture("ls " + lib_dir)
    return [os.path.join(lib_dir, jar.strip()) for jar in jars]
"""


"""
tools jars - used in verifiable producer

cmd += "for file in %s/tools/build/libs/kafka-tools*.jar; do CLASSPATH=$CLASSPATH:$file; done; " % kafka_trunk_home
cmd += "for file in %s/tools/build/dependant-libs-${SCALA_VERSION}*/*.jar; do CLASSPATH=$CLASSPATH:$file; done; " % kafka_trunk_home
cmd += "export CLASSPATH; "
"""


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


class KafkaPathResolverMixin(object):
    """Mixin to automatically provide pluggable path resolution functionality to any class using it.

    Keep life simple, and don't add a constructor to this class:
    Since use of a mixin entails multiple inheritence, it is *much* simpler to reason about the interaction of this
    class with subclasses if we don't have to worry about method resolution order, constructor signatures etc.
    """

    @property
    def path(self):
        if not hasattr(self, "_path"):
            setattr(self, "_path", create_path_resolver(self.context, "kafka"))
            if hasattr(self.context, "logger") and self.context.logger is not None:
                self.context.logger.debug("Using path resolver %s" % self._path.__class__.__name__)

        return self._path


class KafkaSystemTestPathResolver(PathResolver):
    """Path resolver for Kafka system tests which assumes the following layout:

        /opt/kafka-trunk        # Current version of kafka under test
        /opt/kafka-0.9.0.1      # Example of an older version of kafka installed from tarball
        /opt/kafka-<version>    # Other previous versions of kafka
        ...
    """
    def __init__(self, context, project="kafka"):
        super(KafkaSystemTestPathResolver, self).__init__(context, project=project)

    def home(self, node_or_version=TRUNK):
        version = self._version(node_or_version)
        home_dir = self.project
        if version is not None:
            home_dir += "-%s" % str(version)

        return os.path.join(DEFAULT_KAFKA_INSTALL_ROOT, home_dir)

    def bin(self, node_or_version=TRUNK):
        version = self._version(node_or_version)
        return os.path.join(self.home(version), "bin")

    def script(self, script_name, node_or_version=TRUNK):
        version = self._version(node_or_version)
        return os.path.join(self.bin(version), script_name)

    def jar(self, jar_name, node_or_version=TRUNK):
        version = self._version(node_or_version)
        return JARS[version][jar_name]

    def scratch_space(self, service_instance):
        return os.path.join(DEFAULT_SCRATCH_ROOT, service_instance.name)

    def _version(self, node_or_version):
        if isinstance(node_or_version, KafkaVersion):
            return node_or_version
        else:
            return get_version(node_or_version)

