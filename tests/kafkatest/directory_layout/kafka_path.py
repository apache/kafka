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

import importlib
import os

from kafkatest.version import get_version, KafkaVersion, DEV_BRANCH, LATEST_3_5


"""This module serves a few purposes:

First, it gathers information about path layout in a single place, and second, it
makes the layout of the Kafka installation pluggable, so that users are not forced
to use the layout assumed in the KafkaPathResolver class.

To run system tests using your own path resolver, use for example:

ducktape <TEST_PATH> --globals '{"kafka-path-resolver": "my.path.resolver.CustomResolverClass"}'
"""

SCRATCH_ROOT = "/mnt"
KAFKA_INSTALL_ROOT = "/opt"
KAFKA_PATH_RESOLVER_KEY = "kafka-path-resolver"
KAFKA_PATH_RESOLVER = "kafkatest.directory_layout.kafka_path.KafkaSystemTestPathResolver"

# Variables for jar path resolution
CORE_JAR_NAME = "core"
CORE_LIBS_JAR_NAME = "core-libs"
CORE_DEPENDANT_TEST_LIBS_JAR_NAME = "core-dependant-testlibs"
TOOLS_JAR_NAME = "tools"
TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME = "tools-dependant-libs"
CONNECT_FILE_JAR = "connect-file"

JARS = {
    "dev": {
        CORE_JAR_NAME: "core/build/*/*.jar",
        CORE_LIBS_JAR_NAME: "core/build/libs/*.jar",
        CORE_DEPENDANT_TEST_LIBS_JAR_NAME: "core/build/dependant-testlibs/*.jar",
        TOOLS_JAR_NAME: "tools/build/libs/kafka-tools*.jar",
        TOOLS_DEPENDANT_TEST_LIBS_JAR_NAME: "tools/build/dependant-libs*/*.jar",
        CONNECT_FILE_JAR: "connect/file/build/libs/connect-file*.jar"
    },
    # This version of the file connectors does not contain ServiceLoader manifests
    LATEST_3_5.__str__(): {
        CONNECT_FILE_JAR: "libs/connect-file*.jar"
    }
}


def create_path_resolver(context, project="kafka"):
    """Factory for generating a path resolver class

    This will first check for a fully qualified path resolver classname in context.globals.

    If present, construct a new instance, else default to KafkaSystemTestPathResolver
    """
    assert project is not None

    if KAFKA_PATH_RESOLVER_KEY in context.globals:
        resolver_fully_qualified_classname = context.globals[KAFKA_PATH_RESOLVER_KEY]
    else:
        resolver_fully_qualified_classname = KAFKA_PATH_RESOLVER

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


class KafkaSystemTestPathResolver(object):
    """Path resolver for Kafka system tests which assumes the following layout:

        /opt/kafka-dev          # Current version of kafka under test
        /opt/kafka-0.9.0.1      # Example of an older version of kafka installed from tarball
        /opt/kafka-<version>    # Other previous versions of kafka
        ...
    """
    def __init__(self, context, project="kafka"):
        self.context = context
        self.project = project

    def home(self, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        home_dir = project or self.project
        if version is not None:
            home_dir += "-%s" % str(version)

        return os.path.join(KAFKA_INSTALL_ROOT, home_dir)

    def bin(self, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.home(version, project=project), "bin")

    def script(self, script_name, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.bin(version, project=project), script_name)

    def jar(self, jar_name, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.home(version, project=project), JARS[str(version)][jar_name])

    def scratch_space(self, service_instance):
        return os.path.join(SCRATCH_ROOT, service_instance.service_id)

    def _version(self, node_or_version):
        if isinstance(node_or_version, KafkaVersion):
            return node_or_version
        else:
            return get_version(node_or_version)

