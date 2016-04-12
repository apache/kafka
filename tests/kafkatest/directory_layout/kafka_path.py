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
import importlib
import os


# TODO - describe how overriding can work
"""This module contains tools for resolving the location of directories and scripts.


"""

DEFAULT_KAFKA_INSTALL_ROOT = "/opt"
PATH_RESOLVER_KEY = "kafka-path-resolver"


class KafkaSystemTestPathResolver(PathResolver):
    """Path resolver for Kafka system tests which assumes the following layout:

        /opt/kafka-trunk        # Current version of kafka under test
        /opt/kafka-0.9.0.1      # Example of an older version of kafka installed from tarball
        /opt/kafka-<version>    # Other previous versions of kafka
        ...
    """
    def __init__(self, context):
        super(KafkaSystemTestPathResolver, self).__init__(context)
        self.project_name = "kafka"

    def script_path(self, script_name, version):

        # first check for key in globals
        #
        # key = "%s-%s-script" % (self.project_name, script_name)
        # if key in self.context.globals:
        #     return self.context.globals[key]

        return os.path.join(self.project_bin(self.project_name, version), script_name)

    def project_home(self, project_name, version):

        home_dir = project_name
        if version is not None:
            home_dir += "-%s" % str(version)
        # key = home_dir + "-home"
        #
        # if key in self.context.globals:
        #     return self.context.globals[key]

        return os.path.join(DEFAULT_KAFKA_INSTALL_ROOT, home_dir)

    def project_bin(self, project_name, version):

        # key = project_name
        # if version is not None:
        #     key += "-" + str(version)
        # key += "-bin"
        #
        # # First check globals for override
        # if key in self.context.globals:
        #     return self.context.globals[key]

        return os.path.join(self.project_home(project_name, version), "bin")


def script_path(context, script_name, node=None):
        if PATH_RESOLVER_KEY in context.globals:
            (module_name, resolver_class_name) = context.globals[PATH_RESOLVER_KEY].rsplit('.', 1)
            cluster_mod = importlib.import_module(module_name)
            path_resolver_class = getattr(cluster_mod, resolver_class_name)
            path_resolver = path_resolver_class(context)
        else:
            path_resolver = KafkaSystemTestPathResolver(context)

        if node is None or not hasattr(node, "version"):
            version = None
        else:
            version = node.version

        return path_resolver.script_path(script_name, version)


def _home_key(node=None):
    if node is None or not hasattr(node, "version"):
        return "kafka-trunk"
    else:
        return "kafka-" + str(node.version)


def kafka_home(context, node=None):
    #TODO - delete me
    """Return name of kafka directory for the given node.

    This provides a convenient way to support different versions of kafka or kafka tools running
    on different nodes.
    """

    key = _home_key(node)

    if key in context.globals:
        return context.globals[key]
    else:
        # TODO - prepend /opt?
        return key

