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


from kafkatest.directory_layout.kafka_path import create_path_resolver, KafkaSystemTestPathResolver, \
    KAFKA_PATH_RESOLVER_KEY
from kafkatest.version.version import V_0_9_0_1


class DummyContext(object):
    def __init__(self):
        self.globals = {}


class DummyPathResolver(object):
    """Dummy class to help check path resolver creation."""
    pass


class CheckCreatePathResolver(object):
    def check_create_path_resolver_override(self):
        """Test override behavior when instantiating a path resolver using our factory function.

        If context.globals has an entry for a path resolver class, use that class instead of the default.
        """
        mock_context = DummyContext()
        mock_context.globals[KAFKA_PATH_RESOLVER_KEY] = \
            "kafkatest_unit_test.directory_layout.check_project_paths.DummyPathResolver"

        resolver = create_path_resolver(mock_context)
        assert type(resolver) == DummyPathResolver

    def check_create_path_resolver_default(self):
        """Test default behavior when instantiating a path resolver using our factory function.
        """
        resolver = create_path_resolver(DummyContext())
        assert type(resolver) == KafkaSystemTestPathResolver

    def check_source_paths(self):
        """Check expected path resolution when installing from source. Aka SOURCE_INSTALL."""
        resolver = create_path_resolver(DummyContext())

        assert resolver.home() == "/opt/kafka-trunk"
        assert resolver.bin() == "/opt/kafka-trunk/bin"
        assert resolver.script("kafka-run-class.sh") == "/opt/kafka-trunk/bin/kafka-run-class.sh"

    def check_versioned_source_paths(self):
        """Check expected paths when using versions."""
        resolver = create_path_resolver(DummyContext())

        assert resolver.home(V_0_9_0_1) == "/opt/kafka-0.9.0.1"
        assert resolver.bin(V_0_9_0_1) == "/opt/kafka-0.9.0.1/bin"
        assert resolver.script("kafka-run-class.sh", V_0_9_0_1) == "/opt/kafka-0.9.0.1/bin/kafka-run-class.sh"
