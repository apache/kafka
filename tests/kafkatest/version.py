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


from distutils.version import LooseVersion
from kafkatest.utils import kafkatest_version


class KafkaVersion(LooseVersion):
    """Container for kafka versions which makes versions simple to compare.

    distutils.version.LooseVersion (and StrictVersion) has robust comparison and ordering logic.

    Example:

        v10 = KafkaVersion("0.10.0")
        v9 = KafkaVersion("0.9.0.1")
        assert v10 > v9  # assertion passes!
    """
    def __init__(self, version_string):
        self.is_dev = (version_string.lower() == "dev")
        if self.is_dev:
            version_string = kafkatest_version()

            # Drop dev suffix if present
            dev_suffix_index = version_string.find(".dev")
            if dev_suffix_index >= 0:
                version_string = version_string[:dev_suffix_index]

        # Don't use the form super.(...).__init__(...) because
        # LooseVersion is an "old style" python class
        LooseVersion.__init__(self, version_string)

    def __str__(self):
        if self.is_dev:
            return "dev"
        else:
            return LooseVersion.__str__(self)

    def _cmp(self, other):
        if isinstance(other, str):
            other = KafkaVersion(other)

        if other.is_dev:
            if self.is_dev:
                return 0
            return -1
        elif self.is_dev:
            return 1

        return LooseVersion._cmp(self, other)

    def supports_named_listeners(self):
        return self >= V_0_10_2_0

    def acl_command_supports_bootstrap_server(self):
        return self >= V_2_1_0

    def topic_command_supports_bootstrap_server(self):
        return self >= V_2_3_0

    def topic_command_supports_if_not_exists_with_bootstrap_server(self):
        return self >= V_2_6_0

    def supports_tls_to_zookeeper(self):
        # indicate if KIP-515 is available
        return self >= V_2_5_0

    def reassign_partitions_command_supports_bootstrap_server(self):
        return self >= V_2_5_0

    def kafka_configs_command_uses_bootstrap_server(self):
        # everything except User SCRAM Credentials (KIP-554)
        return self >= V_2_6_0

    def kafka_configs_command_uses_bootstrap_server_scram(self):
        # User SCRAM Credentials (KIP-554)
        return self >= V_2_7_0

def get_version(node=None):
    """Return the version attached to the given node.
    Default to DEV_BRANCH if node or node.version is undefined (aka None)
    """
    if node is not None and hasattr(node, "version") and node.version is not None:
        return node.version
    else:
        return DEV_BRANCH

DEV_BRANCH = KafkaVersion("dev")
DEV_VERSION = KafkaVersion("2.8.0-SNAPSHOT")

# 0.8.2.x versions
V_0_8_2_1 = KafkaVersion("0.8.2.1")
V_0_8_2_2 = KafkaVersion("0.8.2.2")
LATEST_0_8_2 = V_0_8_2_2

# 0.9.0.x versions
V_0_9_0_0 = KafkaVersion("0.9.0.0")
V_0_9_0_1 = KafkaVersion("0.9.0.1")
LATEST_0_9 = V_0_9_0_1

# 0.10.0.x versions
V_0_10_0_0 = KafkaVersion("0.10.0.0")
V_0_10_0_1 = KafkaVersion("0.10.0.1")
LATEST_0_10_0 = V_0_10_0_1

# 0.10.1.x versions
V_0_10_1_0 = KafkaVersion("0.10.1.0")
V_0_10_1_1 = KafkaVersion("0.10.1.1")
LATEST_0_10_1 = V_0_10_1_1

# 0.10.2.x versions
V_0_10_2_0 = KafkaVersion("0.10.2.0")
V_0_10_2_1 = KafkaVersion("0.10.2.1")
V_0_10_2_2 = KafkaVersion("0.10.2.2")
LATEST_0_10_2 = V_0_10_2_2

LATEST_0_10 = LATEST_0_10_2

# 0.11.0.x versions
V_0_11_0_0 = KafkaVersion("0.11.0.0")
V_0_11_0_1 = KafkaVersion("0.11.0.1")
V_0_11_0_2 = KafkaVersion("0.11.0.2")
V_0_11_0_3 = KafkaVersion("0.11.0.3")
LATEST_0_11_0 = V_0_11_0_3
LATEST_0_11 = LATEST_0_11_0

# 1.0.x versions
V_1_0_0 = KafkaVersion("1.0.0")
V_1_0_1 = KafkaVersion("1.0.1")
V_1_0_2 = KafkaVersion("1.0.2")
LATEST_1_0 = V_1_0_2

# 1.1.x versions
V_1_1_0 = KafkaVersion("1.1.0")
V_1_1_1 = KafkaVersion("1.1.1")
LATEST_1_1 = V_1_1_1

# 2.0.x versions
V_2_0_0 = KafkaVersion("2.0.0")
V_2_0_1 = KafkaVersion("2.0.1")
LATEST_2_0 = V_2_0_1

# 2.1.x versions
V_2_1_0 = KafkaVersion("2.1.0")
V_2_1_1 = KafkaVersion("2.1.1")
LATEST_2_1 = V_2_1_1

# 2.2.x versions
V_2_2_0 = KafkaVersion("2.2.0")
V_2_2_1 = KafkaVersion("2.2.1")
V_2_2_2 = KafkaVersion("2.2.2")
LATEST_2_2 = V_2_2_2

# 2.3.x versions
V_2_3_0 = KafkaVersion("2.3.0")
V_2_3_1 = KafkaVersion("2.3.1")
LATEST_2_3 = V_2_3_1

# 2.4.x versions
V_2_4_0 = KafkaVersion("2.4.0")
V_2_4_1 = KafkaVersion("2.4.1")
LATEST_2_4 = V_2_4_1

# 2.5.x versions
V_2_5_0 = KafkaVersion("2.5.0")
V_2_5_1 = KafkaVersion("2.5.1")
LATEST_2_5 = V_2_5_1

# 2.6.x versions
V_2_6_0 = KafkaVersion("2.6.0")
LATEST_2_6 = V_2_6_0

# 2.7.x versions
V_2_7_0 = KafkaVersion("2.7.0")
LATEST_2_7 = V_2_7_0
