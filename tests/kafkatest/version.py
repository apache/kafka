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


from kafkatest.utils import kafkatest_version

from distutils.version import LooseVersion


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


def get_version(node=None):
    """Return the version attached to the given node.
    Default to DEV_BRANCH if node or node.version is undefined (aka None)
    """
    if node is not None and hasattr(node, "version") and node.version is not None:
        return node.version
    else:
        return DEV_BRANCH

DEV_BRANCH = KafkaVersion("dev")
DEV_VERSION = KafkaVersion("1.2.0-SNAPSHOT")

# 0.8.2.X versions
V_0_8_2_1 = KafkaVersion("0.8.2.1")
V_0_8_2_2 = KafkaVersion("0.8.2.2")
LATEST_0_8_2 = V_0_8_2_2

# 0.9.0.X versions
V_0_9_0_0 = KafkaVersion("0.9.0.0")
V_0_9_0_1 = KafkaVersion("0.9.0.1")
LATEST_0_9 = V_0_9_0_1

# 0.10.0.X versions
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
LATEST_0_10_2 = V_0_10_2_1

LATEST_0_10 = LATEST_0_10_2

# 0.11.0.x versions
V_0_11_0_0 = KafkaVersion("0.11.0.0")
V_0_11_0_1 = KafkaVersion("0.11.0.1")
V_0_11_0_2 = KafkaVersion("0.11.0.2")
LATEST_0_11_0 = V_0_11_0_2
LATEST_0_11 = LATEST_0_11_0

# 1.0.x versions
V_1_0_0 = KafkaVersion("1.0.0")
V_1_0_1 = KafkaVersion("1.0.1")
LATEST_1_0 = V_1_0_1

# 1.1.x versions
V_1_1_0 = KafkaVersion("1.1.0")
LATEST_1_1 = V_1_1_0
