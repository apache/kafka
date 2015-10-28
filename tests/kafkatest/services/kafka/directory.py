# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# "trunk" installation of kafka
KAFKA_TRUNK = "kafka-trunk"


def kafka_dir(node=None):
    """Return name of kafka directory for the given node.

    This provides a convenient way to support different versions of kafka or kafka tools running
    on different nodes.
    """
    if node is None:
        return KAFKA_TRUNK

    if not hasattr(node, "version"):
        return KAFKA_TRUNK

    return "kafka-" + str(node.version)