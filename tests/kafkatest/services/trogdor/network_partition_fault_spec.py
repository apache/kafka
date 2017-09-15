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

from kafkatest.services.trogdor.fault_spec import FaultSpec


class NetworkPartitionFaultSpec(FaultSpec):
    """
    The specification for a network partition fault.

    Network partition faults fracture the network into different partitions
    that cannot communicate with each other.
    """

    def __init__(self, start_ms, duration_ms, partitions):
        """
        Create a new NetworkPartitionFaultSpec.

        :param start_ms:        The start time, as described in fault_spec.py
        :param duration_ms:     The duration in milliseconds.
        :param partitions:      An array of arrays describing the partitions.
                                The inner arrays may contain either node names,
                                or ClusterNode objects.
        """
        super(NetworkPartitionFaultSpec, self).__init__(start_ms, duration_ms)
        self.partitions = []
        for partition in partitions:
            nodes = []
            for obj in partition:
                if isinstance(obj, basestring):
                    nodes.append(obj)
                else:
                    nodes.append(obj.name)
            self.partitions.append(nodes)

    def message(self):
        return {
            "class": "org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec",
            "startMs": self.start_ms,
            "durationMs": self.duration_ms,
            "partitions": self.partitions,
        }
