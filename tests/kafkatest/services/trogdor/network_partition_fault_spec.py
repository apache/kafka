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

from kafkatest.services.trogdor.task_spec import TaskSpec


class NetworkPartitionFaultSpec(TaskSpec):
    """
    The specification for a network partition fault.

    Network partition faults fracture the network into different partitions
    that cannot communicate with each other.
    """

    def __init__(self, start_ms, duration_ms, partitions):
        """
        Create a new NetworkPartitionFaultSpec.

        :param start_ms:        The start time, as described in task_spec.py
        :param duration_ms:     The duration in milliseconds.
        :param partitions:      An array of arrays describing the partitions.
                                The inner arrays may contain either node names,
                                or ClusterNode objects.
        """
        super(NetworkPartitionFaultSpec, self).__init__(start_ms, duration_ms)
        self.message["class"] = "org.apache.kafka.trogdor.fault.NetworkPartitionFaultSpec"
        self.message["partitions"] = [TaskSpec.to_node_names(p) for p in partitions]
