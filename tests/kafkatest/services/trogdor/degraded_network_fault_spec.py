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


class DegradedNetworkFaultSpec(TaskSpec):
    """
    The specification for a network degradation fault.

    Degrades the network so that traffic on a subset of nodes has higher latency
    """

    def __init__(self, start_ms, duration_ms):
        """
        Create a new NetworkDegradeFaultSpec.

        :param start_ms:        The start time, as described in task_spec.py
        :param duration_ms:     The duration in milliseconds.
        """
        super(DegradedNetworkFaultSpec, self).__init__(start_ms, duration_ms)
        self.message["class"] = "org.apache.kafka.trogdor.fault.DegradedNetworkFaultSpec"
        self.message["nodeSpecs"] = {}

    def add_node_spec(self, node, networkDevice, latencyMs=0, rateLimitKbit=0):
        """
        Add a node spec to this fault spec
        :param node:            The node name which is to be degraded
        :param networkDevice:   The network device name (e.g., eth0) to apply the degradation to
        :param latencyMs:       Optional. How much latency to add to each packet
        :param rateLimitKbit:   Optional. Maximum throughput in kilobits per second to allow
        :return:
        """
        self.message["nodeSpecs"][node] = {
            "rateLimitKbit": rateLimitKbit, "latencyMs": latencyMs, "networkDevice": networkDevice
        }
