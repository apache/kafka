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

    def __init__(self, start_ms, duration_ms, node_specs):
        """
        Create a new NetworkDegradeFaultSpec.

        :param start_ms:        The start time, as described in task_spec.py
        :param duration_ms:     The duration in milliseconds.
        :param node_latencies:  A dict of node name to desired latency
        :param network_device:  The name of the network device
        """
        super(DegradedNetworkFaultSpec, self).__init__(start_ms, duration_ms)
        self.message["class"] = "org.apache.kafka.trogdor.fault.DegradedNetworkFaultSpec"
        self.message["nodeSpecs"] = node_specs
