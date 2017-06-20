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

from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from kafkatest.fault.network_fracture import NetworkFracture

class TestNetworkFracture(Test):
    """
    Sanity checks that the network fracture fault is effective at preventing
    network communication between partitions.
    """
    def __init__(self, test_context):
        super(TestNetworkFracture, self).__init__(test_context)

    def min_cluster_size(self):
        return 6

    @cluster(num_nodes=6)
    def test_partition(self):
        group_a = self.test_context.cluster.alloc(3)
        group_b = self.test_context.cluster.alloc(3)
        network_fracture = NetworkFracture([group_a, group_b])
        self.logger.info("Created node groups [%s] and [%s]." % \
                         (", ".join(network_fracture.node_to_ip_address(node) for node in group_a),
                          ", ".join(network_fracture.node_to_ip_address(node) for node in group_b)))
        self.logger.info("Created %s" % str(network_fracture))
        network_fracture.start()
        for group in [group_a, group_b]:
            for node in group:
                for node2 in group:
                    self.check_connectivity(network_fracture, True, node, node2)
        for node_a in group_a:
            for node_b in group_b:
                self.check_connectivity(network_fracture, False, node_a, node_b)
                self.check_connectivity(network_fracture, False, node_b, node_a)
        network_fracture.stop()
        self.logger.info("Successfully checked connectivity.")

    """
    Check if node_a can connect to port 22 (the ssh port) of node_b.
    """
    def check_connectivity(self, network_fracture, expect_success, node_a, node_b):
        if expect_success:
            prequel = ""
        else:
            prequel = "! "
        node_a.account.ssh("%snc -w 1 -z %s 22" % (prequel, network_fracture.node_to_ip_address(node_b)))
