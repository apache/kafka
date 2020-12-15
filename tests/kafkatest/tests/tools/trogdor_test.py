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

from kafkatest.services.trogdor.network_partition_fault_spec import NetworkPartitionFaultSpec

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.no_op_task_spec import NoOpTaskSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.utils import node_is_reachable


class TrogdorTest(Test):
    """
    Tests the Trogdor fault injection daemon in isolation.
    """

    def __init__(self, test_context):
        super(TrogdorTest, self).__init__(test_context)

    def set_up_trogdor(self, num_agent_nodes):
        self.agent_nodes = self.test_context.cluster.alloc(ClusterSpec.simple_linux(num_agent_nodes))
        self.trogdor = TrogdorService(context=self.test_context, agent_nodes=self.agent_nodes)
        for agent_node in self.agent_nodes:
            agent_node.account.logger = self.trogdor.logger
        self.trogdor.start()

    def setUp(self):
        self.trogdor = None
        self.agent_nodes = None

    def tearDown(self):
        if self.trogdor is not None:
            self.trogdor.stop()
            self.trogdor = None
        if self.agent_nodes is not None:
            self.test_context.cluster.free(self.agent_nodes)
            self.agent_nodes = None

    @cluster(num_nodes=4)
    def test_trogdor_service(self):
        """
        Test that we can bring up Trogdor and create a no-op fault.
        """
        self.set_up_trogdor(3)
        spec = NoOpTaskSpec(0, TaskSpec.MAX_DURATION_MS)
        self.trogdor.create_task("myfault", spec)
        def check_for_myfault():
            faults = self.trogdor.tasks()["tasks"]
            self.logger.info("tasks = %s" % faults)
            return "myfault" in faults
        wait_until(lambda: check_for_myfault,
                   timeout_sec=10, backoff_sec=.2, err_msg="Failed to read back myfault.")
        self.trogdor.stop_task("myfault")

    @cluster(num_nodes=4)
    def test_network_partition_fault(self):
        """
        Test that the network partition fault results in a true network partition between nodes.
        """
        self.set_up_trogdor(3)
        spec = NetworkPartitionFaultSpec(0, TaskSpec.MAX_DURATION_MS,
                                            [[self.agent_nodes[0]], self.agent_nodes[1:]])
        partitions = spec.message["partitions"]
        assert 2 == len(partitions)
        assert [self.agent_nodes[0].name] == partitions[0]
        assert [self.agent_nodes[1].name, self.agent_nodes[2].name] == partitions[1]
        self.trogdor.create_task("partition0", spec)
        def verify_nodes_partitioned():
            if node_is_reachable(self.agent_nodes[0], self.agent_nodes[1]):
                return False
            if node_is_reachable(self.agent_nodes[1], self.agent_nodes[0]):
                return False
            if node_is_reachable(self.agent_nodes[2], self.agent_nodes[0]):
                return False
            return True
        wait_until(lambda: verify_nodes_partitioned,
                   timeout_sec=10, backoff_sec=.2, err_msg="Failed to verify that the nodes were partitioned.")
        if not node_is_reachable(self.agent_nodes[0], self.agent_nodes[0]):
            raise RuntimeError("Node 0 must be reachable from itself.")
        if not node_is_reachable(self.agent_nodes[1], self.agent_nodes[2]):
            raise RuntimeError("Node 2 must be reachable from node 1.")
        if not node_is_reachable(self.agent_nodes[2], self.agent_nodes[1]):
            raise RuntimeError("Node 1 must be reachable from node 2.")
