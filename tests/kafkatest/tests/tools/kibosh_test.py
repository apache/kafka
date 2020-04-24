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

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.trogdor.task_spec import TaskSpec
from kafkatest.services.trogdor.files_unreadable_fault_spec import FilesUnreadableFaultSpec
from kafkatest.services.trogdor.kibosh import KiboshService
import json


class KiboshTest(Test):
    TARGET = "/mnt/kibosh-target"
    MIRROR = "/mnt/kibosh-mirror"

    """
    Tests the Kibosh fault injection filesystem in isolation.
    """

    def __init__(self, test_context):
        super(KiboshTest, self).__init__(test_context)

    def set_up_kibosh(self, num_nodes):
        self.nodes = self.test_context.cluster.alloc(ClusterSpec.simple_linux(num_nodes))
        for node in self.nodes:
            node.account.logger = self.logger
            node.account.ssh("mkdir -p -- %s %s" % (KiboshTest.TARGET, KiboshTest.MIRROR))
        self.kibosh = KiboshService(self.test_context, self.nodes,
                                    KiboshTest.TARGET, KiboshTest.MIRROR)
        for node in self.nodes:
            node.account.logger = self.kibosh.logger
        self.kibosh.start()

    def setUp(self):
        self.kibosh = None
        self.nodes = None

    def tearDown(self):
        if self.kibosh is not None:
            self.kibosh.stop()
            self.kibosh = None
        for node in self.nodes:
            node.account.ssh("rm -rf -- %s %s" % (KiboshTest.TARGET, KiboshTest.MIRROR))
        if self.nodes is not None:
            self.test_context.cluster.free(self.nodes)
            self.nodes = None

    @cluster(num_nodes=4)
    def test_kibosh_service(self):
        pass
        """
        Test that we can bring up kibosh and create a fault.
        """
        self.set_up_kibosh(3)
        spec = FilesUnreadableFaultSpec(0, TaskSpec.MAX_DURATION_MS,
                [self.nodes[0].name], KiboshTest.TARGET, "/foo", 12)
        node = self.nodes[0]

        def check(self, node, expected_json):
            fault_json = self.kibosh.get_fault_json(node)
            self.logger.info("Read back: [%s].  Expected: [%s]." % (fault_json, expected_json))
            return fault_json == expected_json

        wait_until(lambda: check(self, node, '{"faults":[]}'),
                   timeout_sec=10, backoff_sec=.2, err_msg="Failed to read back initial empty fault array.")
        self.kibosh.set_faults(node, [spec])
        wait_until(lambda: check(self, node, json.dumps({"faults": [spec.kibosh_message]})),
                   timeout_sec=10, backoff_sec=.2, err_msg="Failed to read back fault array.")
        self.kibosh.set_faults(node, [])
        wait_until(lambda: check(self, node, "{}"),
                   timeout_sec=10, backoff_sec=.2, err_msg="Failed to read back final empty fault array.")
