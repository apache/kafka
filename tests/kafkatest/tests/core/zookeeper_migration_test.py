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

import os
from functools import partial
import time

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.config_property import CLUSTER_ID
from kafkatest.services.kafka.quorum import remote_kraft, ServiceQuorumInfo, zk
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.version import DEV_BRANCH


class TestMigration(EndToEndTest):
    def __init__(self, test_context):
        super(TestMigration, self).__init__(topic="zk-topic", test_context=test_context) # topic="zk-topic",

    def test_offline_migration(self):
        zk_quorum = partial(ServiceQuorumInfo, zk)
        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=DEV_BRANCH)
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, version=DEV_BRANCH, quorum_info_provider=zk_quorum, allow_zk_with_kraft=True)

        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = "PLAINTEXT"

        self.zk.start()

        self.logger.info("!!!!! Pre-generating clusterId in ZK. Don't do this !!!!!")
        cluster_id_json = """{"version": "1", "id": "%s"}""" % CLUSTER_ID
        self.zk.create(path="/cluster")
        self.zk.create(path="/cluster/id", value=cluster_id_json)
        self.kafka.start()

        topic_cfg = {
            "topic": "zk-topic",
            "partitions": 10,
            "replication-factor": 3,
            "configs": {"min.insync.replicas": 2}
        }
        #self.kafka.create_topic(topic_cfg)

        self.create_producer()
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()
        self.await_startup()

        # Start up KRaft controller
        remote_quorum = partial(ServiceQuorumInfo, remote_kraft)
        controller = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=DEV_BRANCH,
                                       allow_zk_with_kraft=True,
                                       remote_kafka=self.kafka,
                                       server_prop_overrides=[["zookeeper.connect", self.zk.connect_setting()], ["metadata.migration.enable", "true"]],
                                       quorum_info_provider=remote_quorum)

        # TODO start the remote controller and kafka cluster separately
        #self.controller.start()
        self.logger.info("Letting producer make some data... check offsets data now")
        #time.sleep(120)
        #self.producer.stop()
        #self.consumer.stop()
        self.kafka.stop()

        self.kafka.reconfigure_zk_as_kraft(controller)
        self.logger.info("!!!!! Removing existing meta.properties. Don't do this !!!!!")
        for node in self.kafka.nodes:
            meta1 = os.path.join(self.kafka.DATA_LOG_DIR_1, "meta.properties")
            meta2 = os.path.join(self.kafka.DATA_LOG_DIR_2, "meta.properties")
            node.account.ssh("rm %s" % meta1)
            node.account.ssh("rm %s" % meta2)

        self.kafka.start(clean=False)
        self.logger.info("Now in KRaft mode... check offsets data again")

        #time.sleep(120)

        def check_topics():
            return "zk-topic" in list(self.kafka.list_topics())
        #wait_until(check_topics, 30, err_msg="Did not see 'zk-topic' in KRaft brokers")

        self.run_validation()

        #self.kafka.stop()
        #self.zk.stop()
        #self.controller.stop()
