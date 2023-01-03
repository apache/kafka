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

from functools import partial
import time

from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.config_property import CLUSTER_ID
from kafkatest.services.kafka.quorum import remote_kraft, ServiceQuorumInfo, zk
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import DEV_BRANCH


class TestMigration(ProduceConsumeValidateTest):
    def __init__(self, test_context):
        super(TestMigration, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.partitions = 3
        self.replication_factor = 3

        # Producer and consumer
        self.producer_throughput = 1000
        self.num_producers = 1
        self.num_consumers = 1

    def wait_until_rejoin(self):
        for partition in range(0, self.partitions):
            wait_until(lambda: len(self.kafka.isr_idx_list(self.topic, partition)) == self.replication_factor, timeout_sec=60,
                       backoff_sec=1, err_msg="Replicas did not rejoin the ISR in a reasonable amount of time")

    def do_migration(self):
        # Start up KRaft controller in migration mode
        remote_quorum = partial(ServiceQuorumInfo, remote_kraft)
        controller = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=DEV_BRANCH,
                                  allow_zk_with_kraft=True,
                                  remote_kafka=self.kafka,
                                  server_prop_overrides=[["zookeeper.connect", self.zk.connect_setting()],
                                                         ["zookeeper.metadata.migration.enable", "true"]],
                                  quorum_info_provider=remote_quorum)
        controller.start()

        self.logger.info("Restarting ZK brokers in migration mode")
        self.kafka.reconfigure_zk_for_migration(controller)

        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)
            self.wait_until_rejoin()

        self.logger.info("Restarting ZK brokers as KRaft brokers")
        time.sleep(10)
        self.kafka.reconfigure_zk_as_kraft(controller)

        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)
            self.wait_until_rejoin()

    def test_online_migration(self):
        zk_quorum = partial(ServiceQuorumInfo, zk)
        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=DEV_BRANCH)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  version=DEV_BRANCH,
                                  quorum_info_provider=zk_quorum,
                                  allow_zk_with_kraft=True,
                                  server_prop_overrides=[["zookeeper.metadata.migration.enable", "false"]])
        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = "PLAINTEXT"
        self.zk.start()

        self.logger.info("Pre-generating clusterId for ZK.")
        cluster_id_json = """{"version": "1", "id": "%s"}""" % CLUSTER_ID
        self.zk.create(path="/cluster")
        self.zk.create(path="/cluster/id", value=cluster_id_json)
        self.kafka.start()

        topic_cfg = {
            "topic": self.topic,
            "partitions": self.partitions,
            "replication-factor": self.replication_factor,
            "configs": {"min.insync.replicas": 2}
        }
        self.kafka.create_topic(topic_cfg)

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=["none"],
                                           version=DEV_BRANCH)

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=DEV_BRANCH)

        self.run_produce_consume_validate(core_test_action=self.do_migration)
