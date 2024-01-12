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
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.errors import TimeoutError

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.config_property import CLUSTER_ID, LOG_DIRS
from kafkatest.services.kafka.quorum import isolated_kraft, ServiceQuorumInfo, zk
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import DEV_BRANCH, LATEST_3_4


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

    def do_migration(self, roll_controller = False, downgrade_to_zk = False):
        # Start up KRaft controller in migration mode
        remote_quorum = partial(ServiceQuorumInfo, isolated_kraft)
        controller = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=DEV_BRANCH,
                                  allow_zk_with_kraft=True,
                                  isolated_kafka=self.kafka,
                                  server_prop_overrides=[["zookeeper.connect", self.zk.connect_setting()],
                                                         ["zookeeper.metadata.migration.enable", "true"],
                                                         [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]],
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

        if roll_controller:
            self.logger.info("Restarting KRaft quorum")
            for node in controller.nodes:
                controller.stop_node(node)
                controller.start_node(node)

    @cluster(num_nodes=7)
    @parametrize(roll_controller = True)
    @parametrize(roll_controller = False)
    def test_online_migration(self, roll_controller):
        zk_quorum = partial(ServiceQuorumInfo, zk)
        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=DEV_BRANCH)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  version=DEV_BRANCH,
                                  quorum_info_provider=zk_quorum,
                                  allow_zk_with_kraft=True,
                                  server_prop_overrides=[
                                      ["zookeeper.metadata.migration.enable", "false"],
                                      [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]
                                  ])
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

        self.run_produce_consume_validate(core_test_action=partial(self.do_migration, roll_controller = roll_controller))
        self.kafka.stop()

    @cluster(num_nodes=7)
    @parametrize(metadata_quorum=isolated_kraft)
    def test_pre_migration_mode_3_4(self, metadata_quorum):
        """
        Start a KRaft quorum in 3.4 without migrations enabled. Since we were not correctly writing
        ZkMigrationStateRecord in 3.4, there will be no ZK migration state in the log.

        When upgrading to 3.5+, the controller should see that there are records in the log and
        automatically bootstrap a ZkMigrationStateRecord(NONE) into the log (indicating that this
        cluster was created in KRaft mode).

        This test ensures that even if we enable migrations after the upgrade to 3.5, that no migration
        is able to take place.
        """
        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=LATEST_3_4)
        self.zk.start()

        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  allow_zk_with_kraft=True,
                                  version=LATEST_3_4,
                                  server_prop_overrides=[
                                      ["zookeeper.metadata.migration.enable", "false"],
                                      [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]
                                  ],
                                  topics={self.topic: {"partitions": self.partitions,
                                                       "replication-factor": self.replication_factor,
                                                       'configs': {"min.insync.replicas": 2}}})
        self.kafka.start()

        # Now reconfigure the cluster as if we're trying to do a migration
        self.kafka.server_prop_overrides.clear()
        self.kafka.server_prop_overrides.extend([
            ["zookeeper.metadata.migration.enable", "true"]
        ])

        self.logger.info("Performing rolling upgrade.")
        for node in self.kafka.controller_quorum.nodes:
            self.logger.info("Stopping controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.stop_node(node)
            node.version = DEV_BRANCH
            self.logger.info("Restarting controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.start_node(node)
            # Controller should crash

        # Check the controller's logs for the error message about the migration state
        saw_expected_error = False
        self.logger.info("Waiting for controller to crash")

        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=False)

        for node in self.kafka.controller_quorum.nodes:
            self.kafka.controller_quorum.stop_node(node, clean_shutdown=False)

        for node in self.kafka.controller_quorum.nodes:
            with node.account.monitor_log(KafkaService.STDOUT_STDERR_CAPTURE) as monitor:
                monitor.offset = 0
                try:
                    # Shouldn't have to wait too long to see this log message after startup
                    monitor.wait_until(
                        "Should not have ZK migrations enabled on a cluster that was created in KRaft mode.",
                        timeout_sec=10.0, backoff_sec=.25,
                        err_msg=""
                    )
                    saw_expected_error = True
                    break
                except TimeoutError:
                    continue

        assert saw_expected_error, "Did not see expected ERROR log in the controller logs"

    @cluster(num_nodes=5)
    def test_upgrade_after_3_4_migration(self):
        """
        Perform a migration on version 3.4.0. Then do a rolling upgrade to 3.5+ and ensure we see
        the correct migration state in the log.
        """
        zk_quorum = partial(ServiceQuorumInfo, zk)
        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=LATEST_3_4)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  version=LATEST_3_4,
                                  quorum_info_provider=zk_quorum,
                                  allow_zk_with_kraft=True,
                                  server_prop_overrides=[
                                      ["zookeeper.metadata.migration.enable", "true"],
                                      [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]
                                  ])

        remote_quorum = partial(ServiceQuorumInfo, isolated_kraft)
        controller = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=LATEST_3_4,
                                  allow_zk_with_kraft=True,
                                  isolated_kafka=self.kafka,
                                  server_prop_overrides=[["zookeeper.connect", self.zk.connect_setting()],
                                                         ["zookeeper.metadata.migration.enable", "true"],
                                                         [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]],
                                  quorum_info_provider=remote_quorum)

        self.kafka.security_protocol = "PLAINTEXT"
        self.kafka.interbroker_security_protocol = "PLAINTEXT"
        self.zk.start()

        controller.start()

        self.logger.info("Pre-generating clusterId for ZK.")
        cluster_id_json = """{"version": "1", "id": "%s"}""" % CLUSTER_ID
        self.zk.create(path="/cluster")
        self.zk.create(path="/cluster/id", value=cluster_id_json)
        self.kafka.reconfigure_zk_for_migration(controller)
        self.kafka.start()

        topic_cfg = {
            "topic": self.topic,
            "partitions": self.partitions,
            "replication-factor": self.replication_factor,
            "configs": {"min.insync.replicas": 2}
        }
        self.kafka.create_topic(topic_cfg)

        # Now we're in dual-write mode. The 3.4 controller will have written a PRE_MIGRATION record (1) into the log.
        # We now upgrade the controller to 3.5+ where 1 is redefined as MIGRATION.
        for node in controller.nodes:
            self.logger.info("Stopping controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.stop_node(node)
            node.version = DEV_BRANCH
            self.logger.info("Restarting controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted controller node %s" % node.account.hostname)

        # Check the controller's logs for the INFO message that we're still in the migration state
        saw_expected_log = False
        for node in self.kafka.controller_quorum.nodes:
            with node.account.monitor_log(KafkaService.STDOUT_STDERR_CAPTURE) as monitor:
                monitor.offset = 0
                try:
                    # Shouldn't have to wait too long to see this log message after startup
                    monitor.wait_until(
                        "Staying in ZK migration",
                        timeout_sec=10.0, backoff_sec=.25,
                        err_msg=""
                    )
                    saw_expected_log = True
                    break
                except TimeoutError:
                    continue

        assert saw_expected_log, "Did not see expected INFO log after upgrading from a 3.4 migration"
        self.kafka.stop()

    @cluster(num_nodes=5)
    def test_reconcile_kraft_to_zk(self):
        """
        Perform a migration and delete a topic directly from ZK. Ensure that the topic is added back
        by KRaft during a failover. This exercises the snapshot reconciliation.
        """
        zk_quorum = partial(ServiceQuorumInfo, zk)
        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=DEV_BRANCH)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=3,
                                  zk=self.zk,
                                  version=DEV_BRANCH,
                                  quorum_info_provider=zk_quorum,
                                  allow_zk_with_kraft=True,
                                  server_prop_overrides=[
                                      ["zookeeper.metadata.migration.enable", "false"],
                                      [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]])

        remote_quorum = partial(ServiceQuorumInfo, isolated_kraft)
        controller = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=DEV_BRANCH,
                                  allow_zk_with_kraft=True,
                                  isolated_kafka=self.kafka,
                                  server_prop_overrides=[["zookeeper.connect", self.zk.connect_setting()],
                                                         ["zookeeper.metadata.migration.enable", "true"],
                                                         [LOG_DIRS, KafkaService.DATA_LOG_DIR_1]],
                                  quorum_info_provider=remote_quorum)

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

        # Create topics in ZK mode
        for i in range(10):
            topic_cfg = {
                "topic": f"zk-topic-{i}",
                "partitions": self.partitions,
                "replication-factor": self.replication_factor,
                "configs": {"min.insync.replicas": 2}
            }
            self.kafka.create_topic(topic_cfg)

        controller.start()
        self.kafka.reconfigure_zk_for_migration(controller)
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)
            self.wait_until_rejoin()

        # Check the controller's logs for the INFO message that we're done with migration
        saw_expected_log = False
        for node in self.kafka.controller_quorum.nodes:
            with node.account.monitor_log(KafkaService.STDOUT_STDERR_CAPTURE) as monitor:
                monitor.offset = 0
                try:
                    # Shouldn't have to wait too long to see this log message after startup
                    monitor.wait_until(
                        "Finished initial migration of ZK metadata to KRaft",
                        timeout_sec=10.0, backoff_sec=.25,
                        err_msg=""
                    )
                    saw_expected_log = True
                    break
                except TimeoutError:
                    continue

        assert saw_expected_log, "Did not see expected INFO log after migration"

        # Manually delete a topic from ZK to simulate a missed dual-write
        self.zk.delete(path="/brokers/topics/zk-topic-0", recursive=True)

        # Roll the controller nodes to force a failover, this causes a snapshot reconciliation
        for node in controller.nodes:
            controller.stop_node(node)
            controller.start_node(node)

        def topic_in_zk():
            topics_in_zk = self.zk.get_children(path="/brokers/topics")
            return "zk-topic-0" in topics_in_zk

        wait_until(topic_in_zk, timeout_sec=60,
            backoff_sec=1, err_msg="Topic did not appear in ZK in time.")

        self.kafka.stop()
        controller.stop()
        self.zk.stop()
