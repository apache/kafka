from functools import partial

from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka.config_property import CLUSTER_ID
from kafkatest.services.kafka.quorum import remote_kraft, ServiceQuorumInfo, zk
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.version import DEV_BRANCH


class TestMigration(EndToEndTest):
    def __init__(self, test_context):
        super(TestMigration, self).__init__(topic="zk-topic", test_context=test_context)

    def test_offline_migration(self):
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

        # Start up KRaft controller in migration mode
        remote_quorum = partial(ServiceQuorumInfo, remote_kraft)
        controller = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=DEV_BRANCH,
                                  allow_zk_with_kraft=True,
                                  remote_kafka=self.kafka,
                                  server_prop_overrides=[["zookeeper.connect", self.zk.connect_setting()],
                                                         ["zookeeper.metadata.migration.enable", "true"]],
                                  quorum_info_provider=remote_quorum)
        controller.start()

        self.create_producer()
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()

        self.logger.info("Restarting ZK brokers in migration mode")
        self.kafka.stop()
        self.kafka.reconfigure_zk_for_migration(controller)
        self.kafka.start()

        self.await_startup()
        self.run_validation()
        # self.kafka.reconfigure_zk_as_kraft(controller)
