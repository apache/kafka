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

from kafkatest.services.kafka import KafkaService, quorum, consumer_group
from kafkatest.services.kafka.quorum import isolated_kraft
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.transactional_message_copier import TransactionalMessageCopier
from kafkatest.utils import is_int
from kafkatest.utils.transactions_utils import create_and_start_copiers
from kafkatest.version import LATEST_3_1, LATEST_3_2, LATEST_3_3, LATEST_3_4, LATEST_3_5, \
    LATEST_3_6, LATEST_3_7, LATEST_3_8, LATEST_3_9, DEV_BRANCH, KafkaVersion, LATEST_STABLE_METADATA_VERSION, LATEST_STABLE_TRANSACTION_VERSION

from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

import time

class TransactionsUpgradeTest(Test):
    """Tests transactions by transactionally copying data from a source topic to
    a destination topic while upgrading the cluster. In the end we verify that the final output
    topic contains exactly one committed copy of each message in the input
    topic.
    """
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(TransactionsUpgradeTest, self).__init__(test_context=test_context)

        self.input_topic = "input-topic"
        self.output_topic = "output-topic"

        self.num_brokers = 3

        self.replication_factor = 3

        # Test parameters
        self.num_input_partitions = 1
        self.num_output_partitions = 1
        self.num_seed_messages = 4500
        self.transaction_size = 5

        # The transaction timeout should be lower than the progress timeout, but at
        # least as high as the request timeout (which is 30s by default).
        self.transaction_timeout = 40000
        self.progress_timeout_sec = 60
        self.consumer_group = "transactions-test-consumer-group"

    def seed_messages(self, topic, num_seed_messages):
        seed_timeout_sec = 10000
        seed_producer = VerifiableProducer(context=self.test_context,
                                           num_nodes=1,
                                           kafka=self.kafka,
                                           topic=topic,
                                           message_validator=is_int,
                                           max_messages=num_seed_messages,
                                           enable_idempotence=True)
        seed_producer.start()
        wait_until(lambda: seed_producer.num_acked >= num_seed_messages,
                   timeout_sec=seed_timeout_sec,
                   err_msg="Producer failed to produce messages %d in %ds." %\
                   (self.num_seed_messages, seed_timeout_sec))
        return seed_producer.acked

    def get_messages_from_topic(self, topic, num_messages, group_protocol):
        consumer = self.start_consumer(topic, group_id="verifying_consumer", group_protocol=group_protocol)
        return self.drain_consumer(consumer, num_messages)

    def start_consumer(self, topic_to_read, group_id, group_protocol):
        consumer = ConsoleConsumer(context=self.test_context,
                                   num_nodes=1,
                                   kafka=self.kafka,
                                   topic=topic_to_read,
                                   group_id=group_id,
                                   message_validator=is_int,
                                   from_beginning=True,
                                   isolation_level="read_committed",
                                   consumer_properties=consumer_group.maybe_set_group_protocol(group_protocol))
        consumer.start()
        # ensure that the consumer is up.
        wait_until(lambda: (len(consumer.messages_consumed[1]) > 0) == True,
                   timeout_sec=60,
                   err_msg="Consumer failed to consume any messages for %ds" %\
                   60)
        return consumer

    def drain_consumer(self, consumer, num_messages):
        # wait until we read at least the expected number of messages.
        # This is a safe check because both failure modes will be caught:
        #  1. If we have 'num_seed_messages' but there are duplicates, then
        #     this is checked for later.
        #
        #  2. If we never reach 'num_seed_messages', then this will cause the
        #     test to fail.
        wait_until(lambda: len(consumer.messages_consumed[1]) >= num_messages,
                   timeout_sec=90,
                   err_msg="Consumer consumed only %d out of %d messages in %ds" %\
                   (len(consumer.messages_consumed[1]), num_messages, 90))
        consumer.stop()
        return consumer.messages_consumed[1]

    def wait_until_rejoin(self):
        for partition in range(0, self.num_input_partitions):
            wait_until(lambda: len(self.kafka.isr_idx_list(self.input_topic, partition)) == self.replication_factor, timeout_sec=60,
                    backoff_sec=1, err_msg="Replicas did not rejoin the ISR in a reasonable amount of time")

        for partition in range(0, self.num_output_partitions):
            wait_until(lambda: len(self.kafka.isr_idx_list(self.output_topic, partition)) == self.replication_factor, timeout_sec=60,
                    backoff_sec=1, err_msg="Replicas did not rejoin the ISR in a reasonable amount of time")

    def perform_upgrade(self, from_kafka_version):
        self.logger.info("Performing rolling upgrade.")
        for node in self.kafka.controller_quorum.nodes:
            self.logger.info("Stopping controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.stop_node(node)
            node.version = DEV_BRANCH
            self.logger.info("Restarting controller node %s" % node.account.hostname)
            self.kafka.controller_quorum.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted controller node %s" % node.account.hostname)
        for node in self.kafka.nodes:
            self.logger.info("Stopping broker node %s" % node.account.hostname)
            self.kafka.stop_node(node)
            node.version = DEV_BRANCH
            self.logger.info("Restarting broker node %s" % node.account.hostname)
            self.kafka.start_node(node)
            self.wait_until_rejoin()
            self.logger.info("Successfully restarted broker node %s" % node.account.hostname)
        self.logger.info("Changing metadata.version to %s" % LATEST_STABLE_METADATA_VERSION)
        self.kafka.upgrade_metadata_version(LATEST_STABLE_METADATA_VERSION)
        self.logger.info("Changing transaction.version to %s" % LATEST_STABLE_TRANSACTION_VERSION)
        self.kafka.run_features_command("upgrade", "transaction.version", LATEST_STABLE_TRANSACTION_VERSION)
        # Restart brokers to ensure new api versions sent
        for node in self.kafka.nodes:
            self.logger.info("Stopping broker node %s" % node.account.hostname)
            self.kafka.stop_node(node)
            self.logger.info("Restarting broker node %s" % node.account.hostname)
            self.kafka.start_node(node)
            self.wait_until_rejoin()

    def copy_messages_transactionally_during_upgrade(self, input_topic, output_topic,
                                                     num_copiers, num_messages_to_copy,
                                                     use_group_metadata, group_protocol,
                                                     from_kafka_version):
        """Copies messages transactionally from the seeded input topic to the
        output topic while an rolling upgrade occurs.

        This method also consumes messages in read_committed mode from the
        output topic.

        It returns the concurrently consumed messages.
        """
        copiers = create_and_start_copiers(test_context=self.test_context,
                                           kafka=self.kafka,
                                           consumer_group=self.consumer_group,
                                           input_topic=input_topic,
                                           output_topic=output_topic,
                                           transaction_size=self.transaction_size,
                                           transaction_timeout=self.transaction_timeout,
                                           num_copiers=num_copiers,
                                           use_group_metadata=use_group_metadata)
        concurrent_consumer = self.start_consumer(output_topic,
                                                  group_id="concurrent_consumer",
                                                  group_protocol=group_protocol)

        self.perform_upgrade(from_kafka_version)

        copier_timeout_sec = 180
        for copier in copiers:
            wait_until(lambda: copier.is_done,
                       timeout_sec=copier_timeout_sec,
                       err_msg="%s - Failed to copy all messages in  %ds." %\
                       (copier.transactional_id, copier_timeout_sec))
        self.logger.info("finished copying messages")

        return self.drain_consumer(concurrent_consumer, num_messages_to_copy)

    def setup_topics(self):
        self.kafka.topics = {
            self.input_topic: {
                "partitions": self.num_input_partitions,
                "replication-factor": self.replication_factor,
                "configs": {
                    "min.insync.replicas": 2
                }
            },
            self.output_topic: {
                "partitions": self.num_output_partitions,
                "replication-factor": self.replication_factor,
                "configs": {
                    "min.insync.replicas": 2
                }
            }
        }

    @cluster(num_nodes=8)
    @matrix(
        from_kafka_version=[str(LATEST_3_9), str(LATEST_3_8), str(LATEST_3_7), str(LATEST_3_6), str(LATEST_3_5), str(LATEST_3_4), str(LATEST_3_3), str(LATEST_3_2), str(LATEST_3_1)],
        metadata_quorum=[isolated_kraft],
        use_new_coordinator=[False],
        group_protocol=[None]
    )
    def test_transactions_upgrade(self, from_kafka_version, metadata_quorum=quorum.isolated_kraft, use_new_coordinator=False, group_protocol=None):
        fromKafkaVersion = KafkaVersion(from_kafka_version)
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=self.num_brokers,
                                  zk=None,
                                  version=fromKafkaVersion,
                                  controller_num_nodes_override=1)

        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

        self.setup_topics()
        self.kafka.start()

        input_messages = self.seed_messages(self.input_topic, self.num_seed_messages)
        concurrently_consumed_messages = self.copy_messages_transactionally_during_upgrade(
            input_topic=self.input_topic, output_topic=self.output_topic, num_copiers=self.num_input_partitions,
            num_messages_to_copy=self.num_seed_messages, use_group_metadata=True, group_protocol=group_protocol,
            from_kafka_version=from_kafka_version)
        output_messages = self.get_messages_from_topic(self.output_topic, self.num_seed_messages, group_protocol)

        concurrently_consumed_message_set = set(concurrently_consumed_messages)
        output_message_set = set(output_messages)
        input_message_set = set(input_messages)

        num_dups = abs(len(output_messages) - len(output_message_set))
        num_dups_in_concurrent_consumer = abs(len(concurrently_consumed_messages)
                                              - len(concurrently_consumed_message_set))
        assert num_dups == 0, "Detected %d duplicates in the output stream" % num_dups
        assert input_message_set == output_message_set, "Input and output message sets are not equal. Num input messages %d. Num output messages %d" %\
            (len(input_message_set), len(output_message_set))

        assert num_dups_in_concurrent_consumer == 0, "Detected %d dups in concurrently consumed messages" % num_dups_in_concurrent_consumer
        assert input_message_set == concurrently_consumed_message_set, \
            "Input and concurrently consumed output message sets are not equal. Num input messages: %d. Num concurrently_consumed_messages: %d" %\
            (len(input_message_set), len(concurrently_consumed_message_set))
