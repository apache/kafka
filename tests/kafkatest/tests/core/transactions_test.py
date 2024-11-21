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
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int
from kafkatest.utils.transactions_utils import create_and_start_copiers

from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

import time

class TransactionsTest(Test):
    """Tests transactions by transactionally copying data from a source topic to
    a destination topic and killing the copy process as well as the broker
    randomly through the process. In the end we verify that the final output
    topic contains exactly one committed copy of each message in the input
    topic.
    """
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(TransactionsTest, self).__init__(test_context=test_context)

        self.input_topic = "input-topic"
        self.output_topic = "output-topic"

        self.num_brokers = 3

        # Test parameters
        self.num_input_partitions = 2
        self.num_output_partitions = 3
        self.num_seed_messages = 100000
        self.transaction_size = 750

        # The transaction timeout should be lower than the progress timeout, but at
        # least as high as the request timeout (which is 30s by default). When the
        # client is hard-bounced, progress may depend on the previous transaction
        # being aborted. When the broker is hard-bounced, we may have to wait as
        # long as the request timeout to get a `Produce` response and we do not
        # want the coordinator timing out the transaction.
        self.transaction_timeout = 40000
        self.progress_timeout_sec = 60
        self.consumer_group = "transactions-test-consumer-group"

        self.kafka = KafkaService(test_context,
                                  num_nodes=self.num_brokers,
                                  zk=None,
                                  controller_num_nodes_override=1)

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

    def bounce_brokers(self, clean_shutdown):
       for node in self.kafka.nodes:
            if clean_shutdown:
                self.kafka.restart_node(node, clean_shutdown = True)
            else:
                self.kafka.stop_node(node, clean_shutdown = False)
                gracePeriodSecs = 5
                brokerSessionTimeoutSecs = 18
                wait_until(lambda: not self.kafka.pids(node),
                           timeout_sec=brokerSessionTimeoutSecs + gracePeriodSecs,
                           err_msg="Failed to see timely disappearance of process for hard-killed broker %s" % str(node.account))
                time.sleep(brokerSessionTimeoutSecs + gracePeriodSecs)
                self.kafka.start_node(node)

            self.kafka.await_no_under_replicated_partitions()

    def bounce_copiers(self, copiers, clean_shutdown):
        for _ in range(3):
            for copier in copiers:
                wait_until(lambda: copier.progress_percent() >= 20.0,
                           timeout_sec=self.progress_timeout_sec,
                           err_msg="%s : Message copier didn't make enough progress in %ds. Current progress: %s" \
                           % (copier.transactional_id, self.progress_timeout_sec, str(copier.progress_percent())))
                self.logger.info("%s - progress: %s" % (copier.transactional_id,
                                                        str(copier.progress_percent())))
                copier.restart(clean_shutdown)

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

    def copy_messages_transactionally(self, failure_mode, bounce_target,
                                      input_topic, output_topic,
                                      num_copiers, num_messages_to_copy,
                                      use_group_metadata, group_protocol):
        """Copies messages transactionally from the seeded input topic to the
        output topic, either bouncing brokers or clients in a hard and soft
        way as it goes.

        This method also consumes messages in read_committed mode from the
        output topic while the bounces and copy is going on.

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
        clean_shutdown = False
        if failure_mode == "clean_bounce":
            clean_shutdown = True

        if bounce_target == "brokers":
            self.bounce_brokers(clean_shutdown)
        elif bounce_target == "clients":
            self.bounce_copiers(copiers, clean_shutdown)

        copier_timeout_sec = 120
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
                "replication-factor": 3,
                "configs": {
                    "min.insync.replicas": 2
                }
            },
            self.output_topic: {
                "partitions": self.num_output_partitions,
                "replication-factor": 3,
                "configs": {
                    "min.insync.replicas": 2
                }
            }
        }

    @cluster(num_nodes=9)
    @matrix(
        failure_mode=["hard_bounce", "clean_bounce"],
        bounce_target=["brokers", "clients"],
        check_order=[True, False],
        use_group_metadata=[True, False],
        metadata_quorum=quorum.all_kraft,
        use_new_coordinator=[False]
    )
    @matrix(
        failure_mode=["hard_bounce", "clean_bounce"],
        bounce_target=["brokers", "clients"],
        check_order=[True, False],
        use_group_metadata=[True, False],
        metadata_quorum=quorum.all_kraft,
        use_new_coordinator=[True],
        group_protocol=consumer_group.all_group_protocols
    )
    def test_transactions(self, failure_mode, bounce_target, check_order, use_group_metadata, metadata_quorum, use_new_coordinator=False, group_protocol=None):
        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True
        if check_order:
            # To check ordering, we simply create input and output topics
            # with a single partition.
            # We reduce the number of seed messages to copy to account for the fewer output
            # partitions, and thus lower parallelism. This helps keep the test
            # time shorter.
            self.num_seed_messages = self.num_seed_messages // 3
            self.num_input_partitions = 1
            self.num_output_partitions = 1

        self.setup_topics()
        self.kafka.start()

        input_messages = self.seed_messages(self.input_topic, self.num_seed_messages)
        concurrently_consumed_messages = self.copy_messages_transactionally(
            failure_mode, bounce_target, input_topic=self.input_topic,
            output_topic=self.output_topic, num_copiers=self.num_input_partitions,
            num_messages_to_copy=self.num_seed_messages, use_group_metadata=use_group_metadata, group_protocol=group_protocol)
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
        if check_order:
            assert input_messages == sorted(input_messages), "The seed messages themselves were not in order"
            assert output_messages == input_messages, "Output messages are not in order"
            assert concurrently_consumed_messages == output_messages, "Concurrently consumed messages are not in order"
