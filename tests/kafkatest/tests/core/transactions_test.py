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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.transactional_message_copier import TransactionalMessageCopier
from kafkatest.utils import is_int

from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until


class TransactionsTest(Test):
    """Tests transactions by transactionally copying data from a source topic to
    a destination topic and killing the copy process as well as the broker
    randomly through the process. In the end we verify that the final output
    topic contains exactly one committed copy of each message in the input
    topic
    """
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(TransactionsTest, self).__init__(test_context=test_context)

        self.input_topic = "input-topic"
        self.output_topic = "output-topic"

        self.num_brokers = 3

        # Test parameters
        self.num_input_partitions = 1
        self.num_output_partitions = 3
        self.num_seed_messages = 5000
        self.transaction_size = 500
        self.first_transactional_id = "my-first-transactional-id"
        self.second_transactional_id = "my-second-transactional-id"
        self.consumer_group = "transactions-test-consumer-group"

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=self.num_brokers,
                                  zk=self.zk,
                                  topics = {
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
                                  })

    def setUp(self):
        self.zk.start()

    def seed_messages(self):
        seed_timeout_sec = 10000
        seed_producer = VerifiableProducer(context=self.test_context,
                                           num_nodes=1,
                                           kafka=self.kafka,
                                           topic=self.input_topic,
                                           message_validator=is_int,
                                           max_messages=self.num_seed_messages,
                                           enable_idempotence=True)

        seed_producer.start()
        wait_until(lambda: seed_producer.num_acked >= self.num_seed_messages,
                   timeout_sec=seed_timeout_sec,
                   err_msg="Producer failed to produce messages %d in  %ds." %\
                   (self.num_seed_messages, seed_timeout_sec))
        seed_producer.stop()
        return seed_producer.acked

    def get_messages_from_output_topic(self):
        consumer = ConsoleConsumer(context=self.test_context,
                                   num_nodes=1,
                                   kafka=self.kafka,
                                   topic=self.output_topic,
                                   new_consumer=True,
                                   message_validator=is_int,
                                   from_beginning=True,
                                   consumer_timeout_ms=5000,
                                   isolation_level="read_committed")
        consumer.start()
        # ensure that the consumer is up.
        wait_until(lambda: consumer.alive(consumer.nodes[0]) == True,
                   timeout_sec=60,
                   err_msg="Consumer failed to start for %ds" %\
                   60)
        # wait until the consumer closes, which will be 5 seconds after
        # receiving the last message.
        wait_until(lambda: consumer.alive(consumer.nodes[0]) == False,
                   timeout_sec=60,
                   err_msg="Consumer failed to consume %d messages in %ds" %\
                   (self.num_seed_messages, 60))
        return consumer.messages_consumed[1]

    def bounce_brokers(self, clean_shutdown):
       for node in self.kafka.nodes:
            if clean_shutdown:
                self.kafka.restart_node(node, clean_shutdown = True)
            else:
                self.kafka.stop_node(node, clean_shutdown = False)
                wait_until(lambda: len(self.kafka.pids(node)) == 0 and not self.kafka.is_registered(node),
                           timeout_sec=self.kafka.zk_session_timeout + 5,
                           err_msg="Failed to see timely deregistration of \
                           hard-killed broker %s" % str(node.account))

    def copy_messages_transactionally(self, failure_mode, bounce_target):
        message_copier = TransactionalMessageCopier(
            context=self.test_context,
            num_nodes=1,
            kafka=self.kafka,
            transactional_id=self.first_transactional_id,
            consumer_group=self.consumer_group,
            input_topic=self.input_topic,
            input_partition=0,
            output_topic=self.output_topic,
            max_messages=self.num_seed_messages,
            transaction_size=self.transaction_size
        )
        message_copier.start()
        clean_shutdown = False
        if failure_mode == "clean_bounce":
            clean_shutdown = True

        if bounce_target == "brokers":
            self.bounce_brokers(clean_shutdown)
        elif bounce_target == "clients":
            for _ in range(3):
                message_copier.restart(clean_shutdown)

        wait_until(lambda: message_copier.is_done,
                   timeout_sec=20,
                   err_msg="Failed to copy %d messages in  %ds." %\
                   (self.num_seed_messages, 20))


    @cluster(num_nodes=8)
    @matrix(failure_mode=["clean_bounce", "hard_bounce"],
            bounce_target=["clients", "brokers"])
    def test_transactions(self, failure_mode, bounce_target):
        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.start()
        input_messages = self.seed_messages()
        self.copy_messages_transactionally(failure_mode, bounce_target)
        output_messages = self.get_messages_from_output_topic()
        output_message_set = set(output_messages)
        input_message_set = set(input_messages)
        num_dups = abs(len(output_messages) - len(output_message_set))
        assert num_dups == 0, "Detected %d duplicates in the output stream" % num_dups
        assert input_message_set == output_message_set, "Input and output message sets are not equal. Num input messages %d. Num output messages %d" %\
            (len(input_message_set), len(output_message_set))

