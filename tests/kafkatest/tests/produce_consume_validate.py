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

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.utils import validate_delivery

import time

class ProduceConsumeValidateTest(Test):
    """This class provides a shared template for tests which follow the common pattern of:

        - produce to a topic in the background
        - consume from that topic in the background
        - run some logic, e.g. fail topic leader etc.
        - perform validation
    """

    def __init__(self, test_context):
        super(ProduceConsumeValidateTest, self).__init__(test_context=test_context)
        # How long to wait for the producer to declare itself healthy? This can
        # be overidden by inheriting classes.
        self.producer_start_timeout_sec = 20

        # How long to wait for the consumer to start consuming messages?
        self.consumer_start_timeout_sec = 60

        # How long wait for the consumer process to fork? This
        # is important in the case when the consumer is starting from the end,
        # and we don't want it to miss any messages. The race condition this
        # timeout avoids is that the consumer has not forked even after the
        # producer begins producing messages, in which case we will miss the
        # initial set of messages and get spurious test failures.
        self.consumer_init_timeout_sec = 0
        self.enable_idempotence = False

    def start_producer_and_consumer(self):
        # Start background producer and consumer
        self.consumer.start()
        if (self.consumer_init_timeout_sec > 0):
            self.logger.debug("Waiting %ds for the consumer to initialize.",
                              self.consumer_init_timeout_sec)
            start = int(time.time())
            wait_until(lambda: self.consumer.alive(self.consumer.nodes[0]) is True,
                       timeout_sec=self.consumer_init_timeout_sec,
                       err_msg="Consumer process took more than %d s to fork" %\
                       self.consumer_init_timeout_sec)
            end = int(time.time())
            remaining_time = self.consumer_init_timeout_sec - (end - start)
            if remaining_time < 0 :
                remaining_time = 0
            if self.consumer.new_consumer:
                wait_until(lambda: self.consumer.has_partitions_assigned(self.consumer.nodes[0]) is True,
                           timeout_sec=remaining_time,
                           err_msg="Consumer process took more than %d s to have partitions assigned" %\
                           remaining_time)

        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5,
                   timeout_sec=self.producer_start_timeout_sec,
                   err_msg="Producer failed to produce messages for %ds." %\
                   self.producer_start_timeout_sec)
        wait_until(lambda: len(self.consumer.messages_consumed[1]) > 0,
                   timeout_sec=self.consumer_start_timeout_sec,
                   err_msg="Consumer failed to consume messages for %ds." %\
                   self.consumer_start_timeout_sec)

    def check_alive(self):
        msg = ""
        for node in self.consumer.nodes:
            if not self.consumer.alive(node):
                msg = "The consumer has terminated, or timed out, on node %s." % str(node.account)
        for node in self.producer.nodes:
            if not self.producer.alive(node):
                msg += "The producer has terminated, or timed out, on node %s." % str(node.account)
        if len(msg) > 0:
            raise Exception(msg)

    def check_producing(self):
        currently_acked = self.producer.num_acked
        wait_until(lambda: self.producer.num_acked > currently_acked + 5, timeout_sec=30,
                   err_msg="Expected producer to still be producing.")

    def stop_producer_and_consumer(self):
        self.check_alive()
        self.check_producing()
        self.producer.stop()
        self.consumer.wait()

    def run_produce_consume_validate(self, core_test_action=None, *args):
        """Top-level template for simple produce/consume/validate tests."""
        try:
            self.start_producer_and_consumer()

            if core_test_action is not None:
                core_test_action(*args)

            self.stop_producer_and_consumer()
            self.validate()
        except BaseException as e:
            for s in self.test_context.services:
                self.mark_for_collect(s)
            raise

    def validate(self):
        messages_consumed = self.consumer.messages_consumed[1]

        self.logger.info("Number of acked records: %d" % len(self.producer.acked))
        self.logger.info("Number of consumed records: %d" % len(messages_consumed))

        def check_lost_data(missing_records):
            return self.kafka.search_data_files(self.topic, missing_records)

        succeeded, error_msg = validate_delivery(self.producer.acked, messages_consumed,
                                                 self.enable_idempotence, check_lost_data)

        # Collect all logs if validation fails
        if not succeeded:
            for s in self.test_context.services:
                self.mark_for_collect(s)

        assert succeeded, error_msg
