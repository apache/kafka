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


class ProduceConsumeValidateTest(Test):
    """This class provides a shared template for tests which follow the common pattern of:

        - produce to a topic in the background
        - consume from that topic in the background
        - run some logic, e.g. fail topic leader etc.
        - perform validation
    """

    def __init__(self, test_context):
        super(ProduceConsumeValidateTest, self).__init__(test_context=test_context)

    def setup_producer_and_consumer(self):
        raise NotImplementedError("Subclasses should implement this")

    def start_producer_and_consumer(self):
        # Start background producer and consumer
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=10,
             err_msg="Producer failed to start in a reasonable amount of time.")
        self.consumer.start()
        wait_until(lambda: len(self.consumer.messages_consumed[1]) > 0, timeout_sec=60,
             err_msg="Consumer failed to start in a reasonable amount of time.")

    def stop_producer_and_consumer(self):
        for node in self.consumer.nodes:
            if not self.consumer.alive(node):
                self.logger.warn("Consumer on %s is not alive and probably should be." % str(node.account))
        for node in self.producer.nodes:
            if not self.producer.alive(node):
                self.logger.warn("Producer on %s is not alive and probably should be." % str(node.account))

        # Check that producer is still successfully producing
        currently_acked = self.producer.num_acked
        wait_until(lambda: self.producer.num_acked > currently_acked + 5, timeout_sec=30,
             err_msg="Expected producer to still be producing.")

        self.producer.stop()
        self.consumer.wait()

    def run_produce_consume_validate(self, core_test_action=None):
        """Top-level template for simple produce/consume/validate tests."""

        self.start_producer_and_consumer()

        if core_test_action is not None:
            core_test_action()

        self.stop_producer_and_consumer()
        self.validate()

    def validate(self):
        """Check that each acked message was consumed."""

        self.acked = self.producer.acked
        self.not_acked = self.producer.not_acked

        # Check produced vs consumed
        self.consumed = self.consumer.messages_consumed[1]
        self.logger.info("num consumed:  %d" % len(self.consumed))

        success = True
        msg = ""

        if len(set(self.consumed)) != len(self.consumed):
            # There are duplicates. This is ok, so report it but don't fail the test
            msg += "There are duplicate messages in the log\n"

        if not set(self.consumed).issuperset(set(self.acked)):
            # Every acked message must appear in the logs. I.e. consumed messages must be superset of acked messages.
            acked_minus_consumed = set(self.producer.acked) - set(self.consumed)
            success = False

            msg += "At least one acked message did not appear in the consumed messages. acked_minus_consumed: "
            if len(acked_minus_consumed) < 20:
                msg += str(acked_minus_consumed)
            else:
                for i in range(20):
                    msg += str(acked_minus_consumed.pop()) + ", "
                msg += "...plus " + str(len(acked_minus_consumed) - 20) + " more"

        # collect all logs if validation fails
        if not success:
            for s in self.test_context.services:
                self.mark_for_collect(s)

        assert success, msg

