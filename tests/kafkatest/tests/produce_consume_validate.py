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
            raise e

    @staticmethod
    def annotate_missing_msgs(missing, acked, consumed, msg):
        msg += "%s acked message did not make it to the Consumer. They are: " % len(missing)
        if len(missing) < 20:
            msg += str(missing) + ". "
        else:
            for i in range(20):
                msg += str(missing.pop()) + ", "
            msg += "...plus %s more. Total Acked: %s, Total Consumed: %s. " \
                   % (len(missing) - 20, len(set(acked)), len(set(consumed)))
        return msg

    @staticmethod
    def annotate_data_lost(data_lost, msg, number_validated):
        print_limit = 10
        if len(data_lost) > 0:
            msg += "The first %s missing messages were validated to ensure they are in Kafka's data files. " \
                   "%s were missing. This suggests data loss. Here are some of the messages not found in the data files: %s\n" \
                   % (number_validated, len(data_lost), str(data_lost[0:print_limit]) if len(data_lost) > print_limit else str(data_lost))
        else:
            msg += "We validated that the first %s of these missing messages correctly made it into Kafka's data files. " \
                   "This suggests they were lost on their way to the consumer." % number_validated
        return msg

    def validate(self):
        """Check that each acked message was consumed."""
        success = True
        msg = ""
        acked = self.producer.acked
        consumed = self.consumer.messages_consumed[1]
        missing = set(acked) - set(consumed)

        self.logger.info("num consumed:  %d" % len(consumed))

        # Were all acked messages consumed?
        if len(missing) > 0:
            msg = self.annotate_missing_msgs(missing, acked, consumed, msg)
            success = False

            #Did we miss anything due to data loss?
            to_validate = list(missing)[0:1000 if len(missing) > 1000 else len(missing)]
            data_lost = self.kafka.search_data_files(self.topic, to_validate)
            msg = self.annotate_data_lost(data_lost, msg, len(to_validate))


        # Are there duplicates?
        if len(set(consumed)) != len(consumed):
            msg += "(There are also %s duplicate messages in the log - but that is an acceptable outcome)\n" % abs(len(set(consumed)) - len(consumed))

        # Collect all logs if validation fails
        if not success:
            for s in self.test_context.services:
                self.mark_for_collect(s)

        assert success, msg
