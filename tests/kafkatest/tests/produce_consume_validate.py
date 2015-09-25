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
    def __init__(self, test_context):
        super(ProduceConsumeValidateTest, self).__init__(test_context=test_context)

    def setup_producer_and_consumer(self):
        raise NotImplementedError("Subclasses should implement this")

    def start_producer_and_consumer(self):
        # Start background producer and consumer
        self.producer.start()
        wait_until(lambda: self.producer.num_acked > 5, timeout_sec=5,
             err_msg="Producer failed to start in a reasonable amount of time.")
        self.consumer.start()

    def stop_producer_and_consumer(self):
        for node in self.consumer.nodes:
            assert self.consumer.alive(node)
        self.producer.stop()
        self.consumer.wait()

    def run_default_test_template(self, core_test_action):
        """Top-level template for simple produce/consume/validate tests."""

        self.start_producer_and_consumer()
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
            msg += "At least one acked message did not appear in the consumed messages. acked_minus_consumed: " + str(acked_minus_consumed)

        if not success:
            # Collect all the data logs if there was a failure
            self.mark_for_collect(self.kafka)

        if not success:
            self.mark_for_collect(self.producer)

        assert success, msg

