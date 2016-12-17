# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from ducktape.errors import TimeoutError
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_0_9, LATEST_0_8_2, TRUNK, LATEST_0_10_0, KafkaVersion

# Tests to check api versions check is performed correctly.
class ApiVersionsCheckTest(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(ApiVersionsCheckTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
            
        self.zk.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1
        self.messages_per_producer = 1000

    @parametrize(broker_version=str(TRUNK), should_fail=False)
    @parametrize(broker_version=str(LATEST_0_10_0), should_fail=True)
    @parametrize(broker_version=str(LATEST_0_9), should_fail=True)
    @parametrize(broker_version=str(LATEST_0_8_2), should_fail=True)
    def test_api_versions_check(self, broker_version, should_fail):

        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=KafkaVersion(broker_version), topics={self.topic: {
                                                                    "partitions": 1,
                                                                    "replication-factor": 1,
                                                                    'configs': {"min.insync.replicas": 1}}})
        self.kafka.start()
         
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           compression_types=["none"],
                                           version=KafkaVersion(str(TRUNK)))

        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000, new_consumer=True,
                                        message_validator=is_int, version=KafkaVersion(str(TRUNK)))

        try:
            self.run_produce_consume_validate(lambda: wait_until(
                lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
                timeout_sec=120, backoff_sec=1,
                err_msg="Producer did not produce all messages in reasonable amount of time"))
            if should_fail:
                raise Exception("Producer from version {} should fail to produce to broker version {}".format(KafkaVersion(str(TRUNK)), KafkaVersion(str(broker_version))))
        except TimeoutError as timeout_error:
            if not should_fail:
                raise timeout_error
