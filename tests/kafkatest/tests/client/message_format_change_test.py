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

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import config_property, KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_0_9, LATEST_0_10, LATEST_0_11, V_2_8_0, DEV_BRANCH, KafkaVersion


class MessageFormatChangeTest(ProduceConsumeValidateTest):

    def __init__(self, test_context):
        super(MessageFormatChangeTest, self).__init__(test_context=test_context)

    def setUp(self):
        self.topic = "test_topic"
        self.zk = ZookeeperService(self.test_context, num_nodes=1) if quorum.for_test(self.test_context) == quorum.zk else None

        if self.zk:
            self.zk.start()

        # Producer and consumer
        self.producer_throughput = 10000
        self.num_producers = 1
        self.num_consumers = 1
        self.messages_per_producer = 100

    def produce_and_consume(self, producer_version, consumer_version, group):
        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic,
                                           throughput=self.producer_throughput,
                                           message_validator=is_int,
                                           version=KafkaVersion(producer_version))
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka,
                                        self.topic, consumer_timeout_ms=30000,
                                        message_validator=is_int, version=KafkaVersion(consumer_version))
        self.consumer.group_id = group
        self.run_produce_consume_validate(lambda: wait_until(
            lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
            timeout_sec=120, backoff_sec=1,
            err_msg="Producer did not produce all messages in reasonable amount of time"))

    @cluster(num_nodes=12)
    @matrix(producer_version=[str(DEV_BRANCH)], consumer_version=[str(DEV_BRANCH)], metadata_quorum=[quorum.zk])
    @matrix(producer_version=[str(LATEST_0_10)], consumer_version=[str(LATEST_0_10)], metadata_quorum=[quorum.zk])
    @matrix(producer_version=[str(LATEST_0_9)], consumer_version=[str(LATEST_0_9)], metadata_quorum=[quorum.zk])
    def test_compatibility(self, producer_version, consumer_version, metadata_quorum=quorum.zk):
        """ This tests performs the following checks:
        The workload is a mix of 0.9.x, 0.10.x and 0.11.x producers and consumers
        that produce to and consume from a DEV_BRANCH cluster
        1. initially the topic is using message format 0.9.0
        2. change the message format version for topic to 0.10.0 on the fly.
        3. change the message format version for topic to 0.11.0 on the fly.
        4. change the message format version for topic back to 0.10.0 on the fly (only if the client version is 0.11.0 or newer)
        - The producers and consumers should not have any issue.

        Note regarding step number 4. Downgrading the message format version is generally unsupported as it breaks
        older clients. More concretely, if we downgrade a topic from 0.11.0 to 0.10.0 after it contains messages with
        version 0.11.0, we will return the 0.11.0 messages without down conversion due to an optimisation in the
        handling of fetch requests. This will break any consumer that doesn't support 0.11.0. So, in practice, step 4
        is similar to step 2 and it didn't seem worth it to increase the cluster size to in order to add a step 5 that
        would change the message format version for the topic back to 0.9.0.0.
        """
        self.kafka = KafkaService(self.test_context, num_nodes=3, zk=self.zk, version=DEV_BRANCH, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    'configs': {"min.insync.replicas": 2}}},
                                                                    controller_num_nodes_override=1)
        for node in self.kafka.nodes:
            node.config[config_property.INTER_BROKER_PROTOCOL_VERSION] = str(V_2_8_0) # required for writing old message formats

        self.kafka.start()
        self.logger.info("First format change to 0.9.0")
        self.kafka.alter_message_format(self.topic, str(LATEST_0_9))
        self.produce_and_consume(producer_version, consumer_version, "group1")

        self.logger.info("Second format change to 0.10.0")
        self.kafka.alter_message_format(self.topic, str(LATEST_0_10))
        self.produce_and_consume(producer_version, consumer_version, "group2")

        self.logger.info("Third format change to 0.11.0")
        self.kafka.alter_message_format(self.topic, str(LATEST_0_11))
        self.produce_and_consume(producer_version, consumer_version, "group3")

        if producer_version == str(DEV_BRANCH) and consumer_version == str(DEV_BRANCH):
            self.logger.info("Fourth format change back to 0.10.0")
            self.kafka.alter_message_format(self.topic, str(LATEST_0_10))
            self.produce_and_consume(producer_version, consumer_version, "group4")


