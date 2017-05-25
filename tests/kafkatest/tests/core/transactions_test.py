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


from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int

from ducktape.tests.test import Test
from ducktape.mark import parametrize
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
        self.num_input_partitions = 2
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
                                      self.input_topic : {
                                          "partitions" : self.num_input_partitions,
                                          "replication-factor": 3,
                                          "configs": {
                                              "min.insync.replicas" : 2
                                          }
                                      },
                                      self.output_topic : {
                                          "partitions" : self.num_output_partitions,
                                          "replication-factor": 3,
                                          "configs": {
                                              "min.insync.replicas" : 2
                                          }
                                      }
                                  })


       @cluster(num_nodes=8)
       def test_transactions(self):
           security_protocol = 'PLAINTEXT'
           self.kafka.security_protocol = security_protocol
           self.kafka.interbroker_security_protocol = security_protocol
           seed_timeout_ms = 10000
           seed_producer = VerifiableProducer(context=self.test_context,
                                              num_nodes=1,
                                              kafka=self.kafka,
                                              topic=self.input_topic,
                                              message_validator=is_int,
                                              max_messages=self.num_seed_messages,
                                              enable_idempotence=True)
           self.kafka.start()
           seed_producer.start()
           wait_until(lambda: seed_producer.num_acked >= this.num_seed_messages,
                      timeout_sec=seed_timeout_ms,
                      err_msg="Producer failed to produce messages for %ds." %\
                      seed_timeout_ms)






    print "hello, transactions!"
    k
