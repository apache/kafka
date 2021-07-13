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


from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService


class TestBounce(Test):
    """Sanity checks on verifiable producer service class with cluster roll."""
    def __init__(self, test_context):
        super(TestBounce, self).__init__(test_context)

        quorum_size_arg_name = 'quorum_size'
        default_quorum_size = 1
        quorum_size = default_quorum_size if not test_context.injected_args else test_context.injected_args.get(quorum_size_arg_name, default_quorum_size)
        if quorum_size < 1:
            raise Exception("Illegal %s value provided for the test: %s" % (quorum_size_arg_name, quorum_size))
        self.topic = "topic"
        self.zk = ZookeeperService(test_context, num_nodes=quorum_size) if quorum.for_test(test_context) == quorum.zk else None
        num_kafka_nodes = quorum_size if quorum.for_test(test_context) == quorum.colocated_kraft else 1
        self.kafka = KafkaService(test_context, num_nodes=num_kafka_nodes, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}},
                                  controller_num_nodes_override=quorum_size)
        self.num_messages = 1000

    def create_producer(self):
        # This will produce to source kafka cluster
        self.producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka, topic=self.topic,
                                           max_messages=self.num_messages, throughput=self.num_messages // 10)
    def setUp(self):
        if self.zk:
            self.zk.start()

    # ZooKeeper and KRaft, quorum size = 1
    @cluster(num_nodes=4)
    @matrix(metadata_quorum=quorum.all, quorum_size=[1])
    # Remote and Co-located KRaft, quorum size = 3
    @cluster(num_nodes=6)
    @matrix(metadata_quorum=quorum.all_kraft, quorum_size=[3])
    def test_simple_run(self, metadata_quorum, quorum_size):
        """
        Test that we can start VerifiableProducer on the current branch snapshot version, and
        verify that we can produce a small number of messages both before and after a subsequent roll.
        """
        self.kafka.start()
        for first_time in [True, False]:
            self.create_producer()
            self.producer.start()
            wait_until(lambda: self.producer.num_acked > 5, timeout_sec=15,
                       err_msg="Producer failed to start in a reasonable amount of time.")

            self.producer.wait()
            num_produced = self.producer.num_acked
            assert num_produced == self.num_messages, "num_produced: %d, num_messages: %d" % (num_produced, self.num_messages)
            if first_time:
                self.producer.stop()
                if self.kafka.quorum_info.using_kraft and self.kafka.remote_controller_quorum:
                    self.kafka.remote_controller_quorum.restart_cluster()
                self.kafka.restart_cluster()
