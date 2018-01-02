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
from ducktape.mark.resource import cluster
from ducktape.mark import parametrize, matrix
from kafkatest.tests.kafka_test import KafkaTest

from kafkatest.services.performance.streams_performance import StreamsSimpleBenchmarkService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.version import DEV_BRANCH

class StreamsSimpleBenchmarkTest(Test):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context)
        self.num_records = 10000000L
        self.replication = 1
        self.num_threads = 1

    @cluster(num_nodes=9)
    @matrix(test=["produce", "consume", "count", "processstream", "processstreamwithsink", "processstreamwithstatestore", "processstreamwithcachedstatestore", "kstreamktablejoin", "kstreamkstreamjoin", "ktablektablejoin", "yahoo"], scale=[1, 3])
    def test_simple_benchmark(self, test, scale):
        """
        Run simple Kafka Streams benchmark
        """
        self.driver = [None] * (scale + 1)
        node = [None] * (scale)
        data = [None] * (scale)

        #############
        # SETUP PHASE
        #############
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()
        self.kafka = KafkaService(self.test_context, num_nodes=scale, zk=self.zk, version=DEV_BRANCH, topics={
            'simpleBenchmarkSourceTopic' : { 'partitions': scale, 'replication-factor': self.replication },
            'countTopic' : { 'partitions': scale, 'replication-factor': self.replication },
            'simpleBenchmarkSinkTopic' : { 'partitions': scale, 'replication-factor': self.replication },
            'joinSourceTopic1KStreamKStream' : { 'partitions': scale, 'replication-factor': self.replication },
            'joinSourceTopic2KStreamKStream' : { 'partitions': scale, 'replication-factor': self.replication },
            'joinSourceTopic1KStreamKTable' : { 'partitions': scale, 'replication-factor': self.replication },
            'joinSourceTopic2KStreamKTable' : { 'partitions': scale, 'replication-factor': self.replication },
            'joinSourceTopic1KTableKTable' : { 'partitions': scale, 'replication-factor': self.replication },
            'joinSourceTopic2KTableKTable' : { 'partitions': scale, 'replication-factor': self.replication },
            'yahooCampaigns' : { 'partitions': 20, 'replication-factor': self.replication },
            'yahooEvents' : { 'partitions': 20, 'replication-factor': self.replication }
        })
        self.kafka.log_level = "INFO"
        self.kafka.start()
 
        ################
        # LOAD PHASE
        ################
        self.load_driver = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                         self.num_records * scale, "true", test,
                                                         self.num_threads)
        self.load_driver.start()
        self.load_driver.wait()
        self.load_driver.stop()

        ################
        # RUN PHASE
        ################
        for num in range(0, scale):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                             self.num_records/(scale), "false", test,
                                                             self.num_threads)
            self.driver[num].start()

        #######################
        # STOP + COLLECT PHASE
        #######################
        for num in range(0, scale):    
            self.driver[num].wait()    
            self.driver[num].stop()
            node[num] = self.driver[num].node
            node[num].account.ssh("grep Performance %s" % self.driver[num].STDOUT_FILE, allow_fail=False)
            data[num] = self.driver[num].collect_data(node[num], "" )
                

        final = {}
        for num in range(0, scale):
            for key in data[num]:
                final[key + str(num)] = data[num][key]
        
        return final
