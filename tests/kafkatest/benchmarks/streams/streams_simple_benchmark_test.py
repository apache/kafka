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

from ducktape.mark.resource import cluster

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.performance.streams_performance import StreamsSimpleBenchmarkService


class StreamsSimpleBenchmarkTest(KafkaTest):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context, num_zk=1, num_brokers=1,topics={
            'simpleBenchmarkSourceTopic' : { 'partitions': 9, 'replication-factor': 1 },
            'simpleBenchmarkSinkTopic' : { 'partitions': 9, 'replication-factor': 1 },
            'joinSourceTopic1KStreamKStream' : { 'partitions': 9, 'replication-factor': 1 },
            'joinSourceTopic2KStreamKStream' : { 'partitions': 9, 'replication-factor': 1 },
            'joinSourceTopic1KStreamKTable' : { 'partitions': 9, 'replication-factor': 1 },
            'joinSourceTopic2KStreamKTable' : { 'partitions': 9, 'replication-factor': 1 },
            'joinSourceTopic1KTableKTable' : { 'partitions': 9, 'replication-factor': 1 },
            'joinSourceTopic2KTableKTable' : { 'partitions': 9, 'replication-factor': 1 }
        })
        self.scale = 3 
        self.driver = [None] * (self.scale + 1)


    @cluster(num_nodes=9)
    def test_simple_benchmark(self):
        """
        Run simple Kafka Streams benchmark
        """
        tests = ["processstream"]
        node = [None] * (self.scale)
        data = [None] * (self.scale)
        for test_name in tests:
            self.driver[0] = StreamsSimpleBenchmarkService(self.test_context, self.kafka, 1000000L, "true", test_name)
            self.driver[0].start()
            self.driver[0].wait()
            self.driver[0].stop()

            for num in range(1, self.scale):
                self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka, 1000L, "false", test_name)
                self.driver[num].start()
                
            for num in range(1, self.scale):    
                self.driver[num].wait()    
                self.driver[num].stop()
                node[num] = self.driver[num].node
                node[num].account.ssh("grep Performance %s" % self.driver[num].STDOUT_FILE, allow_fail=False)
                data[num] = self.driver[num].collect_data(node[num], "Node " + str(num) + " ")
                
        final = data[1].copy()
        for num in range(2, self.scale):    
           final.update(data[num])
        
        return final
