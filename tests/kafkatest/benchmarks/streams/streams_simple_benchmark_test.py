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
from ducktape.mark import parametrize, matrix, ignore
from kafkatest.tests.kafka_test import KafkaTest

from kafkatest.services.performance.streams_performance import StreamsSimpleBenchmarkService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.version import DEV_BRANCH
import time


class StreamsSimpleBenchmarkTest(Test):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context)
        self.num_records = 1000000L
        self.replication = 1


    def setup_system(self, scale):
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
            'joinSourceTopic2KTableKTable' : { 'partitions': scale, 'replication-factor': self.replication }
        })
        self.kafka.start()

    def load_system(self, scale, test):
        ################
        # LOAD PHASE
        ################
        self.load_driver = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                         self.num_records * scale, "true", test)
        self.load_driver.start()
        self.load_driver.wait()
        self.load_driver.stop()

    def stop_and_collect_data(self, scale):
        #######################
        # STOP + COLLECT PHASE
        #######################
        for num in range(0, scale):    
            self.driver[num].wait()    
            self.driver[num].stop()
            self.node[num] = self.driver[num].node
            self.node[num].account.ssh("grep Performance %s" % self.driver[num].STDOUT_FILE, allow_fail=False)
            self.data[num] = self.driver[num].collect_data(self.node[num], "" )
        
    @cluster(num_nodes=9)
    @matrix(test=["consume", "processstream", "processstreamwithsink", "processstreamwithstatestore", "kstreamktablejoin", "kstreamkstreamjoin", "ktablektablejoin", "count"], scale=[1])
    def test_simple_benchmark(self, test, scale):
        """
        Run simple Kafka Streams benchmark
        """
        self.driver = [None] * (scale + 1)
        self.node = [None] * (scale)
        self.data = [None] * (scale)

        self.setup_system(scale)
        self.load_system(scale, test)
        
        ################
        # RUN PHASE
        ################
        start_time = time.time()
        for num in range(0, scale):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                             self.num_records/(scale), "false", test)
            self.driver[num].start()

        self.stop_and_collect_data(scale)
        end_time = time.time()
        
        final = {}
        for num in range(0, scale):
            for key in self.data[num]:
                final[key + str(num)] = self.data[num][key]
        final[test + str(" latency")] = end_time - start_time
        
        return final


    # This test should not run nightly. It is currently designed for manual runs.
    @ignore
    @cluster(num_nodes=9)
    @matrix(test=["count"], scale=[1])
    def test_benchmark_with_failure_and_restore(self, test, scale):
        """
        Run simple Kafka Streams benchmark
        """
        self.driver = [None] * (scale + 1)
        self.node = [None] * (scale + 1)
        self.data = [None] * (scale + 1)
        
        self.setup_system(scale)
        self.load_system(scale, test)
        

        ################
        # RUN PHASE
        ################
        start_time = time.time()
        for num in range(0, scale):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                             self.num_records/2, "false", test)
            self.driver[num].start()

        self.stop_and_collect_data(scale)
         
        ################
        # RESTART PHASE
        ################
        for num in range(0, scale):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                             self.num_records/3, "false", test)
            self.driver[num].start()

        self.stop_and_collect_data(scale)
        end_time = time.time()


        final = {}
        for num in range(0, scale):
            for key in self.data[num]:
                final[key + str(num)] = self.data[num][key]

        final[test + str(" latency")] = end_time - start_time

        return final

    # This test should not run nightly. It is currently designed for manual runs.
    @ignore
    @cluster(num_nodes=9)
    @matrix(test=["count"])
    def test_benchmark_with_standby_tasks(self, test):
        """
        Run simple Kafka Streams benchmark
        """
        scale = 1
        # Note: replicas must match number of replicas in SimpleBenchmark.java
        # Currently we are not passing this argument to that file, but should do.
        # This is a manual step.
        replicas = 2
        
        self.driver = [None] * (scale * replicas)
        self.node = [None] * (scale * replicas)
        self.data = [None] * (scale * replicas)
        
        self.setup_system(scale)
        self.load_system(scale, test)
        
        ################
        # RUN PHASE
        ################
        start_time = time.time()
        #
        for num in range(0, scale):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                             self.num_records/2, "false", test)
            self.driver[num].start()
        #
        for num in range(scale, scale*replicas):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context, self.kafka,
                                                             self.num_records/3, "false", test)
            self.driver[num].start()

        self.stop_and_collect_data(scale * replicas)

        end_time = time.time()


        final = {}
        for num in range(0, scale):
            for key in self.data[num]:
                final[key + str(num)] = self.data[num][key]

        final[test + str(" latency")] = end_time - start_time

        return final
