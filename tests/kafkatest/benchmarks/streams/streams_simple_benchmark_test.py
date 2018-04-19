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

STREAMS_SIMPLE_TESTS = ["streamprocess", "streamprocesswithsink", "streamprocesswithstatestore", "streamprocesswithwindowstore"]
STREAMS_COUNT_TESTS = ["streamcount", "streamcountwindowed"]
STREAMS_JOIN_TESTS = ["streamtablejoin", "streamstreamjoin", "tabletablejoin"]
NON_STREAMS_TESTS = ["consume", "consumeproduce"]

ALL_TEST = "all"
STREAMS_SIMPLE_TEST = "streams-simple"
STREAMS_COUNT_TEST = "streams-count"
STREAMS_JOIN_TEST = "streams-join"


class StreamsSimpleBenchmarkTest(Test):
    """
    Simple benchmark of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsSimpleBenchmarkTest, self).__init__(test_context)

        # these values could be updated in ad-hoc benchmarks
        self.key_skew = 0
        self.value_size = 1024
        self.num_records = 10000000L
        self.num_threads = 1

        self.replication = 1

    @cluster(num_nodes=12)
    @matrix(test=["consume", "consumeproduce", "streams-simple", "streams-count", "streams-join"], scale=[1])
    def test_simple_benchmark(self, test, scale):
        """
        Run simple Kafka Streams benchmark
        """
        self.driver = [None] * (scale + 1)

        self.final = {}

        #############
        # SETUP PHASE
        #############
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()
        self.kafka = KafkaService(self.test_context, num_nodes=scale, zk=self.zk, version=DEV_BRANCH, topics={
            'simpleBenchmarkSourceTopic1' : { 'partitions': scale, 'replication-factor': self.replication },
            'simpleBenchmarkSourceTopic2' : { 'partitions': scale, 'replication-factor': self.replication },
            'simpleBenchmarkSinkTopic' : { 'partitions': scale, 'replication-factor': self.replication },
            'yahooCampaigns' : { 'partitions': 20, 'replication-factor': self.replication },
            'yahooEvents' : { 'partitions': 20, 'replication-factor': self.replication }
        })
        self.kafka.log_level = "INFO"
        self.kafka.start()


        load_test = ""
        if test == ALL_TEST:
            load_test = "load-two"
        if test in STREAMS_JOIN_TESTS or test == STREAMS_JOIN_TEST:
            load_test = "load-two"
        if test in STREAMS_COUNT_TESTS or test == STREAMS_COUNT_TEST:
            load_test = "load-one"
        if test in STREAMS_SIMPLE_TESTS or test == STREAMS_SIMPLE_TEST:
            load_test = "load-one"
        if test in NON_STREAMS_TESTS:
            load_test = "load-one"



        ################
        # LOAD PHASE
        ################
        self.load_driver = StreamsSimpleBenchmarkService(self.test_context,
                                                         self.kafka,
                                                         load_test,
                                                         self.num_threads,
                                                         self.num_records,
                                                         self.key_skew,
                                                         self.value_size)

        self.load_driver.start()
        self.load_driver.wait(3600) # wait at most 30 minutes
        self.load_driver.stop()

        if test == ALL_TEST:
            for single_test in STREAMS_SIMPLE_TESTS + STREAMS_COUNT_TESTS + STREAMS_JOIN_TESTS:
                self.run_test(single_test, scale)
        elif test == STREAMS_SIMPLE_TEST:
            for single_test in STREAMS_SIMPLE_TESTS:
                self.run_test(single_test, scale)
        elif test == STREAMS_COUNT_TEST:
            for single_test in STREAMS_COUNT_TESTS:
                self.run_test(single_test, scale)
        elif test == STREAMS_JOIN_TEST:
            for single_test in STREAMS_JOIN_TESTS:
                self.run_test(single_test, scale)
        else:
            self.run_test(test, scale)

        return self.final

    def run_test(self, test, scale):

        ################
        # RUN PHASE
        ################
        for num in range(0, scale):
            self.driver[num] = StreamsSimpleBenchmarkService(self.test_context,
                                                             self.kafka,
                                                             test,
                                                             self.num_threads,
                                                             self.num_records,
                                                             self.key_skew,
                                                             self.value_size)
            self.driver[num].start()

        #######################
        # STOP + COLLECT PHASE
        #######################
        data = [None] * (scale)

        for num in range(0, scale):
            self.driver[num].wait()
            self.driver[num].stop()
            self.driver[num].node.account.ssh("grep Performance %s" % self.driver[num].STDOUT_FILE, allow_fail=False)
            data[num] = self.driver[num].collect_data(self.driver[num].node, "")
            self.driver[num].read_jmx_output_all_nodes()

        for num in range(0, scale):
            for key in data[num]:
                self.final[key + "-" + str(num)] = data[num][key]

            for key in sorted(self.driver[num].jmx_stats[0]):
                self.logger.info("%s: %s" % (key, self.driver[num].jmx_stats[0][key]))

            self.final[test + "-jmx-avg-" + str(num)] = self.driver[num].average_jmx_value
            self.final[test + "-jmx-max-" + str(num)] = self.driver[num].maximum_jmx_value
