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

from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.streams import StreamsTestBaseService
from kafkatest.services.kafka import KafkaConfig
from kafkatest.services import streams_property

#
# Class used to start the simple Kafka Streams benchmark
#

class StreamsSimpleBenchmarkService(StreamsTestBaseService):
    """Base class for simple Kafka Streams benchmark"""

    def __init__(self, test_context, kafka, test_name, num_threads, num_recs_or_wait_ms, key_skew, value_size):
        super(StreamsSimpleBenchmarkService, self).__init__(test_context,
                                                            kafka,
                                                            "org.apache.kafka.streams.perf.SimpleBenchmark",
                                                            test_name,
                                                            num_recs_or_wait_ms,
                                                            key_skew,
                                                            value_size)

        self.jmx_option = ""
        if test_name.startswith('stream') or test_name.startswith('table'):
            self.jmx_option = "stream-jmx"
            JmxMixin.__init__(self,
                              num_nodes=1,
                              jmx_object_names=['kafka.streams:type=stream-metrics,client-id=simple-benchmark-StreamThread-%d' %(i+1) for i in range(num_threads)],
                              jmx_attributes=['process-latency-avg',
                                              'process-rate',
                                              'commit-latency-avg',
                                              'commit-rate',
                                              'poll-latency-avg',
                                              'poll-rate'],
                              root=StreamsTestBaseService.PERSISTENT_ROOT)

        if test_name.startswith('consume'):
            self.jmx_option = "consumer-jmx"
            JmxMixin.__init__(self,
                              num_nodes=1,
                              jmx_object_names=['kafka.consumer:type=consumer-fetch-manager-metrics,client-id=simple-benchmark-consumer'],
                              jmx_attributes=['records-consumed-rate'],
                              root=StreamsTestBaseService.PERSISTENT_ROOT)

        self.num_threads = num_threads

    def prop_file(self):
        cfg = KafkaConfig(**{streams_property.STATE_DIR: self.PERSISTENT_ROOT,
                             streams_property.KAFKA_SERVERS: self.kafka.bootstrap_servers(),
                             streams_property.NUM_THREADS: self.num_threads})
        return cfg.render()


    def start_cmd(self, node):
        if self.jmx_option != "":
            args = self.args.copy()
            args['jmx_port'] = self.jmx_port
            args['config_file'] = self.CONFIG_FILE
            args['stdout'] = self.STDOUT_FILE
            args['stderr'] = self.STDERR_FILE
            args['pidfile'] = self.PID_FILE
            args['log4j'] = self.LOG4J_CONFIG_FILE
            args['kafka_run_class'] = self.path.script("kafka-run-class.sh", node)

            cmd = "( export JMX_PORT=%(jmx_port)s; export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j)s\"; " \
                  "INCLUDE_TEST_JARS=true %(kafka_run_class)s %(streams_class_name)s " \
                  " %(config_file)s %(user_test_args1)s %(user_test_args2)s %(user_test_args3)s" \
                  " %(user_test_args4)s & echo $! >&3 ) 1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s" % args

        else:
            cmd = super(StreamsSimpleBenchmarkService, self).start_cmd(node)

        return cmd

    def start_node(self, node):
        super(StreamsSimpleBenchmarkService, self).start_node(node)

        if self.jmx_option != "":
            self.start_jmx_tool(1, node)

    def clean_node(self, node):
        if self.jmx_option != "":
            JmxMixin.clean_node(self, node)

        super(StreamsSimpleBenchmarkService, self).clean_node(node)

    def collect_data(self, node, tag = None):
        # Collect the data and return it to the framework
        output = node.account.ssh_capture("grep Performance %s" % self.STDOUT_FILE)
        data = {}
        for line in output:
            parts = line.split(':')
            data[tag + parts[0]] = parts[1]
        return data
