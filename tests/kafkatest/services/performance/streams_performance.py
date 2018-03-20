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

#
# Class used to start the simple Kafka Streams benchmark
#
class StreamsSimpleBenchmarkService(StreamsTestBaseService):
    """Base class for simple Kafka Streams benchmark"""

    def __init__(self, test_context, kafka, numrecs, load_phase, test_name, num_threads):
        super(StreamsSimpleBenchmarkService, self).__init__(test_context,
                                                            kafka,
                                                            "org.apache.kafka.streams.perf.SimpleBenchmark",
                                                            numrecs,
                                                            load_phase,
                                                            test_name,
                                                            num_threads)

        self.load_phase = load_phase

        if self.load_phase == "false":
            JmxMixin.__init__(self, num_nodes=1,
                              jmx_object_names=['kafka.streams:type=stream-metrics,client-id=simple-benchmark-StreamThread-%d' %i for i in range(num_threads)],
                              jmx_attributes=['process-latency-avg',
                                              'process-rate',
                                              'commit-latency-avg',
                                              'commit-rate',
                                              'poll-latency-avg',
                                              'poll-rate'],
                              root=StreamsTestBaseService.PERSISTENT_ROOT,
                              report_interval=5000)

    def start_cmd(self, node):
        cmd = super(StreamsSimpleBenchmarkService, self).start_cmd(node)

        if self.load_phase == "false":
            args = self.args.copy()
            args['jmx_port'] = self.jmx_port
            args['kafka'] = self.kafka.bootstrap_servers()
            args['config_file'] = self.CONFIG_FILE
            args['stdout'] = self.STDOUT_FILE
            args['stderr'] = self.STDERR_FILE
            args['pidfile'] = self.PID_FILE
            args['log4j'] = self.LOG4J_CONFIG_FILE
            args['kafka_run_class'] = self.path.script("kafka-run-class.sh", node)

            cmd = "( export JMX_PORT=%(jmx_port)s; export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j)s\"; " \
                  "INCLUDE_TEST_JARS=true %(kafka_run_class)s %(streams_class_name)s " \
                  " %(kafka)s %(config_file)s %(user_test_args)s %(user_test_args1)s %(user_test_args2)s" \
                  " %(user_test_args3)s & echo $! >&3 ) 1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s" % args


            self.start_jmx_tool(1, node)

        self.logger.info("Executing streams simple benchmark cmd: " + cmd)

        return cmd

    def start_node(self, node):
        super(StreamsSimpleBenchmarkService, self).start_node(node)

        if self.load_phase == "false":
            self.start_jmx_tool(1, node)


def clean_node(self, node):
        if self.load_phase == "false":
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
