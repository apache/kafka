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

    def collect_data(self, node, tag = None):
        # Collect the data and return it to the framework
        output = node.account.ssh_capture("grep Performance %s" % self.STDOUT_FILE)
        data = {}
        for line in output:
            parts = line.split(':')
            data[tag + parts[0]] = parts[1]
        return data
