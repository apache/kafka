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

from kafkatest.tests.kafka_test import KafkaTest
from kafkatest.services.copycat import CopycatDistributedService
from ducktape.utils.util import wait_until
import hashlib, subprocess, json, itertools

class CopycatDistributedFileTest(KafkaTest):
    """
    Simple test of Copycat in distributed mode, producing data from files on on Copycat cluster and consuming it on
    another, validating the total output is identical to the input.
    """

    INPUT_FILE = "/mnt/copycat.input"
    OUTPUT_FILE = "/mnt/copycat.output"

    TOPIC = "test"
    OFFSETS_TOPIC = "copycat-offsets"
    CONFIG_TOPIC = "copycat-configs"

    FIRST_INPUT_LISTS = [["foo", "bar", "baz"], ["foo2", "bar2", "baz2"]]
    FIRST_INPUTS = ["\n".join(input_list) + "\n" for input_list in FIRST_INPUT_LISTS]
    SECOND_INPUT_LISTS = [["razz", "ma", "tazz"], ["razz2", "ma2", "tazz2"]]
    SECOND_INPUTS = ["\n".join(input_list) + "\n" for input_list in SECOND_INPUT_LISTS]

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(CopycatDistributedFileTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test' : { 'partitions': 1, 'replication-factor': 1 }
        })

        # FIXME these should have multiple nodes. However, currently the connectors are submitted via command line,
        # which means we would get duplicates. Both would run, but they would have conflicting keys for offsets and
        # configs. Until we have real distributed coordination of workers with unified connector submission, we need
        # to restrict each of these to a single node.
        self.num_nodes = 1
        self.source = CopycatDistributedService(test_context, self.num_nodes, self.kafka, [self.INPUT_FILE])
        self.sink = CopycatDistributedService(test_context, self.num_nodes, self.kafka, [self.OUTPUT_FILE])

    def test_file_source_and_sink(self, converter="org.apache.kafka.copycat.json.JsonConverter", schemas=True):
        assert converter != None, "converter type must be set"
        # Template parameters
        self.key_converter = converter
        self.value_converter = converter
        self.schemas = schemas

        # These need to be set
        self.source.set_configs(self.render("copycat-distributed.properties"), self.render("copycat-file-source.properties"))
        self.sink.set_configs(self.render("copycat-distributed.properties"), self.render("copycat-file-sink.properties"))

        self.source.start()
        self.sink.start()

        # Generating data on the source node should generate new records and create new output on the sink node
        for node, input in zip(self.source.nodes, self.FIRST_INPUTS):
            node.account.ssh("echo -e -n " + repr(input) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.FIRST_INPUT_LISTS[:self.num_nodes]), timeout_sec=60, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Restarting both should result in them picking up where they left off,
        # only processing new data.
        self.source.restart()
        self.sink.restart()

        for node, input in zip(self.source.nodes, self.SECOND_INPUTS):
            node.account.ssh("echo -e -n " + repr(input) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.FIRST_INPUT_LISTS[:self.num_nodes] + self.SECOND_INPUT_LISTS[:self.num_nodes]), timeout_sec=60, err_msg="Sink output file never converged to the same state as the input file")

    def validate_output(self, inputs):
        try:
            input_set = set(itertools.chain(*inputs))
            output_set = set(itertools.chain(*[
                [line.strip() for line in node.account.ssh_capture("cat " + self.OUTPUT_FILE)] for node in self.sink.nodes
            ]))
            return input_set == output_set
        except subprocess.CalledProcessError:
            return False
