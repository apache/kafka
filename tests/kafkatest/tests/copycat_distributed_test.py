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

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUTS = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUTS = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(CopycatDistributedFileTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test' : { 'partitions': 1, 'replication-factor': 1 }
        })

        self.cc = CopycatDistributedService(test_context, 2, self.kafka, [self.INPUT_FILE, self.OUTPUT_FILE])

    def test_file_source_and_sink(self, converter="org.apache.kafka.copycat.json.JsonConverter", schemas=True):
        assert converter != None, "converter type must be set"
        # Template parameters
        self.key_converter = converter
        self.value_converter = converter
        self.schemas = schemas

        self.cc.set_configs(lambda node: self.render("copycat-distributed.properties", node=node))

        self.cc.start()

        self.logger.info("Creating connectors")
        for connector_props in [self.render("copycat-file-source.properties"), self.render("copycat-file-sink.properties")]:
            connector_config = dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])
            self.cc.create_connector(connector_config)

        # Generating data on the source node should generate new records and create new output on the sink node. Timeouts
        # here need to be more generous than they are for standalone mode because a) it takes longer to write configs,
        # do rebalancing of the group, etc, and b) without explicit leave group support, rebalancing takes awhile
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.FIRST_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.FIRST_INPUT_LIST), timeout_sec=120, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Restarting both should result in them picking up where they left off,
        # only processing new data.
        self.cc.restart()

        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.SECOND_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.FIRST_INPUT_LIST + self.SECOND_INPUT_LIST), timeout_sec=120, err_msg="Sink output file never converged to the same state as the input file")

    def validate_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
        # Between the first and second rounds, we might even end up with half the data on each node.
        output_set = set(itertools.chain(*[
            [line.strip() for line in self.file_contents(node, self.OUTPUT_FILE)] for node in self.cc.nodes
        ]))
        return input_set == output_set


    def file_contents(self, node, file):
        try:
            # Convert to a list here or the CalledProcessError may be returned during a call to the generator instead of
            # immediately
            return list(node.account.ssh_capture("cat " + file))
        except subprocess.CalledProcessError:
            return []
