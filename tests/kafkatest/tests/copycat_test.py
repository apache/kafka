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
from kafkatest.services.copycat import CopycatStandaloneService
from ducktape.utils.util import wait_until
import hashlib, subprocess

class CopycatStandaloneFileTest(KafkaTest):
    """
    Simple test of Copycat that produces data from a file in one Copycat
    standalone process and consumes it on another, validating the output is
    identical to the input.
    """

    INPUT_FILE = "/mnt/copycat.input"
    OUTPUT_FILE = "/mnt/copycat.output"

    OFFSETS_FILE = "/mnt/copycat.offsets"

    FIRST_INPUT = "foo\nbar\nbaz\n"
    SECOND_INPUT = "razz\nma\ntazz\n"

    def __init__(self, test_context):
        super(CopycatStandaloneFileTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test' : { 'partitions': 1, 'replication-factor': 1 }
        })

        self.source = CopycatStandaloneService(test_context, self.kafka, [self.INPUT_FILE, self.OFFSETS_FILE])
        self.sink = CopycatStandaloneService(test_context, self.kafka, [self.OUTPUT_FILE, self.OFFSETS_FILE])

    def test_file_source_and_sink(self):
        # These need to be set
        self.source.set_configs(self.render("copycat-standalone.properties"), self.render("copycat-file-source.properties"))
        self.sink.set_configs(self.render("copycat-standalone.properties"), self.render("copycat-file-sink.properties"))

        self.source.start()
        self.sink.start()

        # Generating data on the source node should generate new records and create new output on the sink node
        self.source.node.account.ssh("echo -e -n " + repr(self.FIRST_INPUT) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.FIRST_INPUT), timeout_sec=60, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Restarting both should result in them picking up where they left off,
        # only processing new data.
        self.source.restart()
        self.sink.restart()

        self.source.node.account.ssh("echo -e -n " + repr(self.SECOND_INPUT) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.FIRST_INPUT + self.SECOND_INPUT), timeout_sec=60, err_msg="Sink output file never converged to the same state as the input file")

    def validate_output(self, value):
        try:
            output_hash = list(self.sink.node.account.ssh_capture("md5sum " + self.OUTPUT_FILE))[0].strip().split()[0]
            return output_hash == hashlib.md5(value).hexdigest()
        except subprocess.CalledProcessError:
            return False
