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
from ducktape.utils.util import wait_until
from ducktape.mark import matrix, parametrize
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.errors import TimeoutError

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.connect import ConnectServiceBase, ConnectStandaloneService, ErrorTolerance
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig

import hashlib
import json


class ConnectStandaloneFileTest(Test):
    """
    Simple test of Kafka Connect that produces data from a file in one
    standalone process and consumes it on another, validating the output is
    identical to the input.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    INPUT_FILE = "/mnt/connect.input"
    OUTPUT_FILE = "/mnt/connect.output"

    OFFSETS_FILE = "/mnt/connect.offsets"

    TOPIC = "${file:%s:topic.external}" % ConnectServiceBase.EXTERNAL_CONFIGS_FILE
    TOPIC_TEST = "test"

    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUT = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUT = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(ConnectStandaloneFileTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            'test' : { 'partitions': 1, 'replication-factor': 1 }
        }

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

    @cluster(num_nodes=5)
    @parametrize(converter="org.apache.kafka.connect.json.JsonConverter", schemas=True)
    @parametrize(converter="org.apache.kafka.connect.json.JsonConverter", schemas=False)
    @parametrize(converter="org.apache.kafka.connect.storage.StringConverter", schemas=None)
    @parametrize(security_protocol=SecurityConfig.PLAINTEXT)
    @cluster(num_nodes=6)
    @matrix(security_protocol=[SecurityConfig.SASL_SSL], metadata_quorum=quorum.all_non_upgrade)
    def test_file_source_and_sink(self, converter="org.apache.kafka.connect.json.JsonConverter", schemas=True, security_protocol='PLAINTEXT',
                                  metadata_quorum=quorum.zk):
        """
        Validates basic end-to-end functionality of Connect standalone using the file source and sink converters. Includes
        parameterizations to test different converters (which also test per-connector converter overrides), schema/schemaless
        modes, and security support.
        """
        assert converter is not None, "converter type must be set"
        # Template parameters. Note that we don't set key/value.converter. These default to JsonConverter and we validate
        # converter overrides via the connector configuration.
        if converter != "org.apache.kafka.connect.json.JsonConverter":
            self.override_key_converter = converter
            self.override_value_converter = converter
        self.schemas = schemas

        self.kafka = KafkaService(self.test_context, self.num_brokers, self.zk,
                                  security_protocol=security_protocol, interbroker_security_protocol=security_protocol,
                                  topics=self.topics, controller_num_nodes_override=self.num_zk)

        self.source = ConnectStandaloneService(self.test_context, self.kafka, [self.INPUT_FILE, self.OFFSETS_FILE],
                                               include_filestream_connectors=True)
        self.sink = ConnectStandaloneService(self.test_context, self.kafka, [self.OUTPUT_FILE, self.OFFSETS_FILE],
                                             include_filestream_connectors=True)
        self.consumer_validator = ConsoleConsumer(self.test_context, 1, self.kafka, self.TOPIC_TEST,
                                                  consumer_timeout_ms=10000)

        if self.zk:
            self.zk.start()
        self.kafka.start()

        self.source.set_configs(lambda node: self.render("connect-standalone.properties", node=node), [self.render("connect-file-source.properties")])
        self.sink.set_configs(lambda node: self.render("connect-standalone.properties", node=node), [self.render("connect-file-sink.properties")])

        self.source.set_external_configs(lambda node: self.render("connect-file-external.properties", node=node))
        self.sink.set_external_configs(lambda node: self.render("connect-file-external.properties", node=node))

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

        # Validate the format of the data in the Kafka topic
        self.consumer_validator.run()
        expected = json.dumps([line if not self.schemas else { "schema": self.SCHEMA, "payload": line } for line in self.FIRST_INPUT_LIST + self.SECOND_INPUT_LIST])
        decoder = (json.loads if converter.endswith("JsonConverter") else str)
        actual = json.dumps([decoder(x) for x in self.consumer_validator.messages_consumed[1]])
        assert expected == actual, "Expected %s but saw %s in Kafka" % (expected, actual)

    def validate_output(self, value):
        try:
            output_hash = list(self.sink.node.account.ssh_capture("md5sum " + self.OUTPUT_FILE))[0].strip().split()[0]
            return output_hash == hashlib.md5(value.encode('utf-8')).hexdigest()
        except RemoteCommandError:
            return False

    @cluster(num_nodes=5)
    @parametrize(error_tolerance=ErrorTolerance.ALL, metadata_quorum=quorum.zk)
    @parametrize(error_tolerance=ErrorTolerance.NONE, metadata_quorum=quorum.isolated_kraft)
    @parametrize(error_tolerance=ErrorTolerance.ALL, metadata_quorum=quorum.isolated_kraft)
    @parametrize(error_tolerance=ErrorTolerance.NONE, metadata_quorum=quorum.zk)
    def test_skip_and_log_to_dlq(self, error_tolerance, metadata_quorum):
        self.kafka = KafkaService(self.test_context, self.num_brokers, self.zk, topics=self.topics)

        # set config props
        self.override_error_tolerance_props = error_tolerance
        self.enable_deadletterqueue = True

        successful_records = []
        faulty_records = []
        records = []
        for i in range(0, 1000):
            if i % 2 == 0:
                records.append('{"some_key":' + str(i) + '}')
                successful_records.append('{some_key=' + str(i) + '}')
            else:
                # badly formatted json records (missing a quote after the key)
                records.append('{"some_key:' + str(i) + '}')
                faulty_records.append('{"some_key:' + str(i) + '}')

        records = "\n".join(records) + "\n"
        successful_records = "\n".join(successful_records) + "\n"
        if error_tolerance == ErrorTolerance.ALL:
            faulty_records = ",".join(faulty_records)
        else:
            faulty_records = faulty_records[0]

        self.source = ConnectStandaloneService(self.test_context, self.kafka, [self.INPUT_FILE, self.OFFSETS_FILE],
                                               include_filestream_connectors=True)
        self.sink = ConnectStandaloneService(self.test_context, self.kafka, [self.OUTPUT_FILE, self.OFFSETS_FILE],
                                             include_filestream_connectors=True)

        if self.zk:
            self.zk.start()
        self.kafka.start()

        self.override_key_converter = "org.apache.kafka.connect.storage.StringConverter"
        self.override_value_converter = "org.apache.kafka.connect.storage.StringConverter"
        self.source.set_configs(lambda node: self.render("connect-standalone.properties", node=node), [self.render("connect-file-source.properties")])

        self.override_key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.override_value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.override_key_converter_schemas_enable = False
        self.override_value_converter_schemas_enable = False
        self.sink.set_configs(lambda node: self.render("connect-standalone.properties", node=node), [self.render("connect-file-sink.properties")])

        self.source.set_external_configs(lambda node: self.render("connect-file-external.properties", node=node))
        self.sink.set_external_configs(lambda node: self.render("connect-file-external.properties", node=node))

        self.source.start()
        self.sink.start()

        # Generating data on the source node should generate new records and create new output on the sink node
        self.source.node.account.ssh("echo -e -n " + repr(records) + " >> " + self.INPUT_FILE)

        if error_tolerance == ErrorTolerance.NONE:
            try:
                wait_until(lambda: self.validate_output(successful_records), timeout_sec=15,
                           err_msg="Clean records added to input file were not seen in the output file in a reasonable amount of time.")
                raise Exception("Expected to not find any results in this file.")
            except TimeoutError:
                self.logger.info("Caught expected exception")
        else:
            wait_until(lambda: self.validate_output(successful_records), timeout_sec=15,
                       err_msg="Clean records added to input file were not seen in the output file in a reasonable amount of time.")

        if self.enable_deadletterqueue:
            self.logger.info("Reading records from deadletterqueue")
            consumer_validator = ConsoleConsumer(self.test_context, 1, self.kafka, "my-connector-errors",
                                                 consumer_timeout_ms=10000)
            consumer_validator.run()
            actual = ",".join(consumer_validator.messages_consumed[1])
            assert faulty_records == actual, "Expected %s but saw %s in dead letter queue" % (faulty_records, actual)
