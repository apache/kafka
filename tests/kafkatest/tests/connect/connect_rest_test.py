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
from kafkatest.services.connect import ConnectDistributedService, ConnectRestError, ConnectServiceBase
from kafkatest.services.kafka import quorum
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.cluster.remoteaccount import RemoteCommandError

import json
import itertools


class ConnectRestApiTest(KafkaTest):
    """
    Test of Kafka Connect's REST API endpoints.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    FILE_SOURCE_CONFIGS = {'name', 'connector.class', 'tasks.max', 'key.converter', 'value.converter', 'header.converter', 'batch.size',
                           'topic', 'file', 'transforms', 'config.action.reload', 'errors.retry.timeout', 'errors.retry.delay.max.ms',
                           'errors.tolerance', 'errors.log.enable', 'errors.log.include.messages', 'predicates', 'topic.creation.groups',
                           'exactly.once.support', 'transaction.boundary', 'transaction.boundary.interval.ms', 'offsets.storage.topic',
                           'tasks.max.enforce'}
    FILE_SINK_CONFIGS = {'name', 'connector.class', 'tasks.max', 'key.converter', 'value.converter', 'header.converter', 'topics',
                         'file', 'transforms', 'topics.regex', 'config.action.reload', 'errors.retry.timeout', 'errors.retry.delay.max.ms',
                         'errors.tolerance', 'errors.log.enable', 'errors.log.include.messages', 'errors.deadletterqueue.topic.name',
                         'errors.deadletterqueue.topic.replication.factor', 'errors.deadletterqueue.context.headers.enable', 'predicates',
                         'tasks.max.enforce'}

    INPUT_FILE = "/mnt/connect.input"
    INPUT_FILE2 = "/mnt/connect.input2"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "topic-${file:%s:topic.external}" % ConnectServiceBase.EXTERNAL_CONFIGS_FILE
    TOPIC_TEST = "test"

    DEFAULT_BATCH_SIZE = "2000"
    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    INPUT_LIST = ["foo", "bar", "baz"]
    INPUTS = "\n".join(INPUT_LIST) + "\n"
    LONGER_INPUT_LIST = ["foo", "bar", "baz", "razz", "ma", "tazz"]
    LONER_INPUTS = "\n".join(LONGER_INPUT_LIST) + "\n"

    SCHEMA = {"type": "string", "optional": False}

    CONNECT_PROTOCOL="compatible"

    def __init__(self, test_context):
        super(ConnectRestApiTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test': {'partitions': 1, 'replication-factor': 1}
        })

        self.cc = ConnectDistributedService(test_context, 2, self.kafka, [self.INPUT_FILE, self.INPUT_FILE2, self.OUTPUT_FILE],
                                            include_filestream_connectors=True)

    @cluster(num_nodes=4)
    @matrix(connect_protocol=['compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_rest_api(self, connect_protocol, metadata_quorum):
        # Template parameters
        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True
        self.CONNECT_PROTOCOL = connect_protocol

        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.set_external_configs(lambda node: self.render("connect-file-external.properties", node=node))

        self.cc.start()

        assert self.cc.list_connectors() == []

        # After MM2 and the connector classes that it added, the assertion here checks that the registered
        # Connect plugins are a superset of the connectors expected to be present.
        assert set([connector_plugin['class'] for connector_plugin in self.cc.list_connector_plugins()]).issuperset(
            {self.FILE_SOURCE_CONNECTOR, self.FILE_SINK_CONNECTOR})

        source_connector_props = self.render("connect-file-source.properties")
        sink_connector_props = self.render("connect-file-sink.properties")

        self.logger.info("Validating connector configurations")
        source_connector_config = self._config_dict_from_props(source_connector_props)
        configs = self.cc.validate_config(self.FILE_SOURCE_CONNECTOR, source_connector_config)
        self.verify_config(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_CONFIGS, configs)

        sink_connector_config = self._config_dict_from_props(sink_connector_props)
        configs = self.cc.validate_config(self.FILE_SINK_CONNECTOR, sink_connector_config)
        self.verify_config(self.FILE_SINK_CONNECTOR, self.FILE_SINK_CONFIGS, configs)

        self.logger.info("Creating connectors")
        self.cc.create_connector(source_connector_config)
        self.cc.create_connector(sink_connector_config)

        # We should see the connectors appear
        wait_until(lambda: set(self.cc.list_connectors()) == set(["local-file-source", "local-file-sink"]),
                   timeout_sec=10, err_msg="Connectors that were just created did not appear in connector listing")

        # We'll only do very simple validation that the connectors and tasks really ran.
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self.validate_output(self.INPUT_LIST), timeout_sec=120, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Trying to create the same connector again should cause an error
        try:
            self.cc.create_connector(self._config_dict_from_props(source_connector_props))
            assert False, "creating the same connector should have caused a conflict"
        except ConnectRestError:
            pass # expected

        # Validate that we can get info about connectors
        expected_source_info = {
            'name': 'local-file-source',
            'config': self._config_dict_from_props(source_connector_props),
            'tasks': [{'connector': 'local-file-source', 'task': 0}],
            'type': 'source'
        }
        source_info = self.cc.get_connector("local-file-source")
        assert expected_source_info == source_info, "Incorrect info:" + json.dumps(source_info)
        source_config = self.cc.get_connector_config("local-file-source")
        assert expected_source_info['config'] == source_config, "Incorrect config: " + json.dumps(source_config)
        expected_sink_info = {
            'name': 'local-file-sink',
            'config': self._config_dict_from_props(sink_connector_props),
            'tasks': [{'connector': 'local-file-sink', 'task': 0}],
            'type': 'sink'
        }
        sink_info = self.cc.get_connector("local-file-sink")
        assert expected_sink_info == sink_info, "Incorrect info:" + json.dumps(sink_info)
        sink_config = self.cc.get_connector_config("local-file-sink")
        assert expected_sink_info['config'] == sink_config, "Incorrect config: " + json.dumps(sink_config)

        # Validate that we can get info about tasks. This info should definitely be available now without waiting since
        # we've already seen data appear in files.
        # TODO: It would be nice to validate a complete listing, but that doesn't make sense for the file connectors
        expected_source_task_info = [{
            'id': {'connector': 'local-file-source', 'task': 0},
            'config': {
                'connector.class': 'org.apache.kafka.connect.file.FileStreamSourceConnector',
                'task.class': 'org.apache.kafka.connect.file.FileStreamSourceTask',
                'file': self.INPUT_FILE,
                'topic': self.TOPIC,
                'tasks.max': '1',
                'name': 'local-file-source',
                'errors.log.include.messages': 'true',
                'errors.tolerance': 'none',
                'errors.log.enable': 'true'
            }
        }]
        source_task_info = self.cc.get_connector_tasks("local-file-source")
        assert expected_source_task_info == source_task_info, "Incorrect info:" + json.dumps(source_task_info)
        expected_sink_task_info = [{
            'id': {'connector': 'local-file-sink', 'task': 0},
            'config': {
                'connector.class': 'org.apache.kafka.connect.file.FileStreamSinkConnector',
                'task.class': 'org.apache.kafka.connect.file.FileStreamSinkTask',
                'file': self.OUTPUT_FILE,
                'topics': self.TOPIC,
                'key.converter.schemas.enable': 'True',
                'tasks.max': '1',
                'name': 'local-file-sink',
                'value.converter.schemas.enable':'True',
                'errors.tolerance': 'none',
                'errors.log.enable': 'true',
                'errors.log.include.messages': 'true',
            }
        }]
        sink_task_info = self.cc.get_connector_tasks("local-file-sink")
        assert expected_sink_task_info == sink_task_info, "Incorrect info:" + json.dumps(sink_task_info)

        file_source_config = self._config_dict_from_props(source_connector_props)
        file_source_config['file'] = self.INPUT_FILE2
        self.cc.set_connector_config("local-file-source", file_source_config)

        # We should also be able to verify that the modified configs caused the tasks to move to the new file and pick up
        # more data.
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.LONER_INPUTS) + " >> " + self.INPUT_FILE2)
        wait_until(lambda: self.validate_output(self.LONGER_INPUT_LIST), timeout_sec=120, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        self.cc.delete_connector("local-file-source")
        self.cc.delete_connector("local-file-sink")
        wait_until(lambda: not self.cc.list_connectors(), timeout_sec=10, err_msg="Deleted connectors did not disappear from REST listing")

    def validate_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
        output_set = set(itertools.chain(*[
            [line.strip() for line in self.file_contents(node, self.OUTPUT_FILE)] for node in self.cc.nodes
            ]))
        return input_set == output_set

    def file_contents(self, node, file):
        try:
            # Convert to a list here or the RemoteCommandError may be returned during a call to the generator instead of
            # immediately
            return list(node.account.ssh_capture("cat " + file))
        except RemoteCommandError:
            return []

    def _config_dict_from_props(self, connector_props):
        return dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])

    def verify_config(self, name, config_def, configs):
        # Should have zero errors
        assert name == configs['name']
        # Should have zero errors
        assert 0 == configs['error_count']
        # Should return all configuration
        config_names = [config['definition']['name'] for config in configs['configs']]
        assert config_def == set(config_names)
