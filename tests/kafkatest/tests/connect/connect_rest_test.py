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
from kafkatest.services.connect import ConnectDistributedService, ConnectRestError
from ducktape.utils.util import wait_until
import subprocess
import json
import itertools


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


class ConnectRestApiTest(KafkaTest):
    """
    Test of Kafka Connect's REST API endpoints.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    COMMON_GROUP = 'Common'
    # The value denote whether the config is required or not.
    COMMON_CONNECTOR_CONFIGS = {
        'name': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 1, 'width': 'MEDIUM'},
        'connector.class': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 2, 'width': 'LONG'},
        'task.max': {'required': False, 'type': 'INT', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 3, 'width': 'SHORT'}
    }

    FILE_SOURCE_CONFIGS = {
        'topic': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': None, 'order': -1, 'width': 'NONE'},
        'file': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': None, 'order': -1, 'width': 'NONE'}
    }

    FILE_SINK_CONFIGS = {
        'topics': {'required': False, 'type': 'LIST', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 4, 'width': 'LONG'},
        'file': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': None, 'order': -1, 'width': 'NONE'}
    }

    FILE_SOURCE_ALL_CONFIGS = merge_two_dicts(COMMON_CONNECTOR_CONFIGS, FILE_SOURCE_CONFIGS)
    FILE_SINK_ALL_CONFIGS = merge_two_dicts(COMMON_CONNECTOR_CONFIGS, FILE_SINK_CONFIGS)

    INPUT_FILE = "/mnt/connect.input"
    INPUT_FILE2 = "/mnt/connect.input2"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    OFFSETS_TOPIC = "connect-offsets"
    CONFIG_TOPIC = "connect-configs"
    STATUS_TOPIC = "connect-status"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    INPUT_LIST = ["foo", "bar", "baz"]
    INPUTS = "\n".join(INPUT_LIST) + "\n"
    LONGER_INPUT_LIST = ["foo", "bar", "baz", "razz", "ma", "tazz"]
    LONER_INPUTS = "\n".join(LONGER_INPUT_LIST) + "\n"

    SCHEMA = {"type": "string", "optional": False}

    def __init__(self, test_context):
        super(ConnectRestApiTest, self).__init__(test_context, num_zk=1, num_brokers=1, topics={
            'test': {'partitions': 1, 'replication-factor': 1}
        })

        self.cc = ConnectDistributedService(test_context, 2, self.kafka, [self.INPUT_FILE, self.INPUT_FILE2, self.OUTPUT_FILE])

    def test_rest_api(self):
        # Template parameters
        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        assert self.cc.list_connectors() == []

        assert set([connector_plugin['class'] for connector_plugin in self.cc.list_connector_plugins()]) == {self.FILE_SOURCE_CONNECTOR, self.FILE_SINK_CONNECTOR}

        source_connector_props = self.render("connect-file-source.properties")
        sink_connector_props = self.render("connect-file-sink.properties")

        source_connector_config = self._config_dict_from_props(source_connector_props)
        expected_source_config_values = {
            'name': {'name': 'name', 'value': 'local-file-source', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SOURCE_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': '1', 'errors': [], 'recommended_values': [], 'visible': True},
            'file': {'name': 'file', 'value': self.INPUT_FILE, 'errors': [], 'recommended_values': [], 'visible': True},
            'topic': {'name': 'topic', 'value': self.TOPIC, 'errors': [], 'recommended_values': [], 'visible': True}
        }
        configs = self.validate_config_def(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_ALL_CONFIGS, source_connector_config)
        # Should have zero errors
        assert 0 == configs['error_count']
        actual_config_values = self.get_config_values(configs)
        assert expected_source_config_values == actual_config_values

        sink_connector_config = self._config_dict_from_props(sink_connector_props)
        expected_sink_config_values = {
            'name': {'name': 'name', 'value': 'local-file-sink', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SINK_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': '1', 'errors': [], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': self.TOPIC, 'errors': [], 'recommended_values': [], 'visible': True},
            'file': {'name': 'file', 'value': self.OUTPUT_FILE, 'errors': [], 'recommended_values': [], 'visible': True},
        }
        configs = self.validate_config_def(self.FILE_SINK_CONNECTOR, self.FILE_SINK_ALL_CONFIGS, sink_connector_config)
        # Should have zero errors
        assert 0 == configs['error_count']
        actual_config_values = self.get_config_values(configs)
        assert expected_sink_config_values == actual_config_values

        self.logger.info("Creating connectors")
        for connector_props in [source_connector_props, sink_connector_props]:
            connector_config = self._config_dict_from_props(connector_props)
            self.cc.create_connector(connector_config, retries=120, retry_backoff=1)

        # We should see the connectors appear
        wait_until(lambda: set(self.cc.list_connectors(retries=5, retry_backoff=1)) == set(["local-file-source", "local-file-sink"]),
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
            'tasks': [{ 'connector': 'local-file-source', 'task': 0 }]
        }
        source_info = self.cc.get_connector("local-file-source")
        assert expected_source_info == source_info, "Incorrect info:" + json.dumps(source_info)
        source_config = self.cc.get_connector_config("local-file-source")
        assert expected_source_info['config'] == source_config, "Incorrect config: " + json.dumps(source_config)
        expected_sink_info = {
            'name': 'local-file-sink',
            'config': self._config_dict_from_props(sink_connector_props),
            'tasks': [{'connector': 'local-file-sink', 'task': 0 }]
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
                'task.class': 'org.apache.kafka.connect.file.FileStreamSourceTask',
                'file': self.INPUT_FILE,
                'topic': self.TOPIC
            }
        }]
        source_task_info = self.cc.get_connector_tasks("local-file-source")
        assert expected_source_task_info == source_task_info, "Incorrect info:" + json.dumps(source_task_info)
        expected_sink_task_info = [{
            'id': {'connector': 'local-file-sink', 'task': 0},
            'config': {
                'task.class': 'org.apache.kafka.connect.file.FileStreamSinkTask',
                'file': self.OUTPUT_FILE,
                'topics': self.TOPIC
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

        self.cc.delete_connector("local-file-source", retries=120, retry_backoff=1)
        self.cc.delete_connector("local-file-sink", retries=120, retry_backoff=1)
        wait_until(lambda: len(self.cc.list_connectors(retries=5, retry_backoff=1)) == 0, timeout_sec=10, err_msg="Deleted connectors did not disappear from REST listing")

    def validate_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
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

    def _config_dict_from_props(self, connector_props):
        return dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])

    def validate_config_def(self, connector_type, expected_configs, config_values):
        configs = self.cc.validate_config(connector_type, config_values)
        config_list = configs['configs']
        name = configs['name']
        error_count = configs['error_count']

        assert name == connector_type

        total_errors = 0
        for config in config_list:
            total_errors += len(config['value']['errors'])

        assert error_count == total_errors

        for config in config_list:
            if config in expected_configs.keys():
                config_def = config['definition']
                config_name = config_def['name']
                assert expected_configs[config_name]['required'] == config_def['required']
                assert expected_configs[config_name]['type'] == config_def['type']
                assert expected_configs[config_name]['importance'] == config_def['importance']
                assert expected_configs[config_name]['group'] == config_def['group']
                assert expected_configs[config_name]['order'] == config_def['order']
                assert expected_configs[config_name]['width'] == config_def['width']
        return configs

    def get_config_values(self, configs):
        config_list = configs['configs']
        config_values = {}
        for config in config_list:
            config_def = config['definition']
            config_value = config['value']
            config_name = config_def['name']
            config_values[config_name] = config_value
        return config_values
