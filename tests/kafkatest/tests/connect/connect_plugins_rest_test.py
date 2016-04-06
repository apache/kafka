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
from kafkatest.services.connect import ConnectDistributedService


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


class ConnectPluginsRestApiTest(KafkaTest):
    """
    Test of Kafka Connect's REST APIs for connector plugins.
    """
    OFFSETS_TOPIC = "connect-offsets"
    CONFIG_TOPIC = "connect-configs"
    STATUS_TOPIC = "connect-status"

    COMMON_GROUP = 'Common'
    # The value denote whether the config is required or not.
    COMMON_CONNECTOR_CONFIGS = {
        'name': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 1, 'width': 'MEDIUM'},
        'connector.class': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 2, 'width': 'LONG'},
        'task.max': {'required': False, 'type': 'INT', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 3, 'width': 'SHORT'},
        'topics': {'required': False, 'type': 'LIST', 'importance': 'HIGH', 'group': COMMON_GROUP, 'order': 4, 'width': 'LONG'}
    }

    FILE_SOURCE_CONFIGS = {
        'topic': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': None, 'order': -1, 'width': 'NONE'},
        'file': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': None, 'order': -1, 'width': 'NONE'}
    }

    FILE_SINK_CONFIGS = {
        'file': {'required': True, 'type': 'STRING', 'importance': 'HIGH', 'group': None, 'order': -1, 'width': 'NONE'}
    }

    FILE_SOURCE_ALL_CONFIGS = merge_two_dicts(COMMON_CONNECTOR_CONFIGS, FILE_SOURCE_CONFIGS)
    FILE_SINK_ALL_CONFIGS = merge_two_dicts(COMMON_CONNECTOR_CONFIGS, FILE_SINK_CONFIGS)

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SOURCE_CONNECTOR_SHORT = 'FileStreamSourceConnector'
    FILE_SOURCE_CONNECTOR_ALIAS = 'FileStreamSource'

    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'
    FILE_SINK_CONNECTOR_SHORT = 'FileStreamSinkConnector'
    FILE_SINK_CONNECTOR_ALIAS = 'FileStreamSink'

    def __init__(self, test_context):
        super(ConnectPluginsRestApiTest, self).__init__(test_context, num_zk=1, num_brokers=1)
        self.cc = ConnectDistributedService(test_context, 2, self.kafka)

    def test_rest_api(self):
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        expected = {self.FILE_SOURCE_CONNECTOR, self.FILE_SINK_CONNECTOR}
        connector_plugins = self.cc.list_connector_plugins()
        connector_plugins_set = set([connector_plugin['class'] for connector_plugin in connector_plugins])
        self.validate_plugins(expected, connector_plugins_set)
        self.validate_config_def(self.FILE_SOURCE_CONNECTOR, self.COMMON_CONNECTOR_CONFIGS, {})
        self.validate_config_def(self.FILE_SOURCE_CONNECTOR_SHORT, self.COMMON_CONNECTOR_CONFIGS, {})
        self.validate_config_def(self.FILE_SOURCE_CONNECTOR_ALIAS, self.COMMON_CONNECTOR_CONFIGS, {})

        self.validate_config_def(self.FILE_SINK_CONNECTOR, self.COMMON_CONNECTOR_CONFIGS, {})
        self.validate_config_def(self.FILE_SINK_CONNECTOR_SHORT, self.COMMON_CONNECTOR_CONFIGS, {})
        self.validate_config_def(self.FILE_SINK_CONNECTOR_ALIAS, self.COMMON_CONNECTOR_CONFIGS, {})

        # Test the case that we have invalid values in connector common configs
        props = {
            'name': 'test',
            'connector.class': self.FILE_SOURCE_CONNECTOR,
            'tasks.max': 0
        }

        expected_config_values = {
            'name': {'name': 'name', 'value': 'test', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SOURCE_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': 0, 'errors': ['Invalid value 0 for configuration tasks.max: Value must be at least 1'], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': [], 'errors': [], 'recommended_values': [], 'visible': True}
        }

        configs = self.validate_config_def(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_ALL_CONFIGS, props)
        actual_config_values = self.get_config_values(configs)
        self.validate_config_value(expected_config_values, actual_config_values)

        # Test the case that we have invalid values in connector common configs
        props = {
            'name': 'test',
            'connector.class': self.FILE_SINK_CONNECTOR,
            'tasks.max': 0
        }

        expected_config_values = {
            'name': {'name': 'name', 'value': 'test', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SINK_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': 0, 'errors': ['Invalid value 0 for configuration tasks.max: Value must be at least 1'], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': [], 'errors': [], 'recommended_values': [], 'visible': True}
        }

        configs = self.validate_config_def(self.FILE_SINK_CONNECTOR, self.FILE_SINK_ALL_CONFIGS, props)
        actual_config_values = self.get_config_values(configs)
        self.validate_config_value(expected_config_values, actual_config_values)

        # Test the case that we only provide the connector common configs, but the configs have
        # valid values
        props = {
            'name': 'test',
            'connector.class': self.FILE_SOURCE_CONNECTOR,
            'tasks.max': 1
        }

        expected_config_values = {
            'name': {'name': 'name', 'value': 'test', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SOURCE_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': 1, 'errors': [], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': [], 'errors': [], 'recommended_values': [], 'visible': True},
            'file': {'name': 'file', 'value': None, 'errors': ['Missing required configuration "file" which has no default value.'], 'recommended_values': [], 'visible': True},
            'topic': {'name': 'topic', 'value': None, 'errors': ['Missing required configuration "topic" which has no default value.'], 'recommended_values': [], 'visible': True}
        }

        configs = self.validate_config_def(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_ALL_CONFIGS, props)
        # Should only show errors of missing required configuration.
        assert 2 == configs['error_count']
        actual_config_values = self.get_config_values(configs)
        self.validate_config_value(expected_config_values, actual_config_values)

        # Test the case that we only provide the connector common configs, but the configs have
        # valid values
        props = {
            'name': 'test',
            'connector.class': self.FILE_SINK_CONNECTOR,
            'tasks.max': 1
        }

        expected_config_values = {
            'name': {'name': 'name', 'value': 'test', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SINK_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': 1, 'errors': [], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': [], 'errors': [], 'recommended_values': [], 'visible': True},
            'file': {'name': 'file', 'value': None, 'errors': ['Missing required configuration "file" which has no default value.'], 'recommended_values': [], 'visible': True},
        }

        configs = self.validate_config_def(self.FILE_SINK_CONNECTOR, self.FILE_SINK_ALL_CONFIGS, props)
        # Should only show errors of missing required configuration.
        assert 1 == configs['error_count']
        actual_config_values = self.get_config_values(configs)
        self.validate_config_value(expected_config_values, actual_config_values)

        # Test the case that all the configs are provided
        props = {
            'name': 'test',
            'connector.class': self.FILE_SOURCE_CONNECTOR,
            'tasks.max': 1,
            'topic': 'file-topic',
            'file': 'file.txt'
        }

        expected_config_values = {
            'name': {'name': 'name', 'value': 'test', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SOURCE_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': 1, 'errors': [], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': [], 'errors': [], 'recommended_values': [], 'visible': True},
            'file': {'name': 'file', 'value': 'file.txt', 'errors': [], 'recommended_values': [], 'visible': True},
            'topic': {'name': 'topic', 'value': 'file-topic', 'errors': [], 'recommended_values': [], 'visible': True}
        }
        configs = self.validate_config_def(self.FILE_SOURCE_CONNECTOR, self.FILE_SOURCE_ALL_CONFIGS, props)
        # Should have zero errors
        assert 0 == configs['error_count']
        actual_config_values = self.get_config_values(configs)
        self.validate_config_value(expected_config_values, actual_config_values)

        # Test the case that all the configs are provided
        props = {
            'name': 'test',
            'connector.class': self.FILE_SINK_CONNECTOR,
            'tasks.max': 1,
            'topics': 'file-topic',
            'file': 'file-sink.txt'
        }

        expected_config_values = {
            'name': {'name': 'name', 'value': 'test', 'errors': [], 'recommended_values': [], 'visible': True},
            'connector.class': {'name': 'connector.class', 'value': self.FILE_SINK_CONNECTOR, 'errors': [], 'recommended_values': [], 'visible': True},
            'tasks.max': {'name': 'tasks.max', 'value': 1, 'errors': [], 'recommended_values': [], 'visible': True},
            'topics': {'name': 'topics', 'value': ['file-topic'], 'errors': [], 'recommended_values': [], 'visible': True},
            'file': {'name': 'file', 'value': 'file-sink.txt', 'errors': [], 'recommended_values': [], 'visible': True},
        }
        configs = self.validate_config_def(self.FILE_SINK_CONNECTOR, self.FILE_SINK_ALL_CONFIGS, props)
        # Should have zero errors
        assert 0 == configs['error_count']
        actual_config_values = self.get_config_values(configs)
        self.validate_config_value(expected_config_values, actual_config_values)

    def validate_plugins(self, expected, actual):
        assert expected == actual

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

    def validate_config_value(self, expected, actual):
        assert expected == actual

    def get_config_values(self, configs):
        config_list = configs['configs']
        config_values = {}
        for config in config_list:
            config_def = config['definition']
            config_value = config['value']
            config_name = config_def['name']
            config_values[config_name] = config_value
        return config_values
