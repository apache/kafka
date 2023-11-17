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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService, config_property, quorum
from kafkatest.services.connect import ConnectDistributedService, VerifiableSource, VerifiableSink, ConnectRestError, MockSink, MockSource
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH, LATEST_2_3, LATEST_2_2, LATEST_2_1, LATEST_2_0, LATEST_1_1, LATEST_1_0, LATEST_0_11_0, LATEST_0_10_2, LATEST_0_10_1, LATEST_0_10_0, LATEST_0_9, LATEST_0_8_2, KafkaVersion

from functools import reduce
from collections import Counter, namedtuple
import itertools
import json
import operator
import time

class ConnectDistributedTest(Test):
    """
    Simple test of Kafka Connect in distributed mode, producing data from files on one cluster and consuming it on
    another, validating the total output is identical to the input.
    """

    FILE_SOURCE_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSourceConnector'
    FILE_SINK_CONNECTOR = 'org.apache.kafka.connect.file.FileStreamSinkConnector'

    INPUT_FILE = "/mnt/connect.input"
    OUTPUT_FILE = "/mnt/connect.output"

    TOPIC = "test"
    OFFSETS_TOPIC = "connect-offsets"
    OFFSETS_REPLICATION_FACTOR = "1"
    OFFSETS_PARTITIONS = "1"
    CONFIG_TOPIC = "connect-configs"
    CONFIG_REPLICATION_FACTOR = "1"
    STATUS_TOPIC = "connect-status"
    STATUS_REPLICATION_FACTOR = "1"
    STATUS_PARTITIONS = "1"
    EXACTLY_ONCE_SOURCE_SUPPORT = "disabled"
    SCHEDULED_REBALANCE_MAX_DELAY_MS = "60000"
    CONNECT_PROTOCOL="sessioned"

    # Since tasks can be assigned to any node and we're testing with files, we need to make sure the content is the same
    # across all nodes.
    FIRST_INPUT_LIST = ["foo", "bar", "baz"]
    FIRST_INPUTS = "\n".join(FIRST_INPUT_LIST) + "\n"
    SECOND_INPUT_LIST = ["razz", "ma", "tazz"]
    SECOND_INPUTS = "\n".join(SECOND_INPUT_LIST) + "\n"

    SCHEMA = { "type": "string", "optional": False }

    def __init__(self, test_context):
        super(ConnectDistributedTest, self).__init__(test_context)
        self.num_zk = 1
        self.num_brokers = 1
        self.topics = {
            self.TOPIC: {'partitions': 1, 'replication-factor': 1}
        }

        self.zk = ZookeeperService(test_context, self.num_zk) if quorum.for_test(test_context) == quorum.zk else None

        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

    def setup_services(self,
                       security_protocol=SecurityConfig.PLAINTEXT,
                       timestamp_type=None,
                       broker_version=DEV_BRANCH,
                       auto_create_topics=False,
                       include_filestream_connectors=False,
                       num_workers=3):
        self.kafka = KafkaService(self.test_context, self.num_brokers, self.zk,
                                  security_protocol=security_protocol, interbroker_security_protocol=security_protocol,
                                  topics=self.topics, version=broker_version,
                                  server_prop_overrides=[
                                      ["auto.create.topics.enable", str(auto_create_topics)],
                                      ["transaction.state.log.replication.factor", str(self.num_brokers)],
                                      ["transaction.state.log.min.isr", str(self.num_brokers)]
                                  ])
        if timestamp_type is not None:
            for node in self.kafka.nodes:
                node.config[config_property.MESSAGE_TIMESTAMP_TYPE] = timestamp_type

        self.cc = ConnectDistributedService(self.test_context, num_workers, self.kafka, [self.INPUT_FILE, self.OUTPUT_FILE],
                                            include_filestream_connectors=include_filestream_connectors)
        self.cc.log_level = "DEBUG"

        if self.zk:
            self.zk.start()
        self.kafka.start()

    def _start_connector(self, config_file):
        connector_props = self.render(config_file)
        connector_config = dict([line.strip().split('=', 1) for line in connector_props.split('\n') if line.strip() and not line.strip().startswith('#')])
        self.cc.create_connector(connector_config)
            
    def _connector_status(self, connector, node=None):
        try:
            return self.cc.get_connector_status(connector, node)
        except ConnectRestError:
            return None

    def _connector_has_state(self, status, state):
        return status is not None and status['connector']['state'] == state

    def _task_has_state(self, task_id, status, state):
        if not status:
            return False

        tasks = status['tasks']
        if not tasks:
            return False

        for task in tasks:
            if task['id'] == task_id:
                return task['state'] == state

        return False

    def _all_tasks_have_state(self, status, task_count, state):
        if status is None:
            return False

        tasks = status['tasks']
        if len(tasks) != task_count:
            return False

        return reduce(operator.and_, [task['state'] == state for task in tasks], True)

    def is_running(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'RUNNING') and self._all_tasks_have_state(status, connector.tasks, 'RUNNING')

    def is_paused(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'PAUSED') and self._all_tasks_have_state(status, connector.tasks, 'PAUSED')

    def connector_is_running(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'RUNNING')

    def connector_is_failed(self, connector, node=None):
        status = self._connector_status(connector.name, node)
        return self._connector_has_state(status, 'FAILED')

    def task_is_failed(self, connector, task_id, node=None):
        status = self._connector_status(connector.name, node)
        return self._task_has_state(task_id, status, 'FAILED')

    def task_is_running(self, connector, task_id, node=None):
        status = self._connector_status(connector.name, node)
        return self._task_has_state(task_id, status, 'RUNNING')

    @cluster(num_nodes=5)
    @matrix(exactly_once_source=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_restart_failed_connector(self, exactly_once_source, connect_protocol, metadata_quorum):
        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if exactly_once_source else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        if exactly_once_source:
            self.connector = MockSource(self.cc, mode='connector-failure', delay_sec=5)
        else:
            self.connector = MockSink(self.cc, self.topics.keys(), mode='connector-failure', delay_sec=5)
        self.connector.start()

        wait_until(lambda: self.connector_is_failed(self.connector), timeout_sec=15,
                   err_msg="Failed to see connector transition to the FAILED state")

        self.cc.restart_connector(self.connector.name)
        
        wait_until(lambda: self.connector_is_running(self.connector), timeout_sec=10,
                   err_msg="Failed to see connector transition to the RUNNING state")

    @cluster(num_nodes=5)
    @matrix(connector_type=['source', 'exactly-once source', 'sink'], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_restart_failed_task(self, connector_type, connect_protocol, metadata_quorum):
        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if connector_type == 'exactly-once source' else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        connector = None
        if connector_type == "sink":
            connector = MockSink(self.cc, self.topics.keys(), mode='task-failure', delay_sec=5)
        else:
            connector = MockSource(self.cc, mode='task-failure', delay_sec=5)
            
        connector.start()

        task_id = 0
        wait_until(lambda: self.task_is_failed(connector, task_id), timeout_sec=20,
                   err_msg="Failed to see task transition to the FAILED state")

        self.cc.restart_task(connector.name, task_id)
        
        wait_until(lambda: self.task_is_running(connector, task_id), timeout_sec=10,
                   err_msg="Failed to see task transition to the RUNNING state")

    @cluster(num_nodes=5)
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_restart_connector_and_tasks_failed_connector(self, connect_protocol, metadata_quorum):
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.sink = MockSink(self.cc, self.topics.keys(), mode='connector-failure', delay_sec=5)
        self.sink.start()

        wait_until(lambda: self.connector_is_failed(self.sink), timeout_sec=15,
                   err_msg="Failed to see connector transition to the FAILED state")

        self.cc.restart_connector_and_tasks(self.sink.name, only_failed = "true", include_tasks = "false")

        wait_until(lambda: self.connector_is_running(self.sink), timeout_sec=10,
                   err_msg="Failed to see connector transition to the RUNNING state")

    @cluster(num_nodes=5)
    @matrix(connector_type=['source', 'sink'], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_restart_connector_and_tasks_failed_task(self, connector_type, connect_protocol, metadata_quorum):
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        connector = None
        if connector_type == "sink":
            connector = MockSink(self.cc, self.topics.keys(), mode='task-failure', delay_sec=5)
        else:
            connector = MockSource(self.cc, mode='task-failure', delay_sec=5)

        connector.start()

        task_id = 0
        wait_until(lambda: self.task_is_failed(connector, task_id), timeout_sec=20,
                   err_msg="Failed to see task transition to the FAILED state")

        self.cc.restart_connector_and_tasks(connector.name, only_failed = "false", include_tasks = "true")

        wait_until(lambda: self.task_is_running(connector, task_id), timeout_sec=10,
                   err_msg="Failed to see task transition to the RUNNING state")

    @cluster(num_nodes=5)
    @matrix(exactly_once_source=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_pause_and_resume_source(self, exactly_once_source, connect_protocol, metadata_quorum):
        """
        Verify that source connectors stop producing records when paused and begin again after
        being resumed.
        """

        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if exactly_once_source else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.source = VerifiableSource(self.cc, topic=self.TOPIC)
        self.source.start()

        wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                   err_msg="Failed to see connector transition to the RUNNING state")
        
        self.cc.pause_connector(self.source.name)

        # wait until all nodes report the paused transition
        for node in self.cc.nodes:
            wait_until(lambda: self.is_paused(self.source, node), timeout_sec=30,
                       err_msg="Failed to see connector transition to the PAUSED state")

        # verify that we do not produce new messages while paused
        num_messages = len(self.source.sent_messages())
        time.sleep(10)
        assert num_messages == len(self.source.sent_messages()), "Paused source connector should not produce any messages"

        self.cc.resume_connector(self.source.name)

        for node in self.cc.nodes:
            wait_until(lambda: self.is_running(self.source, node), timeout_sec=30,
                       err_msg="Failed to see connector transition to the RUNNING state")

        # after resuming, we should see records produced again
        wait_until(lambda: len(self.source.sent_messages()) > num_messages, timeout_sec=30,
                   err_msg="Failed to produce messages after resuming source connector")

    @cluster(num_nodes=5)
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_pause_and_resume_sink(self, connect_protocol, metadata_quorum):
        """
        Verify that sink connectors stop consuming records when paused and begin again after
        being resumed.
        """

        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        # use the verifiable source to produce a steady stream of messages
        self.source = VerifiableSource(self.cc, topic=self.TOPIC)
        self.source.start()

        wait_until(lambda: len(self.source.committed_messages()) > 0, timeout_sec=30,
                   err_msg="Timeout expired waiting for source task to produce a message")

        self.sink = VerifiableSink(self.cc, topics=[self.TOPIC])
        self.sink.start()

        wait_until(lambda: self.is_running(self.sink), timeout_sec=30,
                   err_msg="Failed to see connector transition to the RUNNING state")
        
        self.cc.pause_connector(self.sink.name)

        # wait until all nodes report the paused transition
        for node in self.cc.nodes:
            wait_until(lambda: self.is_paused(self.sink, node), timeout_sec=30,
                       err_msg="Failed to see connector transition to the PAUSED state")

        # verify that we do not consume new messages while paused
        num_messages = len(self.sink.received_messages())
        time.sleep(10)
        assert num_messages == len(self.sink.received_messages()), "Paused sink connector should not consume any messages"

        self.cc.resume_connector(self.sink.name)

        for node in self.cc.nodes:
            wait_until(lambda: self.is_running(self.sink, node), timeout_sec=30,
                       err_msg="Failed to see connector transition to the RUNNING state")

        # after resuming, we should see records consumed again
        wait_until(lambda: len(self.sink.received_messages()) > num_messages, timeout_sec=30,
                   err_msg="Failed to consume messages after resuming sink connector")

    @cluster(num_nodes=5)
    @matrix(exactly_once_source=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_pause_state_persistent(self, exactly_once_source, connect_protocol, metadata_quorum):
        """
        Verify that paused state is preserved after a cluster restart.
        """

        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if exactly_once_source else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.source = VerifiableSource(self.cc, topic=self.TOPIC)
        self.source.start()

        wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                   err_msg="Failed to see connector transition to the RUNNING state")
        
        self.cc.pause_connector(self.source.name)

        self.cc.restart()

        # we should still be paused after restarting
        for node in self.cc.nodes:
            wait_until(lambda: self.is_paused(self.source, node), timeout_sec=120,
                       err_msg="Failed to see connector startup in PAUSED state")

    @cluster(num_nodes=5)
    def test_dynamic_logging(self):
        """
        Test out the REST API for dynamically adjusting logging levels, on both a single-worker and cluster-wide basis.
        """

        self.setup_services(num_workers=3)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        worker = self.cc.nodes[0]
        initial_loggers = self.cc.get_all_loggers(worker)
        self.logger.debug("Listed all loggers via REST API: %s", str(initial_loggers))
        assert initial_loggers is not None
        assert 'root' in initial_loggers
        # We need root and at least one other namespace (the other namespace is checked
        # later on to make sure that it hasn't changed)
        assert len(initial_loggers) >= 2
        # We haven't made any modifications yet; ensure that the last-modified timestamps
        # for all namespaces are null
        for logger in initial_loggers.values():
            assert logger['last_modified'] is None

        # Find a non-root namespace to adjust
        namespace = None
        for logger in initial_loggers.keys():
            if logger != 'root':
                namespace = logger
                break
        assert namespace is not None

        initial_level = self.cc.get_logger(worker, namespace)['level']
        # Make sure we pick a different one than what's already set for that namespace
        new_level = self._different_level(initial_level)
        request_time = self._set_logger(worker, namespace, new_level)

        # Verify that our adjustment was applied on the worker we issued the request to...
        assert self._loggers_are_set(new_level, request_time, namespace, workers=[worker])
        # ... and that no adjustments have been applied to the other workers in the cluster
        assert self._loggers_are_set(initial_level, None, namespace, workers=self.cc.nodes[1:])

        # Force all loggers to get updated by setting the root namespace to
        # two different levels
        # This guarantees that their last-modified times will be updated
        self._set_logger(worker, 'root', 'DEBUG', 'cluster')
        new_root = 'INFO'
        request_time = self._set_logger(worker, 'root', new_root, 'cluster')
        self._wait_for_loggers(new_root, request_time, 'root')

        new_level = 'DEBUG'
        request_time = self._set_logger(worker, namespace, new_level, 'cluster')
        self._wait_for_loggers(new_level, request_time, namespace)

        prior_all_loggers = [self.cc.get_all_loggers(node) for node in self.cc.nodes]
        # Set the same level twice for a namespace
        self._set_logger(worker, namespace, new_level, 'cluster')

        prior_namespace = namespace
        new_namespace = None
        for logger, level in prior_all_loggers[0].items():
            if logger != 'root' and not logger.startswith(namespace):
                new_namespace = logger
                new_level = self._different_level(level['level'])
        assert new_namespace is not None

        request_time = self._set_logger(worker, new_namespace, new_level, 'cluster')
        self._wait_for_loggers(new_level, request_time, new_namespace)

        # Verify that the last-modified timestamp and logging level of the prior namespace
        # has not changed since the second-most-recent adjustment for it (the most-recent
        # adjustment used the same level and should not have had any impact on level or
        # timestamp)
        new_all_loggers = [self.cc.get_all_loggers(node) for node in self.cc.nodes]
        assert len(prior_all_loggers) == len(new_all_loggers)
        for i in range(len(prior_all_loggers)):
            prior_loggers, new_loggers = prior_all_loggers[i], new_all_loggers[i]
            for logger, prior_level in prior_loggers.items():
                if logger.startswith(prior_namespace):
                    new_level = new_loggers[logger]
                    assert prior_level == new_level

        # Forcibly update all loggers in the cluster to a new level, bumping their
        # last-modified timestamps
        new_root = 'INFO'
        self._set_logger(worker, 'root', 'DEBUG', 'cluster')
        root_request_time = self._set_logger(worker, 'root', new_root, 'cluster')
        self._wait_for_loggers(new_root, root_request_time, 'root')
        # Track the loggers reported on every node
        prior_all_loggers = [self.cc.get_all_loggers(node) for node in self.cc.nodes]

        # Make a final worker-scoped logging adjustment
        namespace = new_namespace
        new_level = self._different_level(new_root)
        request_time = self._set_logger(worker, namespace, new_level, 'worker')
        assert self._loggers_are_set(new_level, request_time, namespace, workers=[worker])

        # Make sure no changes to loggers outside the affected namespace have taken place
        all_loggers = self.cc.get_all_loggers(worker)
        for logger, level in all_loggers.items():
            if not logger.startswith(namespace):
                assert level['level'] == new_root
                assert root_request_time <= level['last_modified'] < request_time

        # Verify that the last worker-scoped request we issued had no effect on other
        # workers in the cluster
        new_all_loggers = [self.cc.get_all_loggers(node) for node in self.cc.nodes]
        # Exclude the first node, which we've made worker-scope modifications to
        # since we last adjusted the cluster-scope root level
        assert prior_all_loggers[1:] == new_all_loggers[1:]

        # Restart a worker and ensure that all logging level adjustments (regardless of scope)
        # have been discarded
        self._restart_worker(worker)
        restarted_loggers = self.cc.get_all_loggers(worker)
        assert initial_loggers == restarted_loggers

    def _different_level(self, current_level):
        return 'INFO' if current_level is None or current_level.upper() != 'INFO' else 'WARN'

    def _set_logger(self, worker, namespace, new_level, scope=None):
        """
        Set a log level via the PUT /admin/loggers/{logger} endpoint, verify that the response
        has the expected format, and then return the time at which the request was issued.
        :param worker: the worker to issue the REST request to
        :param namespace: the logging namespace to adjust
        :param new_level: the new level for the namespace
        :param scope: the scope of the logging adjustment; if None, then no scope will be specified
        in the REST request
        :return: the time at or directly before which the REST request was made
        """
        request_time = int(time.time() * 1000)
        affected_loggers = self.cc.set_logger(worker, namespace, new_level, scope)
        if scope is not None and scope.lower() == 'cluster':
            assert affected_loggers is None
        else:
            assert len(affected_loggers) >= 1
            for logger in affected_loggers:
                assert logger.startswith(namespace)
        return request_time

    def _loggers_are_set(self, expected_level, last_modified, namespace, workers=None):
        """
        Verify that all loggers for a namespace (as returned from the GET /admin/loggers endpoint) have
        an expected level and last-modified timestamp.
        :param expected_level: the expected level for all loggers in the namespace
        :param last_modified: the expected last modified timestamp; if None, then all loggers
        are expected to have null timestamps; otherwise, all loggers are expected to have timestamps
        greater than or equal to this value
        :param namespace: the logging namespace to examine
        :param workers: the workers to query
        :return: whether the expected logging levels and last-modified timestamps are set
        """
        if workers is None:
            workers = self.cc.nodes
        for worker in workers:
            all_loggers = self.cc.get_all_loggers(worker)
            self.logger.debug("Read loggers on %s from Connect REST API: %s", str(worker), str(all_loggers))
            namespaced_loggers = {k: v for k, v in all_loggers.items() if k.startswith(namespace)}
            if len(namespaced_loggers) < 1:
                return False
            for logger in namespaced_loggers.values():
                if logger['level'] != expected_level:
                    return False
                if last_modified is None:
                    # Fail fast if there's a non-null timestamp; it'll never be reset to null
                    assert logger['last_modified'] is None
                elif logger['last_modified'] is None or logger['last_modified'] < last_modified:
                    return False
        return True

    def _wait_for_loggers(self, level, request_time, namespace, workers=None):
        wait_until(
            lambda: self._loggers_are_set(level, request_time, namespace, workers),
            # This should be super quick--just a write+read of the config topic, which workers are constantly polling
            timeout_sec=10,
            err_msg="Log level for namespace '" + namespace + "'  was not adjusted in a reasonable amount of time."
        )

    @cluster(num_nodes=6)
    @matrix(security_protocol=[SecurityConfig.PLAINTEXT, SecurityConfig.SASL_SSL], exactly_once_source=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_file_source_and_sink(self, security_protocol, exactly_once_source, connect_protocol, metadata_quorum):
        """
        Tests that a basic file connector works across clean rolling bounces. This validates that the connector is
        correctly created, tasks instantiated, and as nodes restart the work is rebalanced across nodes.
        """

        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if exactly_once_source else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(security_protocol=security_protocol, include_filestream_connectors=True)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        self.logger.info("Creating connectors")
        self._start_connector("connect-file-source.properties")
        self._start_connector("connect-file-sink.properties")
        
        # Generating data on the source node should generate new records and create new output on the sink node. Timeouts
        # here need to be more generous than they are for standalone mode because a) it takes longer to write configs,
        # do rebalancing of the group, etc, and b) without explicit leave group support, rebalancing takes awhile
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.FIRST_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST), timeout_sec=70, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

        # Restarting both should result in them picking up where they left off,
        # only processing new data.
        self.cc.restart()

        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.SECOND_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST + self.SECOND_INPUT_LIST), timeout_sec=150, err_msg="Sink output file never converged to the same state as the input file")

    @cluster(num_nodes=6)
    @matrix(clean=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_bounce(self, clean, connect_protocol, metadata_quorum):
        """
        Validates that source and sink tasks that run continuously and produce a predictable sequence of messages
        run correctly and deliver messages exactly once when Kafka Connect workers undergo clean rolling bounces,
        and at least once when workers undergo unclean bounces.
        """
        num_tasks = 3

        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.source = VerifiableSource(self.cc, topic=self.TOPIC, tasks=num_tasks, throughput=100)
        self.source.start()
        self.sink = VerifiableSink(self.cc, tasks=num_tasks, topics=[self.TOPIC])
        self.sink.start()

        for i in range(3):
            start = i % len(self.cc.nodes)
            # Don't want to restart worker nodes in the same order every time
            shuffled_nodes = self.cc.nodes[start:] + self.cc.nodes[:start]
            for node in shuffled_nodes:
                self._restart_worker(node, clean=clean)
                # Give additional time for the consumer groups to recover. Even if it is not a hard bounce, there are
                # some cases where a restart can cause a rebalance to take the full length of the session timeout
                # (e.g. if the client shuts down before it has received the memberId from its initial JoinGroup).
                # If we don't give enough time for the group to stabilize, the next bounce may cause consumers to 
                # be shut down before they have any time to process data and we can end up with zero data making it 
                # through the test.
                time.sleep(15)

        # Wait at least scheduled.rebalance.max.delay.ms to expire and rebalance
        time.sleep(60)

        # Allow the connectors to startup, recover, and exit cleanly before
        # ending the test. It's possible for the source connector to make
        # uncommitted progress, and for the sink connector to read messages that
        # have not been committed yet, and fail a later assertion.
        wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                   err_msg="Failed to see connector transition to the RUNNING state")
        time.sleep(15)
        self.source.stop()
        # Ensure that the sink connector has an opportunity to read all
        # committed messages from the source connector.
        wait_until(lambda: self.is_running(self.sink), timeout_sec=30,
                   err_msg="Failed to see connector transition to the RUNNING state")
        time.sleep(15)
        self.sink.stop()
        self.cc.stop()

        # Validate at least once delivery of everything that was reported as written since we should have flushed and
        # cleanly exited. Currently this only tests at least once delivery for sinks because the task may not have consumed
        # all the messages generated by the source task. This needs to be done per-task since seqnos are not unique across
        # tasks.
        success = True
        errors = []
        allow_dups = not clean
        src_messages = self.source.committed_messages()
        sink_messages = self.sink.flushed_messages()
        for task in range(num_tasks):
            # Validate source messages
            src_seqnos = [msg['seqno'] for msg in src_messages if msg['task'] == task]
            # Every seqno up to the largest one we ever saw should appear. Each seqno should only appear once because clean
            # bouncing should commit on rebalance.
            src_seqno_max = max(src_seqnos) if len(src_seqnos) else 0
            self.logger.debug("Max source seqno: %d", src_seqno_max)
            src_seqno_counts = Counter(src_seqnos)
            missing_src_seqnos = sorted(set(range(src_seqno_max)).difference(set(src_seqnos)))
            duplicate_src_seqnos = sorted(seqno for seqno,count in src_seqno_counts.items() if count > 1)

            if missing_src_seqnos:
                self.logger.error("Missing source sequence numbers for task " + str(task))
                errors.append("Found missing source sequence numbers for task %d: %s" % (task, missing_src_seqnos))
                success = False
            if not allow_dups and duplicate_src_seqnos:
                self.logger.error("Duplicate source sequence numbers for task " + str(task))
                errors.append("Found duplicate source sequence numbers for task %d: %s" % (task, duplicate_src_seqnos))
                success = False


            # Validate sink messages
            sink_seqnos = [msg['seqno'] for msg in sink_messages if msg['task'] == task]
            # Every seqno up to the largest one we ever saw should appear. Each seqno should only appear once because
            # clean bouncing should commit on rebalance.
            sink_seqno_max = max(sink_seqnos) if len(sink_seqnos) else 0
            self.logger.debug("Max sink seqno: %d", sink_seqno_max)
            sink_seqno_counts = Counter(sink_seqnos)
            missing_sink_seqnos = sorted(set(range(sink_seqno_max)).difference(set(sink_seqnos)))
            duplicate_sink_seqnos = sorted(seqno for seqno,count in iter(sink_seqno_counts.items()) if count > 1)

            if missing_sink_seqnos:
                self.logger.error("Missing sink sequence numbers for task " + str(task))
                errors.append("Found missing sink sequence numbers for task %d: %s" % (task, missing_sink_seqnos))
                success = False
            if not allow_dups and duplicate_sink_seqnos:
                self.logger.error("Duplicate sink sequence numbers for task " + str(task))
                errors.append("Found duplicate sink sequence numbers for task %d: %s" % (task, duplicate_sink_seqnos))
                success = False

            # Validate source and sink match
            if sink_seqno_max > src_seqno_max:
                self.logger.error("Found sink sequence number greater than any generated sink sequence number for task %d: %d > %d", task, sink_seqno_max, src_seqno_max)
                errors.append("Found sink sequence number greater than any generated sink sequence number for task %d: %d > %d" % (task, sink_seqno_max, src_seqno_max))
                success = False
            if src_seqno_max < 1000 or sink_seqno_max < 1000:
                errors.append("Not enough messages were processed: source:%d sink:%d" % (src_seqno_max, sink_seqno_max))
                success = False

        if not success:
            self.mark_for_collect(self.cc)
            # Also collect the data in the topic to aid in debugging
            consumer_validator = ConsoleConsumer(self.test_context, 1, self.kafka, self.source.topic, consumer_timeout_ms=1000, print_key=True)
            consumer_validator.run()
            self.mark_for_collect(consumer_validator, "consumer_stdout")

        assert success, "Found validation errors:\n" + "\n  ".join(errors)

    @cluster(num_nodes=6)
    @matrix(clean=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_exactly_once_source(self, clean, connect_protocol, metadata_quorum):
        """
        Validates that source tasks run correctly and deliver messages exactly once
        when Kafka Connect workers undergo bounces, both clean and unclean.
        """
        num_tasks = 3

        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.source = VerifiableSource(self.cc, topic=self.TOPIC, tasks=num_tasks, throughput=100, complete_records=True)
        self.source.start()

        for i in range(4):
            start = i % len(self.cc.nodes)
            # Don't want to restart worker nodes in the same order every time
            shuffled_nodes = self.cc.nodes[start:] + self.cc.nodes[:start]
            for node in shuffled_nodes:
                self._restart_worker(node, clean=clean)

                if i < 2:
                    # Give additional time for the worker group to recover. Even if it is not a hard bounce, there are
                    # some cases where a restart can cause a rebalance to take the full length of the session timeout
                    # (e.g. if the client shuts down before it has received the memberId from its initial JoinGroup).
                    # If we don't give enough time for the group to stabilize, the next bounce may cause workers to
                    # be shut down before they have any time to process data and we can end up with zero data making it
                    # through the test.
                    time.sleep(15)
                else:
                    # We also need to make sure that, even without time for the cluster to recover gracefully in between
                    # worker restarts, the cluster and its tasks do not get into an inconsistent state and either duplicate or
                    # drop messages.
                    pass

        # Wait at least scheduled.rebalance.max.delay.ms to expire and rebalance
        time.sleep(60)

        # It's possible that a zombie fencing request from a follower to the leader failed when we bounced the leader
        # We don't automatically retry these requests because some failures (such as insufficient ACLs for the
        # connector's principal) are genuine and need to be reported by failing the task and displaying an error message
        # in the status for the task in the REST API.
        # So, we make a polite request to the cluster to restart any failed tasks
        self.cc.restart_connector_and_tasks(self.source.name, only_failed='true', include_tasks='true')

        # Allow the connectors to startup, recover, and exit cleanly before
        # ending the test.
        wait_until(lambda: self.is_running(self.source), timeout_sec=30,
                   err_msg="Failed to see connector transition to the RUNNING state")
        time.sleep(15)
        self.source.stop()
        self.cc.stop()

        consumer = ConsoleConsumer(self.test_context, 1, self.kafka, self.source.topic, message_validator=json.loads, consumer_timeout_ms=1000, isolation_level="read_committed")
        consumer.run()
        src_messages = consumer.messages_consumed[1]

        success = True
        errors = []
        for task in range(num_tasks):
            # Validate source messages
            src_seqnos = [msg['payload']['seqno'] for msg in src_messages if msg['payload']['task'] == task]
            if not src_seqnos:
                self.logger.error("No records produced by task " + str(task))
                errors.append("No records produced by task %d" % (task))
                success = False
                continue
            # Every seqno up to the largest one we ever saw should appear. Each seqno should only appear once because clean
            # bouncing should commit on rebalance.
            src_seqno_max = max(src_seqnos)
            self.logger.debug("Max source seqno: %d", src_seqno_max)
            src_seqno_counts = Counter(src_seqnos)
            missing_src_seqnos = sorted(set(range(src_seqno_max)).difference(set(src_seqnos)))
            duplicate_src_seqnos = sorted(seqno for seqno,count in src_seqno_counts.items() if count > 1)

            if missing_src_seqnos:
                self.logger.error("Missing source sequence numbers for task " + str(task))
                errors.append("Found missing source sequence numbers for task %d: %s" % (task, missing_src_seqnos))
                success = False
            if duplicate_src_seqnos:
                self.logger.error("Duplicate source sequence numbers for task " + str(task))
                errors.append("Found duplicate source sequence numbers for task %d: %s" % (task, duplicate_src_seqnos))
                success = False

        if not success:
            self.mark_for_collect(self.cc)
            # Also collect the data in the topic to aid in debugging
            consumer_validator = ConsoleConsumer(self.test_context, 1, self.kafka, self.source.topic, consumer_timeout_ms=1000, print_key=True)
            consumer_validator.run()
            self.mark_for_collect(consumer_validator, "consumer_stdout")

        assert success, "Found validation errors:\n" + "\n  ".join(errors)

    @cluster(num_nodes=6)
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'], metadata_quorum=quorum.all_non_upgrade)
    def test_transformations(self, connect_protocol, metadata_quorum):
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(timestamp_type='CreateTime', include_filestream_connectors=True)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        ts_fieldname = 'the_timestamp'

        NamedConnector = namedtuple('Connector', ['name'])

        source_connector = NamedConnector(name='file-src')

        self.cc.create_connector({
            'name': source_connector.name,
            'connector.class': 'org.apache.kafka.connect.file.FileStreamSourceConnector',
            'tasks.max': 1,
            'file': self.INPUT_FILE,
            'topic': self.TOPIC,
            'transforms': 'hoistToStruct,insertTimestampField',
            'transforms.hoistToStruct.type': 'org.apache.kafka.connect.transforms.HoistField$Value',
            'transforms.hoistToStruct.field': 'content',
            'transforms.insertTimestampField.type': 'org.apache.kafka.connect.transforms.InsertField$Value',
            'transforms.insertTimestampField.timestamp.field': ts_fieldname,
        })

        wait_until(lambda: self.connector_is_running(source_connector), timeout_sec=30, err_msg='Failed to see connector transition to the RUNNING state')

        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.FIRST_INPUTS) + " >> " + self.INPUT_FILE)

        consumer = ConsoleConsumer(self.test_context, 1, self.kafka, self.TOPIC, consumer_timeout_ms=15000, print_timestamp=True)
        consumer.run()

        assert len(consumer.messages_consumed[1]) == len(self.FIRST_INPUT_LIST)

        expected_schema = {
            'type': 'struct',
            'fields': [
                {'field': 'content', 'type': 'string', 'optional': False},
                {'field': ts_fieldname, 'name': 'org.apache.kafka.connect.data.Timestamp', 'type': 'int64', 'version': 1, 'optional': True},
            ],
            'optional': False
        }

        for msg in consumer.messages_consumed[1]:
            (ts_info, value) = msg.split('\t')

            assert ts_info.startswith('CreateTime:')
            ts = int(ts_info[len('CreateTime:'):])

            obj = json.loads(value)
            assert obj['schema'] == expected_schema
            assert obj['payload']['content'] in self.FIRST_INPUT_LIST
            assert obj['payload'][ts_fieldname] == ts

    @cluster(num_nodes=5)
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_2_3), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_2_2), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_2_1), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_2_0), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_1_1), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_1_0), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, exactly_once_source=True, connect_protocol='sessioned')
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, exactly_once_source=False, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_10_2), auto_create_topics=False, exactly_once_source=False, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_10_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_10_0), auto_create_topics=True, exactly_once_source=False, connect_protocol='sessioned')
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_3), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_2), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_1_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_1_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_10_2), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_10_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_10_0), auto_create_topics=True, exactly_once_source=False, connect_protocol='compatible')
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_3), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_2), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_1_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_1_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_10_2), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_10_1), auto_create_topics=False, exactly_once_source=False, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_10_0), auto_create_topics=True, exactly_once_source=False, connect_protocol='eager')
    def test_broker_compatibility(self, broker_version, auto_create_topics, exactly_once_source, connect_protocol):
        """
        Verify that Connect will start up with various broker versions with various configurations. 
        When Connect distributed starts up, it either creates internal topics (v0.10.1.0 and after) 
        or relies upon the broker to auto-create the topics (v0.10.0.x and before).
        """
        self.EXACTLY_ONCE_SOURCE_SUPPORT = 'enabled' if exactly_once_source else 'disabled'
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(broker_version=KafkaVersion(broker_version), auto_create_topics=auto_create_topics,
                            security_protocol=SecurityConfig.PLAINTEXT, include_filestream_connectors=True)
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))

        self.cc.start()

        self.logger.info("Creating connectors")
        self._start_connector("connect-file-source.properties")
        self._start_connector("connect-file-sink.properties")

        # Generating data on the source node should generate new records and create new output on the sink node. Timeouts
        # here need to be more generous than they are for standalone mode because a) it takes longer to write configs,
        # do rebalancing of the group, etc, and b) without explicit leave group support, rebalancing takes awhile
        for node in self.cc.nodes:
            node.account.ssh("echo -e -n " + repr(self.FIRST_INPUTS) + " >> " + self.INPUT_FILE)
        wait_until(lambda: self._validate_file_output(self.FIRST_INPUT_LIST), timeout_sec=70, err_msg="Data added to input file was not seen in the output file in a reasonable amount of time.")

    def _validate_file_output(self, input):
        input_set = set(input)
        # Output needs to be collected from all nodes because we can't be sure where the tasks will be scheduled.
        # Between the first and second rounds, we might even end up with half the data on each node.
        output_set = set(itertools.chain(*[
            [line.strip() for line in self._file_contents(node, self.OUTPUT_FILE)] for node in self.cc.nodes
        ]))
        return input_set == output_set

    def _file_contents(self, node, file):
        try:
            # Convert to a list here or the RemoteCommandError may be returned during a call to the generator instead of
            # immediately
            return list(node.account.ssh_capture("cat " + file))
        except RemoteCommandError:
            return []

    def _restart_worker(self, node, clean=True):
        started = time.time()
        self.logger.info("%s bouncing Kafka Connect on %s", clean and "Clean" or "Hard", str(node.account))
        self.cc.stop_node(node, clean_shutdown=clean, await_shutdown=True)
        with node.account.monitor_log(self.cc.LOG_FILE) as monitor:
            self.cc.start_node(node)
            monitor.wait_until("Starting connectors and tasks using config offset", timeout_sec=90,
                               err_msg="Kafka Connect worker didn't successfully join group and start work")
        self.logger.info("Bounced Kafka Connect on %s and rejoined in %f seconds", node.account, time.time() - started)
