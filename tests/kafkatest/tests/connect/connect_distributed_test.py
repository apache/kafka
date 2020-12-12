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
from kafkatest.services.kafka import KafkaService, config_property
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

        self.zk = ZookeeperService(test_context, self.num_zk)

        self.key_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.value_converter = "org.apache.kafka.connect.json.JsonConverter"
        self.schemas = True

    def setup_services(self, security_protocol=SecurityConfig.PLAINTEXT, timestamp_type=None, broker_version=DEV_BRANCH, auto_create_topics=False):
        self.kafka = KafkaService(self.test_context, self.num_brokers, self.zk,
                                  security_protocol=security_protocol, interbroker_security_protocol=security_protocol,
                                  topics=self.topics, version=broker_version,
                                  server_prop_overides=[["auto.create.topics.enable", str(auto_create_topics)]])
        if timestamp_type is not None:
            for node in self.kafka.nodes:
                node.config[config_property.MESSAGE_TIMESTAMP_TYPE] = timestamp_type

        self.cc = ConnectDistributedService(self.test_context, 3, self.kafka, [self.INPUT_FILE, self.OUTPUT_FILE])
        self.cc.log_level = "DEBUG"

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
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_restart_failed_connector(self, connect_protocol):
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services()
        self.cc.set_configs(lambda node: self.render("connect-distributed.properties", node=node))
        self.cc.start()

        self.sink = MockSink(self.cc, self.topics.keys(), mode='connector-failure', delay_sec=5)
        self.sink.start()

        wait_until(lambda: self.connector_is_failed(self.sink), timeout_sec=15,
                   err_msg="Failed to see connector transition to the FAILED state")

        self.cc.restart_connector(self.sink.name)
        
        wait_until(lambda: self.connector_is_running(self.sink), timeout_sec=10,
                   err_msg="Failed to see connector transition to the RUNNING state")

    @cluster(num_nodes=5)
    @matrix(connector_type=['source', 'sink'], connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_restart_failed_task(self, connector_type, connect_protocol):
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
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_pause_and_resume_source(self, connect_protocol):
        """
        Verify that source connectors stop producing records when paused and begin again after
        being resumed.
        """

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
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_pause_and_resume_sink(self, connect_protocol):
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
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_pause_state_persistent(self, connect_protocol):
        """
        Verify that paused state is preserved after a cluster restart.
        """

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

    @cluster(num_nodes=6)
    @matrix(security_protocol=[SecurityConfig.PLAINTEXT, SecurityConfig.SASL_SSL], connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_file_source_and_sink(self, security_protocol, connect_protocol):
        """
        Tests that a basic file connector works across clean rolling bounces. This validates that the connector is
        correctly created, tasks instantiated, and as nodes restart the work is rebalanced across nodes.
        """

        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(security_protocol=security_protocol)
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
    @matrix(clean=[True, False], connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_bounce(self, clean, connect_protocol):
        """
        Validates that source and sink tasks that run continuously and produce a predictable sequence of messages
        run correctly and deliver messages exactly once when Kafka Connect workers undergo clean rolling bounces.
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

        for _ in range(3):
            for node in self.cc.nodes:
                started = time.time()
                self.logger.info("%s bouncing Kafka Connect on %s", clean and "Clean" or "Hard", str(node.account))
                self.cc.stop_node(node, clean_shutdown=clean)
                with node.account.monitor_log(self.cc.LOG_FILE) as monitor:
                    self.cc.start_node(node)
                    monitor.wait_until("Starting connectors and tasks using config offset", timeout_sec=90,
                                       err_msg="Kafka Connect worker didn't successfully join group and start work")
                self.logger.info("Bounced Kafka Connect on %s and rejoined in %f seconds", node.account, time.time() - started)

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
        # cleanly exited. Currently this only tests at least once delivery because the sink task may not have consumed
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
    @matrix(connect_protocol=['sessioned', 'compatible', 'eager'])
    def test_transformations(self, connect_protocol):
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(timestamp_type='CreateTime')
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
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_10_2), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_10_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='sessioned')
    @parametrize(broker_version=str(LATEST_0_10_0), auto_create_topics=True, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='sessioned')
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_3), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_2), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_2_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_1_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_1_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_10_2), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_10_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(LATEST_0_10_0), auto_create_topics=True, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='compatible')
    @parametrize(broker_version=str(DEV_BRANCH), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_3), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_2), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_2_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_1_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_1_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_11_0), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_10_2), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_10_1), auto_create_topics=False, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    @parametrize(broker_version=str(LATEST_0_10_0), auto_create_topics=True, security_protocol=SecurityConfig.PLAINTEXT, connect_protocol='eager')
    def test_broker_compatibility(self, broker_version, auto_create_topics, security_protocol, connect_protocol):
        """
        Verify that Connect will start up with various broker versions with various configurations. 
        When Connect distributed starts up, it either creates internal topics (v0.10.1.0 and after) 
        or relies upon the broker to auto-create the topics (v0.10.0.x and before).
        """
        self.CONNECT_PROTOCOL = connect_protocol
        self.setup_services(broker_version=KafkaVersion(broker_version), auto_create_topics=auto_create_topics, security_protocol=security_protocol)
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
