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


from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import quorum
from kafkatest.services.streams import StreamsTestBaseService
from kafkatest.tests.kafka_test import KafkaTest


class StreamsRelationalSmokeTestService(StreamsTestBaseService):
    def __init__(self, test_context, kafka, mode, nodeId, processing_guarantee):
        super(StreamsRelationalSmokeTestService, self).__init__(
            test_context,
            kafka,
            "ignore",
            "ignore"
        )
        self.mode = mode
        self.nodeId = nodeId
        self.processing_guarantee = processing_guarantee
        self.log4j_template = 'log4j_template.properties'

    def start_cmd(self, node):
        return "( export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j)s\"; " \
               "INCLUDE_TEST_JARS=true %(kafka_run_class)s org.apache.kafka.streams.tests.RelationalSmokeTest " \
               " %(mode)s %(kafka)s %(nodeId)s %(processing_guarantee)s %(state_dir)s" \
               " & echo $! >&3 ) 1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s" % {
                   "log4j": self.LOG4J_CONFIG_FILE,
                   "kafka_run_class": self.path.script("kafka-run-class.sh", node),
                   "mode": self.mode,
                   "kafka": self.kafka.bootstrap_servers(),
                   "nodeId": self.nodeId,
                   "processing_guarantee": self.processing_guarantee,
                   "state_dir": self.PERSISTENT_ROOT,
                   "stdout": self.STDOUT_FILE,
                   "stderr": self.STDERR_FILE,
                   "pidfile": self.PID_FILE
               }

    def start_node(self, node):
        node.account.mkdirs(self.PERSISTENT_ROOT)
        node.account.create_file(self.LOG4J_CONFIG_FILE,
                                 self.render("log4j_template.properties", log_file=self.LOG_FILE))

        self.logger.info("Starting process on " + str(node.account))
        node.account.ssh(self.start_cmd(node))

        if not self.pids(node):
            raise RuntimeError("No process ids recorded")

    def await_command(self, command):
        wait_until(lambda: self.node.account.ssh(command, allow_fail=True),
                   timeout_sec=60,
                   err_msg="Command [%s] never passed in the timeout"
                   )


class StreamsRelationalSmokeTest(KafkaTest):
    """
    Simple test of Kafka Streams.
    """

    def __init__(self, test_context):
        super(StreamsRelationalSmokeTest, self).__init__(test_context, num_zk=1, num_brokers=3, topics={
            'in-article': {'partitions': 3, 'replication-factor': 1},
            'in-comment': {'partitions': 5, 'replication-factor': 1},
            'out-augmented-article': {'partitions': 3, 'replication-factor': 1},
            'out-augmented-comment': {'partitions': 5, 'replication-factor': 1}
        })
        self.test_context = test_context

    @cluster(num_nodes=8)
    @matrix(crash=[False, True],
            processing_guarantee=['exactly_once', 'exactly_once_v2'],
            metadata_quorum=[quorum.isolated_kraft])
    def test_streams(self, crash, processing_guarantee, metadata_quorum):
        driver = StreamsRelationalSmokeTestService(self.test_context, self.kafka, "driver", "ignored", "ignored")

        LOG_FILE = driver.LOG_FILE  # this is the same for all instances of the service, so we can just declare a "constant"

        processor1 = StreamsRelationalSmokeTestService(self.test_context, self.kafka, "application", "processor1", processing_guarantee)
        processor2 = StreamsRelationalSmokeTestService(self.test_context, self.kafka, "application", "processor2", processing_guarantee)

        processor1.start()
        processor2.start()

        processor1.await_command("grep -q 'Streams has started' %s" % LOG_FILE)
        processor2.await_command("grep -q 'Streams has started' %s" % LOG_FILE)

        driver.start()

        # wait for at least one output of stream processing before injecting failures
        driver.await_command("grep -q 'Consumed first Augmented' %s" % LOG_FILE)

        processor1.stop_nodes(not crash)

        processor3 = StreamsRelationalSmokeTestService(self.test_context, self.kafka, "application", "processor3", processing_guarantee)
        processor3.start()
        processor3.await_command("grep -q 'Streams has started' %s" % LOG_FILE)

        processor2.stop_nodes(not crash)

        try:
            driver.node.account.ssh("! grep -q 'Smoke test complete' %s" % LOG_FILE, allow_fail=False)
        except:
            self.logger.info("Streams completed smoke test processing before the scenario was complete." +
                             " Increase the produce duration in RelationalSmokeTest.main().")

        driver.wait()

        driver.node.account.ssh("grep 'Smoke test complete: passed' %s" % driver.LOG_FILE, allow_fail=False)

        driver.stop()

        # the test is over, and the node is going to be cleaned, so there's no need to wait for a clean shutdown.
        processor3.stop_nodes(clean_shutdown = False)
