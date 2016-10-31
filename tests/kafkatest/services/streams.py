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

import os.path
import signal

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin


class StreamsSmokeTestBaseService(KafkaPathResolverMixin, Service):
    """Base class for Streams Smoke Test services providing some common settings and functionality"""

    PERSISTENT_ROOT = "/mnt/streams"
    # The log file contains normal log4j logs written using a file appender. stdout and stderr are handled separately
    LOG_FILE = os.path.join(PERSISTENT_ROOT, "streams.log")
    STDOUT_FILE = os.path.join(PERSISTENT_ROOT, "streams.stdout")
    STDERR_FILE = os.path.join(PERSISTENT_ROOT, "streams.stderr")
    LOG4J_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    PID_FILE = os.path.join(PERSISTENT_ROOT, "streams.pid")

    logs = {
        "streams_log": {
            "path": LOG_FILE,
            "collect_default": True},
        "streams_stdout": {
            "path": STDOUT_FILE,
            "collect_default": True},
        "streams_stderr": {
            "path": STDERR_FILE,
            "collect_default": True},
    }

    def __init__(self, context, kafka, command):
        super(StreamsSmokeTestBaseService, self).__init__(context, 1)
        self.kafka = kafka
        self.args = {'command': command}

    @property
    def node(self):
        return self.nodes[0]

    def pids(self, node):
        try:
            return [pid for pid in node.account.ssh_capture("cat " + self.PID_FILE, callback=int)]
        except:
            return []

    def stop_node(self, node, clean_shutdown=True):
        self.logger.info((clean_shutdown and "Cleanly" or "Forcibly") + " stopping Streams Smoke Test on " + str(node.account))
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=True)
        if clean_shutdown:
            for pid in pids:
                wait_until(lambda: not node.account.alive(pid), timeout_sec=60, err_msg="Streams Smoke Test process on " + str(node.account) + " took too long to exit")

        node.account.ssh("rm -f " + self.PID_FILE, allow_fail=False)

    def restart(self):
        # We don't want to do any clean up here, just restart the process.
        for node in self.nodes:
            self.logger.info("Restarting Kafka Streams on " + str(node.account))
            self.stop_node(node)
            self.start_node(node)

    def abortThenRestart(self):
        # We don't want to do any clean up here, just abort then restart the process. The running service is killed immediately.
        for node in self.nodes:
            self.logger.info("Aborting Kafka Streams on " + str(node.account))
            self.stop_node(node, False)
            self.logger.info("Restarting Kafka Streams on " + str(node.account))
            self.start_node(node)

    def wait(self):
        for node in self.nodes:
            for pid in self.pids(node):
                wait_until(lambda: not node.account.alive(pid), timeout_sec=180, err_msg="Streams Smoke Test process on " + str(node.account) + " took too long to exit")

    def clean_node(self, node):
        node.account.kill_process("streams", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)

    def start_cmd(self, node):
        args = self.args.copy()
        args['kafka'] = self.kafka.bootstrap_servers()
        args['zk'] = self.kafka.zk.connect_setting()
        args['state_dir'] = self.PERSISTENT_ROOT
        args['stdout'] = self.STDOUT_FILE
        args['stderr'] = self.STDERR_FILE
        args['pidfile'] = self.PID_FILE
        args['log4j'] = self.LOG4J_CONFIG_FILE
        args['kafka_run_class'] = self.path.script("kafka-run-class.sh", node)

        cmd = "( export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j)s\"; " \
              "INCLUDE_TEST_JARS=true %(kafka_run_class)s org.apache.kafka.streams.smoketest.StreamsSmokeTest " \
              " %(command)s %(kafka)s %(zk)s %(state_dir)s " \
              " & echo $! >&3 ) 1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s" % args

        return cmd

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)

        node.account.create_file(self.LOG4J_CONFIG_FILE, self.render('tools_log4j.properties', log_file=self.LOG_FILE))

        self.logger.info("Starting StreamsSmokeTest process on " + str(node.account))
        with node.account.monitor_log(self.STDOUT_FILE) as monitor:
            node.account.ssh(self.start_cmd(node))
            monitor.wait_until('StreamsSmokeTest instance started', timeout_sec=15, err_msg="Never saw message indicating StreamsSmokeTest finished startup on " + str(node.account))

        if len(self.pids(node)) == 0:
            raise RuntimeError("No process ids recorded")


class StreamsSmokeTestDriverService(StreamsSmokeTestBaseService):
    def __init__(self, context, kafka):
        super(StreamsSmokeTestDriverService, self).__init__(context, kafka, "run")


class StreamsSmokeTestJobRunnerService(StreamsSmokeTestBaseService):
    def __init__(self, context, kafka):
        super(StreamsSmokeTestJobRunnerService, self).__init__(context, kafka, "process")
