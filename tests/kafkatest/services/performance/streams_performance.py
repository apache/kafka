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

#
# Class used to start the simple Kafka Streams benchmark
#
class StreamsSimpleBenchmarkService(KafkaPathResolverMixin, Service):
    """Base class for simple Kafka Streams benchmark"""

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

    def __init__(self, context, kafka, numrecs):
        super(StreamsSimpleBenchmarkService, self).__init__(context, 1)
        self.kafka = kafka
        self.numrecs = numrecs

    @property
    def node(self):
        return self.nodes[0]

    def pids(self, node):
        try:
            return [pid for pid in node.account.ssh_capture("cat " + self.PID_FILE, callback=int)]
        except:
            return []

    def stop_node(self, node, clean_shutdown=True):
        self.logger.info((clean_shutdown and "Cleanly" or "Forcibly") + " stopping SimpleBenchmark on " + str(node.account))
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=True)
        if clean_shutdown:
            for pid in pids:
                wait_until(lambda: not node.account.alive(pid), timeout_sec=60, err_msg="SimpleBenchmark process on " + str(node.account) + " took too long to exit")

        node.account.ssh("rm -f " + self.PID_FILE, allow_fail=False)

    def wait(self):
        for node in self.nodes:
            for pid in self.pids(node):
                wait_until(lambda: not node.account.alive(pid), timeout_sec=600, err_msg="SimpleBenchmark process on " + str(node.account) + " took too long to exit")

    def clean_node(self, node):
        node.account.kill_process("streams", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)

    def start_cmd(self, node):
        args = {}
        args['kafka'] = self.kafka.bootstrap_servers()
        args['zk'] = self.kafka.zk.connect_setting()
        args['state_dir'] = self.PERSISTENT_ROOT
        args['numrecs'] = self.numrecs
        args['stdout'] = self.STDOUT_FILE
        args['stderr'] = self.STDERR_FILE
        args['pidfile'] = self.PID_FILE
        args['log4j'] = self.LOG4J_CONFIG_FILE
        args['kafka_run_class'] = self.path.script("kafka-run-class.sh", node)

        cmd = "( export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%(log4j)s\"; " \
              "INCLUDE_TEST_JARS=true %(kafka_run_class)s org.apache.kafka.streams.perf.SimpleBenchmark " \
              " %(kafka)s %(zk)s %(state_dir)s %(numrecs)s " \
              " & echo $! >&3 ) 1>> %(stdout)s 2>> %(stderr)s 3> %(pidfile)s" % args

        return cmd

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)

        node.account.create_file(self.LOG4J_CONFIG_FILE, self.render('tools_log4j.properties', log_file=self.LOG_FILE))

        self.logger.info("Starting SimpleBenchmark process on " + str(node.account))
        results = {}
        with node.account.monitor_log(self.STDOUT_FILE) as monitor:
            node.account.ssh(self.start_cmd(node))
            monitor.wait_until('SimpleBenchmark instance started', timeout_sec=15, err_msg="Never saw message indicating SimpleBenchmark finished startup on " + str(node.account))

        if len(self.pids(node)) == 0:
            raise RuntimeError("No process ids recorded")

    def collect_data(self, node):
        # Collect the data and return it to the framework
        output = node.account.ssh_capture("grep Performance %s" % self.STDOUT_FILE)
        data = {}
        for line in output:
            parts = line.split(':')
            data[parts[0]] = float(parts[1])
        return data
