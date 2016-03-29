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

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.services.kafka.directory import kafka_dir


class SimpleConsumerShell(BackgroundThreadService):

    logs = {
        "simple_consumer_shell_log": {
            "path": "/mnt/simple_consumer_shell.log",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, kafka, topic, partition=0):
        super(SimpleConsumerShell, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.topic = topic
        self.partition = partition
        self.output = ""

    def _worker(self, idx, node):
        cmd = self.start_cmd(node)
        self.logger.debug("SimpleConsumerShell %d command: %s" % (idx, cmd))
        self.output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            self.output += line
        self.logger.debug(self.output)

    def start_cmd(self, node):
        cmd = "/opt/%s/bin/" % kafka_dir(node)
        cmd += "kafka-run-class.sh kafka.tools.SimpleConsumerShell"
        cmd += " --topic %s --broker-list %s --partition %s --no-wait-at-logend" % (self.topic, self.kafka.bootstrap_servers(), self.partition)

        cmd += " 2>> /mnt/get_simple_consumer_shell.log | tee -a /mnt/get_simple_consumer_shell.log &"
        return cmd

    def get_output(self):
        return self.output

    def stop_node(self, node):
        node.account.kill_process("SimpleConsumerShell", allow_fail=False)
        if self.worker_threads is None:
            return

        # block until the corresponding thread exits
        if len(self.worker_threads) >= self.idx(node):
            # Need to guard this because stop is preemptively called before the worker threads are added and started
            self.worker_threads[self.idx(node) - 1].join()

    def clean_node(self, node):
        node.account.kill_process("SimpleConsumerShell", clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf /mnt/simple_consumer_shell.log", allow_fail=False)
