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

import os

from ducktape.services.background_thread import BackgroundThreadService

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin, CORE_LIBS_JAR_NAME, CORE_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH

class LogCompactionTester(KafkaPathResolverMixin, BackgroundThreadService):

    OUTPUT_DIR = "/mnt/logcompaction_tester"
    LOG_PATH = os.path.join(OUTPUT_DIR, "logcompaction_tester_stdout.log")
    VERIFICATION_STRING = "Data verification is completed"

    logs = {
        "tool_logs": {
            "path": LOG_PATH,
            "collect_default": True}
    }

    def __init__(self, context, kafka, security_protocol="PLAINTEXT", stop_timeout_sec=30, tls_version=None):
        super(LogCompactionTester, self).__init__(context, 1)

        self.kafka = kafka
        self.security_protocol = security_protocol
        self.tls_version = tls_version
        self.security_config = SecurityConfig(self.context, security_protocol, tls_version=tls_version)
        self.stop_timeout_sec = stop_timeout_sec
        self.log_compaction_completed = False

    def _worker(self, idx, node):
        node.account.ssh("mkdir -p %s" % LogCompactionTester.OUTPUT_DIR)
        cmd = self.start_cmd(node)
        self.logger.info("LogCompactionTester %d command: %s" % (idx, cmd))
        self.security_config.setup_node(node)
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("Checking line:{}".format(line))

            if line.startswith(LogCompactionTester.VERIFICATION_STRING):
                self.log_compaction_completed = True

    def start_cmd(self, node):
        core_libs_jar = self.path.jar(CORE_LIBS_JAR_NAME, DEV_BRANCH)
        core_dependant_test_libs_jar = self.path.jar(CORE_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)

        cmd = "for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_libs_jar
        cmd += " for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_dependant_test_libs_jar
        cmd += " export CLASSPATH;"
        cmd += self.path.script("kafka-run-class.sh", node)
        cmd += " %s" % self.java_class_name()
        cmd += " --bootstrap-server %s --messages 1000000 --sleep 20 --duplicates 10 --percent-deletes 10" % (self.kafka.bootstrap_servers(self.security_protocol))

        cmd += " 2>> %s | tee -a %s &" % (self.logs["tool_logs"]["path"], self.logs["tool_logs"]["path"])
        return cmd

    def stop_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=True,
                                         allow_fail=True)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False,
                                         allow_fail=True)
        node.account.ssh("rm -rf %s" % LogCompactionTester.OUTPUT_DIR, allow_fail=False)

    def java_class_name(self):
        return "kafka.tools.LogCompactionTester"

    @property
    def is_done(self):
        return self.log_compaction_completed
