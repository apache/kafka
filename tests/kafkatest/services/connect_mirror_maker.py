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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin

"""
MirrorMaker 2.0 is a tool for mirroring data between two Kafka clusters.
"""

class ConnectMirrorMaker(KafkaPathResolverMixin, Service):

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/connect_mirror_maker"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "connect_mirror_maker.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    MM2_CONFIG = os.path.join(PERSISTENT_ROOT, "mm2.properties")

    logs = {
        "connect_mirror_maker_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, source, target, topics=".*", log_level="DEBUG"):
        """
        MirrorMaker mirrors messages from one or more source clusters to a single destination cluster.

        Args:
            context:                    standard context
            source:                     source Kafka cluster
            target:                     target Kafka cluster to which data will be mirrored
            topics:                     topics to replicate
        """
        super(ConnectMirrorMaker, self).__init__(context, num_nodes=num_nodes)
        self.log_level = log_level
        self.topics = topics
        self.source = source
        self.target = target
        self.external_jars = None

    def start_cmd(self, node):
        cmd = "export LOG_DIR=%s;" % ConnectMirrorMaker.LOG_DIR
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\";" % ConnectMirrorMaker.LOG4J_CONFIG
        cmd += " export KAFKA_OPTS=%s;" % self.security_config.kafka_opts
        # add external dependencies, for instance for interceptors
        if self.external_jars is not None:
            cmd += "for file in %s; do CLASSPATH=$CLASSPATH:$file; done; " % self.external_jars
            cmd += "export CLASSPATH; "
        cmd += " %s %s " % (self.path.script("kafka-run-class.sh", node),
                           self.java_class_name())
        cmd += ConnectMirrorMaker.MM2_CONFIG
        cmd += " 1>> %s 2>> %s &" % (ConnectMirrorMaker.LOG_FILE, ConnectMirrorMaker.LOG_FILE)
        return cmd

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % ConnectMirrorMaker.PERSISTENT_ROOT, allow_fail=False)
        node.account.ssh("mkdir -p %s" % ConnectMirrorMaker.LOG_DIR, allow_fail=False)

        self.security_config = self.source.security_config.client_config()
        self.security_config.setup_node(node)

        # Create, upload one consumer config file for source cluster
        mm2_props = self.render("mm2.properties")
        mm2_props += str(self.security_config)

        node.account.create_file(ConnectMirrorMaker.MM2_CONFIG, mm2_props)
        self.logger.info("MirrorMaker 2.0 config:\n" + mm2_props)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=ConnectMirrorMaker.LOG_FILE)
        node.account.create_file(ConnectMirrorMaker.LOG4J_CONFIG, log_config)

        # Run mirror maker
        cmd = self.start_cmd(node)
        self.logger.debug("MirrorMaker 2.0 command: %s", cmd)
        node.account.ssh(cmd, allow_fail=False)
        wait_until(lambda: self.alive(node), timeout_sec=30, backoff_sec=.5,
                   err_msg="MirrorMaker 2.0 took too long to start.")
        self.logger.debug("MirrorMaker 2.0 is alive")

    def stop_node(self, node, clean_shutdown=True):
        node.account.kill_java_processes(self.java_class_name(), allow_fail=True,
                                         clean_shutdown=clean_shutdown)
        wait_until(lambda: not self.alive(node), timeout_sec=30, backoff_sec=.5,
                   err_msg="MirrorMaker 2.0 took too long to stop.")

    def clean_node(self, node):
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_java_processes(self.java_class_name(), clean_shutdown=False,
                                         allow_fail=True)
        node.account.ssh("rm -rf %s" % ConnectMirrorMaker.PERSISTENT_ROOT, allow_fail=False)
        self.security_config.clean_node(node)

    def java_class_name(self):
        return "org.apache.kafka.connect.mirror.MirrorMaker"
