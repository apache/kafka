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
import signal

from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin

class ConsoleProducer(KafkaPathResolverMixin, Service):

    PERSISTENT_ROOT = "/mnt"
    CLIENT_PROPERTIES = os.path.join(PERSISTENT_ROOT, "client.properties")
    STDOUT = os.path.join(PERSISTENT_ROOT, "stdout.log")
    STDERR = os.path.join(PERSISTENT_ROOT, "stderr.log")
    STDIN = os.path.join(PERSISTENT_ROOT, "stdin.log")

    logs = {
        "stdout.log": {
            "path": STDOUT,
            "collect_default": True},
        "stderr.log": {
            "path": STDERR,
            "collect_default": True},
        "stdin.log": {
            "path": STDIN,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic,
                 client_id="console-producer", producer_properties={}, jaas_override_variables=None,
                 client_prop_file_override=""):
        super(ConsoleProducer, self).__init__(context=context, num_nodes=num_nodes)
        self.kafka = kafka
        self.topic = topic
        self.client_id = client_id
        self.producer_properties = producer_properties
        self.jaas_override_variables = jaas_override_variables
        self.client_prop_file_override = client_prop_file_override

    def prop_file(self, node):
        """Return a string which can be used to create a configuration file appropriate for the given node."""
        # Process client configuration
        prop_file = self.render('console_producer.properties')

        # Add security properties to the config. If security protocol is not specified,
        # use the default in the template properties.
        self.security_config = self.kafka.security_config.client_config(prop_file, node, self.jaas_override_variables)
        self.security_config.setup_node(node)

        prop_file += str(self.security_config)
        return prop_file

    def path_to_script(self, node):
        """Allow subclasses to override which producer script to run"""
        return self.path.script("kafka-console-producer.sh", node)

    @property
    def process_name(self):
        """Allow subclasses to override what their producer process looks like"""
        return "ConsoleProducer"

    def start_cmd(self, node):
        if self.client_prop_file_override:
            prop_file = self.client_prop_file_override
        else:
            prop_file = self.prop_file(node)

        self.logger.info(prop_file)
        node.account.create_file(self.CLIENT_PROPERTIES, prop_file)
        node.account.create_file(self.STDIN, "")

        cmd = self.path_to_script(node)
        cmd += " --broker-list %s" % self.kafka.bootstrap_servers(protocol=self.kafka.security_protocol)
        cmd += " --topic %s" % self.topic
        cmd += " --producer.config %s" % self.CLIENT_PROPERTIES

        if self.producer_properties is not None:
            for k, v in self.producer_properties.items():
                cmd += " --producer-property %s=%s" % (k, v)

        cmd += " >%s 2>%s <%s &" % (self.STDOUT, self.STDERR, self.STDIN)
        return cmd

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i %s | grep -v grep | awk '{print $1}'" % self.process_name
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def start_node(self, node):
        node.account.ssh(self.start_cmd(node))

    def produce(self, node, messages):
        # NOTE: newlines delimit messages
        node.account.ssh("echo \"%s\" >> %s" %
            ("\n".join(messages).replace('"', '\\"'), self.STDIN))

    def stop_node(self, node):
        try:
            for pid in self.pids(node):
                # NOTE: we need sudo access to kill the nohup process
                node.account.ssh("sudo kill -%d %d" % (signal.SIGTERM.value, pid))
            wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=90, err_msg="producer node failed to stop")
        except Exception as e:
            self.logger.error("error stopping service %s" % e)

    def clean_node(self, node):
        node.account.ssh("sudo rm %s %s %s %s" % (self.CLIENT_PROPERTIES, self.STDOUT, self.STDERR, self.STDIN))