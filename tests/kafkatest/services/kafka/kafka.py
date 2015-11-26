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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from config import KafkaConfig
from kafkatest.services.kafka import config_property
from kafkatest.services.kafka.version import TRUNK
from kafkatest.services.kafka.directory import kafka_dir, KAFKA_TRUNK

from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.security.minikdc import MiniKdc
import json
import re
import signal
import subprocess
import time
import os.path

class KafkaService(JmxMixin, Service):

    PERSISTENT_ROOT = "/mnt"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT, "kafka.log")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "kafka.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "kafka-log4j.properties")
    # Logs such as controller.log, server.log, etc all go here
    OPERATIONAL_LOG_DIR = os.path.join(PERSISTENT_ROOT, "kafka-operational-logs")
    # Kafka log segments etc go here
    DATA_LOG_DIR = os.path.join(PERSISTENT_ROOT, "kafka-data-logs")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "kafka.properties")


    logs = {
        "kafka_operational_logs": {
            "path": OPERATIONAL_LOG_DIR,
            "collect_default": True},
        "kafka_data": {
            "path": DATA_LOG_DIR,
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, zk, security_protocol=SecurityConfig.PLAINTEXT, interbroker_security_protocol=SecurityConfig.PLAINTEXT,
                 sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI, topics=None, version=TRUNK, quota_config=None, jmx_object_names=None, jmx_attributes=[]):
        """
        :type context
        :type zk: ZookeeperService
        :type topics: dict
        """
        Service.__init__(self, context, num_nodes)
        JmxMixin.__init__(self, num_nodes, jmx_object_names, jmx_attributes)
        self.log_level = "DEBUG"

        self.zk = zk
        self.quota_config = quota_config

        self.security_protocol = security_protocol
        self.interbroker_security_protocol = interbroker_security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.topics = topics
        self.minikdc = None

        for node in self.nodes:
            node.version = version
            node.config = KafkaConfig(**{config_property.BROKER_ID: self.idx(node)})

    @property
    def security_config(self):
        return SecurityConfig(self.security_protocol, self.interbroker_security_protocol, sasl_mechanism=self.sasl_mechanism)

    def start(self):
        if self.security_config.has_sasl_kerberos:
            if self.minikdc is None:
                self.minikdc = MiniKdc(self.context, self.nodes)
                self.minikdc.start()
        Service.start(self)

        # Create topics if necessary
        if self.topics is not None:
            for topic, topic_cfg in self.topics.items():
                if topic_cfg is None:
                    topic_cfg = {}

                topic_cfg["topic"] = topic
                self.create_topic(topic_cfg)

    def prop_file(self, node):
        cfg = KafkaConfig(**node.config)
        cfg[config_property.ADVERTISED_HOSTNAME] = node.account.hostname
        cfg[config_property.ZOOKEEPER_CONNECT] = self.zk.connect_setting()

        # TODO - clean up duplicate configuration logic
        prop_file = cfg.render()
        prop_file += self.render('kafka.properties', node=node, broker_id=self.idx(node),
                                  security_config=self.security_config, 
                                  interbroker_security_protocol=self.interbroker_security_protocol,
                                  sasl_mechanism=self.sasl_mechanism)
        return prop_file

    def start_cmd(self, node):
        cmd = "export JMX_PORT=%d; " % self.jmx_port
        cmd += "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % self.LOG4J_CONFIG
        cmd += "export LOG_DIR=%s; " % KafkaService.OPERATIONAL_LOG_DIR
        cmd += "export KAFKA_OPTS=%s; " % self.security_config.kafka_opts
        cmd += "/opt/" + kafka_dir(node) + "/bin/kafka-server-start.sh %s 1>> %s 2>> %s &" % (KafkaService.CONFIG_FILE, KafkaService.STDOUT_CAPTURE, KafkaService.STDERR_CAPTURE)
        return cmd

    def start_node(self, node):
        prop_file = self.prop_file(node)
        self.logger.info("kafka.properties:")
        self.logger.info(prop_file)
        node.account.create_file(KafkaService.CONFIG_FILE, prop_file)
        node.account.create_file(self.LOG4J_CONFIG, self.render('log4j.properties', log_dir=KafkaService.OPERATIONAL_LOG_DIR))

        self.security_config.setup_node(node)

        cmd = self.start_cmd(node)
        self.logger.debug("Attempting to start KafkaService on %s with command: %s" % (str(node.account), cmd))
        with node.account.monitor_log(KafkaService.STDOUT_CAPTURE) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until("Kafka Server.*started", timeout_sec=30, err_msg="Kafka server didn't finish startup")

        self.start_jmx_tool(self.idx(node), node)
        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % str(node))

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i kafka | grep java | grep -v grep | awk '{print $1}'"

            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def signal_node(self, node, sig=signal.SIGTERM):
        pids = self.pids(node)
        for pid in pids:
            node.account.signal(pid, sig)

    def signal_leader(self, topic, partition=0, sig=signal.SIGTERM):
        leader = self.leader(topic, partition)
        self.signal_node(leader, sig)

    def stop_node(self, node, clean_shutdown=True):
        pids = self.pids(node)
        sig = signal.SIGTERM if clean_shutdown else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=False)
        wait_until(lambda: len(self.pids(node)) == 0, timeout_sec=20, err_msg="Kafka node failed to stop")

    def clean_node(self, node):
        JmxMixin.clean_node(self, node)
        self.security_config.clean_node(node)
        node.account.kill_process("kafka", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/*", allow_fail=False)

    def create_topic(self, topic_cfg, node=None):
        """Run the admin tool create topic command.
        Specifying node is optional, and may be done if for different kafka nodes have different versions,
        and we care where command gets run.

        If the node is not specified, run the command from self.nodes[0]
        """
        if node is None:
            node = self.nodes[0]
        self.logger.info("Creating topic %s with settings %s", topic_cfg["topic"], topic_cfg)

        cmd = "/opt/%s/bin/kafka-topics.sh " % kafka_dir(node)
        cmd += "--zookeeper %(zk_connect)s --create --topic %(topic)s --partitions %(partitions)d --replication-factor %(replication)d" % {
                'zk_connect': self.zk.connect_setting(),
                'topic': topic_cfg.get("topic"),
                'partitions': topic_cfg.get('partitions', 1),
                'replication': topic_cfg.get('replication-factor', 1)
            }

        if "configs" in topic_cfg.keys() and topic_cfg["configs"] is not None:
            for config_name, config_value in topic_cfg["configs"].items():
                cmd += " --config %s=%s" % (config_name, str(config_value))

        self.logger.info("Running topic creation command...\n%s" % cmd)
        node.account.ssh(cmd)

        time.sleep(1)
        self.logger.info("Checking to see if topic was properly created...\n%s" % cmd)
        for line in self.describe_topic(topic_cfg["topic"]).split("\n"):
            self.logger.info(line)

    def describe_topic(self, topic, node=None):
        if node is None:
            node = self.nodes[0]
        cmd = "/opt/%s/bin/kafka-topics.sh --zookeeper %s --topic %s --describe" % \
              (kafka_dir(node), self.zk.connect_setting(), topic)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        return output

    def verify_reassign_partitions(self, reassignment, node=None):
        """Run the reassign partitions admin tool in "verify" mode
        """
        if node is None:
            node = self.nodes[0]

        json_file = "/tmp/%s_reassign.json" % str(time.time())

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = "echo %s > %s && " % (json_str, json_file)
        cmd += "/opt/%s/bin/kafka-reassign-partitions.sh " % kafka_dir(node)
        cmd += "--zookeeper %s " % self.zk.connect_setting()
        cmd += "--reassignment-json-file %s " % json_file
        cmd += "--verify "
        cmd += "&& sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Verifying parition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug(output)

        if re.match(".*is in progress.*", output) is not None:
            return False

        return True

    def execute_reassign_partitions(self, reassignment, node=None):
        """Run the reassign partitions admin tool in "verify" mode
        """
        if node is None:
            node = self.nodes[0]
        json_file = "/tmp/%s_reassign.json" % str(time.time())

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = "echo %s > %s && " % (json_str, json_file)
        cmd += "/opt/%s/bin/kafka-reassign-partitions.sh " % kafka_dir(node)
        cmd += "--zookeeper %s " % self.zk.connect_setting()
        cmd += "--reassignment-json-file %s " % json_file
        cmd += "--execute"
        cmd += " && sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Executing parition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug("Verify partition reassignment:")
        self.logger.debug(output)

    def restart_node(self, node, clean_shutdown=True):
        """Restart the given node."""
        self.stop_node(node, clean_shutdown)
        self.start_node(node)

    def leader(self, topic, partition=0):
        """ Get the leader replica for the given topic and partition.
        """
        kafka_dir = KAFKA_TRUNK
        cmd = "/opt/%s/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s " %\
              (kafka_dir, self.zk.connect_setting())
        cmd += "get /brokers/topics/%s/partitions/%d/state" % (topic, partition)
        self.logger.debug(cmd)

        node = self.zk.nodes[0]
        self.logger.debug("Querying zookeeper to find leader replica for topic %s: \n%s" % (cmd, topic))
        partition_state = None
        for line in node.account.ssh_capture(cmd):
            # loop through all lines in the output, but only hold on to the first match
            if partition_state is None:
                match = re.match("^({.+})$", line)
                if match is not None:
                    partition_state = match.groups()[0]

        if partition_state is None:
            raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))

        partition_state = json.loads(partition_state)
        self.logger.info(partition_state)

        leader_idx = int(partition_state["leader"])
        self.logger.info("Leader for topic %s and partition %d is now: %d" % (topic, partition, leader_idx))
        return self.get_node(leader_idx)

    def bootstrap_servers(self):
        """Return comma-delimited list of brokers in this cluster formatted as HOSTNAME1:PORT1,HOSTNAME:PORT2,...

        This is the format expected by many config files.
        """
        return ','.join([node.account.hostname + ":9092" for node in self.nodes])
