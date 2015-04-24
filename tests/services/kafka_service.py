# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.services.service import Service
import time, re, json


class KafkaService(Service):
    def __init__(self, service_context, zk, topics=None):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type zk: ZookeeperService
        :type topics: dict
        """
        super(KafkaService, self).__init__(service_context)
        self.zk = zk
        self.topics = topics

    def start(self):
        super(KafkaService, self).start()

        # Start all nodes in this Kafka service
        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Starting Kafka node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            self.start_node(node)

            # wait for start up
            time.sleep(6)

        # Create topics if necessary
        if self.topics is not None:
            for topic, topic_cfg in self.topics.items():
                if topic_cfg is None:
                    topic_cfg = {}

                topic_cfg["topic"] = topic
                self.create_topic(topic_cfg)

    def create_topic(self, topic_cfg):
        node = self.nodes[0] # any node is fine here
        self.logger.info("Creating topic %s with settings %s", topic_cfg["topic"], topic_cfg)

        cmd = "/opt/kafka/bin/kafka-topics.sh --zookeeper %(zk_connect)s --create "\
            "--topic %(topic)s --partitions %(partitions)d --replication-factor %(replication)d" % {
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

    def describe_topic(self, topic):
        node = self.nodes[0]
        cmd = "/opt/kafka/bin/kafka-topics.sh --zookeeper %s --topic %s --describe" % \
              (self.zk.connect_setting(), topic)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        return output

    def verify_reassign_partitions(self, reassignment):
        """Run the reassign partitions admin tool in "verify" mode
        """
        node = self.nodes[0]
        json_file = "/tmp/" + str(time.time()) + "_reassign.json"

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = "echo %s > %s && " % (json_str, json_file)
        cmd += "/opt/kafka/bin/kafka-reassign-partitions.sh "\
                "--zookeeper %(zk_connect)s "\
                "--reassignment-json-file %(reassignment_file)s "\
                "--verify" % {'zk_connect': self.zk.connect_setting(),
                                'reassignment_file': json_file}
        cmd += " && sleep 1 && rm -f %s" % json_file

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

    def execute_reassign_partitions(self, reassignment):
        """Run the reassign partitions admin tool in "verify" mode
        """
        node = self.nodes[0]
        json_file = "/tmp/" + str(time.time()) + "_reassign.json"

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = "echo %s > %s && " % (json_str, json_file)
        cmd += "/opt/kafka/bin/kafka-reassign-partitions.sh "\
                "--zookeeper %(zk_connect)s "\
                "--reassignment-json-file %(reassignment_file)s "\
                "--execute" % {'zk_connect': self.zk.connect_setting(),
                                'reassignment_file': json_file}
        cmd += " && sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Executing parition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug("Verify partition reassignment:")
        self.logger.debug(output)

    def stop(self):
        """If the service left any running processes or data, clean them up."""
        super(KafkaService, self).stop()

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
            self._stop_and_clean(node, allow_fail=True)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("/opt/kafka/bin/kafka-server-stop.sh", allow_fail=allow_fail)
        time.sleep(5)  # the stop script doesn't wait
        node.account.ssh("rm -rf /mnt/kafka-logs /mnt/kafka.properties /mnt/kafka.log")

    def stop_node(self, node, clean_shutdown=True, allow_fail=True):
        node.account.kill_process("kafka", clean_shutdown, allow_fail)

    def start_node(self, node, config=None):
        if config is None:
            template = open('templates/kafka.properties').read()
            template_params = {
                'broker_id': self.idx(node),
                'hostname': node.account.hostname,
                'zk_connect': self.zk.connect_setting()
            }

            config = template % template_params

        node.account.create_file("/mnt/kafka.properties", config)
        cmd = "/opt/kafka/bin/kafka-server-start.sh /mnt/kafka.properties 1>> /mnt/kafka.log 2>> /mnt/kafka.log &"
        self.logger.debug("Attempting to start KafkaService on %s with command: %s" % (str(node.account), cmd))
        node.account.ssh(cmd)

    def restart_node(self, node, wait_sec=0, clean_shutdown=True):
        self.stop_node(node, clean_shutdown, allow_fail=True)
        time.sleep(wait_sec)
        self.start_node(node)

    def get_leader_node(self, topic, partition=0):
        """ Get the leader replica for the given topic and partition.
        """
        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s " \
              % self.zk.connect_setting()
        cmd += "get /brokers/topics/%s/partitions/%d/state" % (topic, partition)
        self.logger.debug(cmd)

        node = self.nodes[0]
        self.logger.debug("Querying zookeeper to find leader replica for topic %s: \n%s" % (cmd, topic))
        partition_state = None
        for line in node.account.ssh_capture(cmd):
            match = re.match("^({.+})$", line)
            if match is not None:
                partition_state = match.groups()[0]
                break

        if partition_state is None:
            raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))

        partition_state = json.loads(partition_state)
        self.logger.info(partition_state)

        leader_idx = int(partition_state["leader"])
        self.logger.info("Leader for topic %s and partition %d is now: %d" % (topic, partition, leader_idx))
        return self.get_node(leader_idx)

    def bootstrap_servers(self):
        return ','.join([node.account.hostname + ":9092" for node in self.nodes])