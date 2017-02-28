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


import re
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH


class ZookeeperService(KafkaPathResolverMixin, Service):

    logs = {
        "zk_log": {
            "path": "/mnt/zk.log",
            "collect_default": True},
        "zk_data": {
            "path": "/mnt/zookeeper",
            "collect_default": False}
    }

    def __init__(self, context, num_nodes, zk_sasl = False):
        """
        :type context
        """
        self.kafka_opts = ""
        self.zk_sasl = zk_sasl
        super(ZookeeperService, self).__init__(context, num_nodes)

    @property
    def security_config(self):
        return SecurityConfig(self.context, zk_sasl=self.zk_sasl)

    @property
    def security_system_properties(self):
        return "-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider " \
               "-DjaasLoginRenew=3600000 " \
               "-Djava.security.auth.login.config=%s " \
               "-Djava.security.krb5.conf=%s " % (self.security_config.JAAS_CONF_PATH, self.security_config.KRB5CONF_PATH)

    @property
    def zk_principals(self):
        return " zkclient "  + ' '.join(['zookeeper/' + zk_node.account.hostname for zk_node in self.nodes])

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p /mnt/zookeeper")
        node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)

        self.security_config.setup_node(node)
        config_file = self.render('zookeeper.properties')
        self.logger.info("zookeeper.properties:")
        self.logger.info(config_file)
        node.account.create_file("/mnt/zookeeper.properties", config_file)

        start_cmd = "export KAFKA_OPTS=\"%s\";" % (self.kafka_opts + ' ' + self.security_system_properties) \
            if self.security_config.zk_sasl else self.kafka_opts
        start_cmd += "%s " % self.path.script("zookeeper-server-start.sh", node)
        start_cmd += "/mnt/zookeeper.properties 1>> %(path)s 2>> %(path)s &" % self.logs["zk_log"]
        node.account.ssh(start_cmd)

        time.sleep(5)  # give it some time to start

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i zookeeper | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("zookeeper", allow_fail=False)
        wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for zookeeper to stop.")

    def clean_node(self, node):
        self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_process("zookeeper", clean_shutdown=False, allow_fail=True)
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log", allow_fail=False)

    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])

    #
    # This call is used to simulate a rolling upgrade to enable/disable
    # the use of ZooKeeper ACLs.
    #
    def zookeeper_migration(self, node, zk_acl):
        la_migra_cmd = "%s --zookeeper.acl=%s --zookeeper.connect=%s" % \
                       (self.path.script("zookeeper-security-migration.sh", node), zk_acl, self.connect_setting())
        node.account.ssh(la_migra_cmd)

    def query(self, path):
        """
        Queries zookeeper for data associated with 'path' and returns all fields in the schema
        """
        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s kafka.tools.ZooKeeperMainWrapper -server %s get %s" % \
              (kafka_run_class, self.connect_setting(), path)
        self.logger.debug(cmd)

        node = self.nodes[0]
        result = None
        for line in node.account.ssh_capture(cmd):
            # loop through all lines in the output, but only hold on to the first match
            if result is None:
                match = re.match("^({.+})$", line)
                if match is not None:
                    result = match.groups()[0]
        return result
