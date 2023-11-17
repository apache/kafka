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
import re

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH


class ZookeeperService(KafkaPathResolverMixin, Service):
    ROOT = "/mnt/zookeeper"
    DATA = os.path.join(ROOT, "data")
    HEAP_DUMP_FILE = os.path.join(ROOT, "zk_heap_dump.bin")

    logs = {
        "zk_log": {
            "path": "%s/zk.log" % ROOT,
            "collect_default": True},
        "zk_data": {
            "path": DATA,
            "collect_default": False},
        "zk_heap_dump_file": {
            "path": HEAP_DUMP_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, zk_sasl = False, zk_client_port = True, zk_client_secure_port = False,
                 zk_tls_encrypt_only = False, version=DEV_BRANCH):
        """
        :type context
        """
        self.kafka_opts = ""
        self.zk_sasl = zk_sasl
        if (zk_client_secure_port or zk_tls_encrypt_only) and not version.supports_tls_to_zookeeper():
            raise Exception("Cannot use TLS with a ZooKeeper version that does not support it: %s" % str(version))
        if not zk_client_port and not zk_client_secure_port:
            raise Exception("Cannot disable both ZK clientPort and clientSecurePort")
        self.zk_client_port = zk_client_port
        self.zk_client_secure_port = zk_client_secure_port
        self.zk_tls_encrypt_only = zk_tls_encrypt_only
        super(ZookeeperService, self).__init__(context, num_nodes)
        self.set_version(version)

    def set_version(self, version):
        for node in self.nodes:
            node.version = version

    @property
    def security_config(self):
        return SecurityConfig(self.context, zk_sasl=self.zk_sasl, zk_tls=self.zk_client_secure_port)

    @property
    def security_system_properties(self):
        return "-Dzookeeper.authProvider.sasl=org.apache.zookeeper.server.auth.SASLAuthenticationProvider " \
               "-DjaasLoginRenew=3600000 " \
               "-Djava.security.auth.login.config=%s " \
               "-Djava.security.krb5.conf=%s " % (self.security_config.JAAS_CONF_PATH, self.security_config.KRB5CONF_PATH)

    @property
    def zk_principals(self):
        return " zkclient "  + ' '.join(['zookeeper/' + zk_node.account.hostname for zk_node in self.nodes])

    def restart_cluster(self):
        for node in self.nodes:
            self.restart_node(node)

    def restart_node(self, node):
        """Restart the given node."""
        self.stop_node(node)
        self.start_node(node)

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p %s" % ZookeeperService.DATA)
        node.account.ssh("echo %d > %s/myid" % (idx, ZookeeperService.DATA))

        self.security_config.setup_node(node)
        config_file = self.render('zookeeper.properties')
        self.logger.info("zookeeper.properties:")
        self.logger.info(config_file)
        node.account.create_file("%s/zookeeper.properties" % ZookeeperService.ROOT, config_file)

        heap_kafka_opts = "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s" % self.logs["zk_heap_dump_file"]["path"]
        other_kafka_opts = self.kafka_opts + ' ' + self.security_system_properties \
            if self.security_config.zk_sasl else self.kafka_opts
        start_cmd = "export KAFKA_OPTS=\"%s %s\";" % (heap_kafka_opts, other_kafka_opts)
        start_cmd += "%s " % self.path.script("zookeeper-server-start.sh", node)
        start_cmd += "%s/zookeeper.properties &>> %s &" % (ZookeeperService.ROOT, self.logs["zk_log"]["path"])
        node.account.ssh(start_cmd)

        wait_until(lambda: self.listening(node), timeout_sec=30, err_msg="Zookeeper node failed to start")

    def listening(self, node):
        try:
            port = 2181 if self.zk_client_port else 2182
            cmd = "nc -z %s %s" % (node.account.hostname, port)
            node.account.ssh_output(cmd, allow_fail=False)
            self.logger.debug("Zookeeper started accepting connections at: '%s:%s')", node.account.hostname, port)
            return True
        except (RemoteCommandError, ValueError) as e:
            return False

    def pids(self, node):
        return node.account.java_pids(self.java_class_name())

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_java_processes(self.java_class_name(), allow_fail=False)
        node.account.kill_java_processes(self.java_cli_class_name(), allow_fail=False)
        wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for zookeeper to stop.")

    def clean_node(self, node):
        self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
        if self.alive(node):
            self.logger.warn("%s %s was still alive at cleanup time. Killing forcefully..." %
                             (self.__class__.__name__, node.account))
        node.account.kill_java_processes(self.java_class_name(),
                                         clean_shutdown=False, allow_fail=True)
        node.account.kill_java_processes(self.java_cli_class_name(),
                                         clean_shutdown=False, allow_fail=False)
        node.account.ssh("rm -rf -- %s" % ZookeeperService.ROOT, allow_fail=False)


    # force_tls is a necessary option for the case where we define both encrypted and non-encrypted ports
    def connect_setting(self, chroot=None, force_tls=False):
        if chroot and not chroot.startswith("/"):
            raise Exception("ZK chroot must start with '/', invalid chroot: %s" % chroot)

        chroot = '' if chroot is None else chroot
        return ','.join([node.account.hostname + (':2182' if not self.zk_client_port or force_tls else ':2181') + chroot
                         for node in self.nodes])

    def zkTlsConfigFileOption(self, forZooKeeperMain=False):
        if not self.zk_client_secure_port:
            return ""
        return ("-zk-tls-config-file " if forZooKeeperMain else "--zk-tls-config-file ") + \
               (SecurityConfig.ZK_CLIENT_TLS_ENCRYPT_ONLY_CONFIG_PATH if self.zk_tls_encrypt_only else SecurityConfig.ZK_CLIENT_MUTUAL_AUTH_CONFIG_PATH)

    #
    # This call is used to simulate a rolling upgrade to enable/disable
    # the use of ZooKeeper ACLs.
    #
    def zookeeper_migration(self, node, zk_acl):
        la_migra_cmd = "export KAFKA_OPTS=\"%s\";" % \
                       self.security_system_properties if self.security_config.zk_sasl else ""
        la_migra_cmd += "%s --zookeeper.acl=%s --zookeeper.connect=%s %s" % \
                       (self.path.script("zookeeper-security-migration.sh", node), zk_acl,
                        self.connect_setting(force_tls=self.zk_client_secure_port),
                        self.zkTlsConfigFileOption())
        node.account.ssh(la_migra_cmd)

    def _check_chroot(self, chroot):
        if chroot and not chroot.startswith("/"):
            raise Exception("ZK chroot must start with '/', invalid chroot: %s" % chroot)

    def query(self, path, chroot=None):
        """
        Queries zookeeper for data associated with 'path' and returns all fields in the schema
        """
        self._check_chroot(chroot)

        chroot_path = ('' if chroot is None else chroot) + path

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s %s -server %s %s get %s" % \
              (kafka_run_class, self.java_cli_class_name(), self.connect_setting(force_tls=self.zk_client_secure_port),
               self.zkTlsConfigFileOption(True),
               chroot_path)
        self.logger.debug(cmd)

        node = self.nodes[0]
        result = None
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            # loop through all lines in the output, but only hold on to the first match
            if result is None:
                match = re.match("^({.+})$", line)
                if match is not None:
                    result = match.groups()[0]
        return result

    def get_children(self, path, chroot=None):
        """
        Queries zookeeper for data associated with 'path' and returns all fields in the schema
        """
        self._check_chroot(chroot)

        chroot_path = ('' if chroot is None else chroot) + path

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s %s -server %s %s ls %s" % \
              (kafka_run_class, self.java_cli_class_name(), self.connect_setting(force_tls=self.zk_client_secure_port),
               self.zkTlsConfigFileOption(True),
               chroot_path)
        self.logger.debug(cmd)

        node = self.nodes[0]
        result = None
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            # loop through all lines in the output, but only hold on to the first match
            if result is None:
                match = re.match("^(\\[.+\\])$", line)
                if match is not None:
                    result = match.groups()[0]
        if result is None:
            return []
        else:
            return result.strip("[]").split(", ")

    def delete(self, path, recursive, chroot=None):
        """
        Queries zookeeper for data associated with 'path' and returns all fields in the schema
        """
        self._check_chroot(chroot)

        chroot_path = ('' if chroot is None else chroot) + path

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        if recursive:
            op = "deleteall"
        else:
            op = "delete"
        cmd = "%s %s -server %s %s %s %s" % \
              (kafka_run_class, self.java_cli_class_name(), self.connect_setting(force_tls=self.zk_client_secure_port),
               self.zkTlsConfigFileOption(True),
               op, chroot_path)
        self.logger.debug(cmd)

        node = self.nodes[0]
        node.account.ssh_capture(cmd)

    def create(self, path, chroot=None, value=""):
        """
        Create an znode at the given path
        """
        self._check_chroot(chroot)

        chroot_path = ('' if chroot is None else chroot) + path

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s %s -server %s %s create %s '%s'" % \
              (kafka_run_class, self.java_cli_class_name(), self.connect_setting(force_tls=self.zk_client_secure_port),
               self.zkTlsConfigFileOption(True),
               chroot_path, value)
        self.logger.debug(cmd)
        output = self.nodes[0].account.ssh_output(cmd)
        self.logger.debug(output)

    def describeUsers(self):
        """
        Describe the default user using the ConfigCommand CLI
        """

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s kafka.admin.ConfigCommand --zookeeper %s %s --describe --entity-type users --entity-default" % \
              (kafka_run_class, self.connect_setting(force_tls=self.zk_client_secure_port),
               self.zkTlsConfigFileOption())
        self.logger.debug(cmd)
        output = self.nodes[0].account.ssh_output(cmd)
        self.logger.debug(output)

    def list_acls(self, topic):
        """
        List ACLs for the given topic using the AclCommand CLI
        """

        kafka_run_class = self.path.script("kafka-run-class.sh", DEV_BRANCH)
        cmd = "%s kafka.admin.AclCommand --authorizer-properties zookeeper.connect=%s %s --list --topic %s" % \
              (kafka_run_class, self.connect_setting(force_tls=self.zk_client_secure_port),
               self.zkTlsConfigFileOption(),
               topic)
        self.logger.debug(cmd)
        output = self.nodes[0].account.ssh_output(cmd)
        self.logger.debug(output)

    def java_class_name(self):
        """ The class name of the Zookeeper quorum peers. """
        return "org.apache.zookeeper.server.quorum.QuorumPeerMain"

    def java_cli_class_name(self):
        """ The class name of the Zookeeper tool within Kafka. """
        return "org.apache.zookeeper.ZooKeeperMainWithTlsSupportForKafka"
