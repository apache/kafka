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

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka.util import fix_opts_for_new_jvm


class ACLs(KafkaPathResolverMixin):
    def __init__(self, context):
        self.context = context

    def set_acls(self, protocol, kafka, topic, group, force_use_zk_connection=False):
        # Set server ACLs
        kafka_principal = "User:CN=systemtest" if protocol == "SSL" else "User:kafka"
        self.add_cluster_acl(kafka, kafka_principal, force_use_zk_connection=force_use_zk_connection)
        self.add_read_acl(kafka, kafka_principal, "*", force_use_zk_connection=force_use_zk_connection)

        # Set client ACLs
        client_principal = "User:CN=systemtest" if protocol == "SSL" else "User:client"
        self.add_produce_acl(kafka, client_principal, topic, force_use_zk_connection=force_use_zk_connection)
        self.add_consume_acl(kafka, client_principal, topic, group, force_use_zk_connection=force_use_zk_connection)

    def _acl_command_connect_setting(self, kafka, node, force_use_zk_connection):
        """
        Checks if --bootstrap-server config is supported, if yes then returns a string with
        bootstrap server, otherwise returns authorizer properties for zookeeper connection.
        """
        if not force_use_zk_connection and kafka.all_nodes_acl_command_supports_bootstrap_server():
            connection_setting = "--bootstrap-server %s" % (kafka.bootstrap_servers(kafka.security_protocol))
        else:
            connection_setting = "--authorizer-properties zookeeper.connect=%s" % (kafka.zk_connect_setting())

        return connection_setting

    def _kafka_acls_cmd_config(self, kafka, node, force_use_zk_connection):
        """
        Return --command-config parameter to the kafka-acls.sh command. The config parameter specifies
        the security settings that AdminClient uses to connect to a secure kafka server.
        """
        skip_command_config = force_use_zk_connection or not kafka.all_nodes_acl_command_supports_bootstrap_server()
        return "" if skip_command_config else " --command-config <(echo '%s')" % (kafka.security_config.client_config())

    def _acl_cmd_prefix(self, kafka, node, force_use_zk_connection):
        """
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available
        :return command prefix for running kafka-acls
        """
        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s %s %s" % (
            kafka.kafka_acls_cmd(node, force_use_zk_connection),
            self._acl_command_connect_setting(kafka, node, force_use_zk_connection),
            self._kafka_acls_cmd_config(kafka, node, force_use_zk_connection))
        return cmd

    def _add_acl_on_topic(self, kafka, principal, topic, operation_flag, node, force_use_zk_connection):
        """
        :param principal: principal for which ACL is created
        :param topic: topic for which ACL is created
        :param operation_flag: type of ACL created (e.g. --producer, --consumer, --operation=Read)
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available
        """
        cmd = "%(cmd_prefix)s --add --topic=%(topic)s %(operation_flag)s --allow-principal=%(principal)s" % {
            'cmd_prefix': self._acl_cmd_prefix(kafka, node, force_use_zk_connection),
            'topic': topic,
            'operation_flag': operation_flag,
            'principal': principal
        }
        kafka.run_cli_tool(node, cmd)

    def add_cluster_acl(self, kafka, principal, force_use_zk_connection=False):
        """
        :param kafka: Kafka cluster upon which ClusterAction ACL is created
        :param principal: principal for which ClusterAction ACL is created
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available.
               This is necessary for the case where we are bootstrapping ACLs before Kafka is started or before authorizer is enabled
        """
        node = kafka.nodes[0]

        force_use_zk_connection = force_use_zk_connection or not kafka.all_nodes_acl_command_supports_bootstrap_server()

        cmd = "%(cmd_prefix)s --add --cluster --operation=ClusterAction --allow-principal=%(principal)s" % {
            'cmd_prefix': self._acl_cmd_prefix(kafka, node, force_use_zk_connection),
            'principal': principal
        }
        kafka.run_cli_tool(node, cmd)

    def add_read_acl(self, kafka, principal, topic, force_use_zk_connection=False):
        """
        :param kafka: Kafka cluster upon which Read ACL is created
        :param principal: principal for which Read ACL is created
        :param topic: topic for which Read ACL is created
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available.
               This is necessary for the case where we are bootstrapping ACLs before Kafka is started or before authorizer is enabled
        """
        node = kafka.nodes[0]

        force_use_zk_connection = force_use_zk_connection or not kafka.all_nodes_acl_command_supports_bootstrap_server()

        self._add_acl_on_topic(kafka, principal, topic, "--operation=Read", node, force_use_zk_connection)

    def add_produce_acl(self, kafka, principal, topic, force_use_zk_connection=False):
        """
        :param kafka: Kafka cluster upon which Producer ACL is created
        :param principal: principal for which Producer ACL is created
        :param topic: topic for which Producer ACL is created
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available.
               This is necessary for the case where we are bootstrapping ACLs before Kafka is started or before authorizer is enabled
        """
        node = kafka.nodes[0]

        force_use_zk_connection = force_use_zk_connection or not kafka.all_nodes_acl_command_supports_bootstrap_server()

        self._add_acl_on_topic(kafka, principal, topic, "--producer", node, force_use_zk_connection)

    def add_consume_acl(self, kafka, principal, topic, group, force_use_zk_connection=False):
        """
        :param kafka: Kafka cluster upon which Consumer ACL is created
        :param principal: principal for which Consumer ACL is created
        :param topic: topic for which Consumer ACL is created
        :param group: consumewr group for which Consumer ACL is created
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available.
               This is necessary for the case where we are bootstrapping ACLs before Kafka is started or before authorizer is enabled
        """
        node = kafka.nodes[0]

        force_use_zk_connection = force_use_zk_connection or not kafka.all_nodes_acl_command_supports_bootstrap_server()

        cmd = "%(cmd_prefix)s --add --topic=%(topic)s --group=%(group)s --consumer --allow-principal=%(principal)s" % {
            'cmd_prefix': self._acl_cmd_prefix(kafka, node, force_use_zk_connection),
            'topic': topic,
            'group': group,
            'principal': principal
        }
        kafka.run_cli_tool(node, cmd)

