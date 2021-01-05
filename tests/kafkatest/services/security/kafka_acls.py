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


class ACLs:
    def __init__(self, context):
        self.context = context

    def set_acls(self, protocol, kafka, topic, group, force_use_zk_connection=False, additional_cluster_operations_to_grant = []):
        """
        Creates ACls for the Kafka Broker principal that brokers use in tests

        :param protocol: the security protocol to use (e.g. PLAINTEXT, SASL_PLAINTEXT, etc.)
        :param kafka: Kafka cluster upon which ClusterAction ACL is created
        :param topic: topic for which produce and consume ACLs are created
        :param group: consumer group for which consume ACL is created
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available.
               This is necessary for the case where we are bootstrapping ACLs before Kafka is started or before authorizer is enabled
        :param additional_cluster_operations_to_grant may be set to ['Alter', 'Create'] if the cluster is secured since these are required
               to create SCRAM credentials and topics, respectively
        """
        # Set server ACLs
        kafka_principal = "User:CN=systemtest" if protocol == "SSL" else "User:kafka"
        self.add_cluster_acl(kafka, kafka_principal, force_use_zk_connection=force_use_zk_connection, additional_cluster_operations_to_grant = additional_cluster_operations_to_grant)
        self.add_read_acl(kafka, kafka_principal, "*", force_use_zk_connection=force_use_zk_connection)

        # Set client ACLs
        client_principal = "User:CN=systemtest" if protocol == "SSL" else "User:client"
        self.add_produce_acl(kafka, client_principal, topic, force_use_zk_connection=force_use_zk_connection)
        self.add_consume_acl(kafka, client_principal, topic, group, force_use_zk_connection=force_use_zk_connection)

    def _add_acl_on_topic(self, kafka, principal, topic, operation_flag, node, force_use_zk_connection):
        """
        :param principal: principal for which ACL is created
        :param topic: topic for which ACL is created
        :param operation_flag: type of ACL created (e.g. --producer, --consumer, --operation=Read)
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available
        """
        cmd = "%(cmd_prefix)s --add --topic=%(topic)s %(operation_flag)s --allow-principal=%(principal)s" % {
            'cmd_prefix': kafka.kafka_acls_cmd_with_optional_security_settings(node, force_use_zk_connection),
            'topic': topic,
            'operation_flag': operation_flag,
            'principal': principal
        }
        kafka.run_cli_tool(node, cmd)

    def add_cluster_acl(self, kafka, principal, force_use_zk_connection=False, additional_cluster_operations_to_grant = []):
        """
        :param kafka: Kafka cluster upon which ClusterAction ACL is created
        :param principal: principal for which ClusterAction ACL is created
        :param node: Node to use when determining connection settings
        :param force_use_zk_connection: forces the use of ZooKeeper when true, otherwise AdminClient is used when available.
               This is necessary for the case where we are bootstrapping ACLs before Kafka is started or before authorizer is enabled
        :param additional_cluster_operations_to_grant may be set to ['Alter', 'Create'] if the cluster is secured since these are required
               to create SCRAM credentials and topics, respectively
        """
        node = kafka.nodes[0]

        for operation in ['ClusterAction'] + additional_cluster_operations_to_grant:
            cmd = "%(cmd_prefix)s --add --cluster --operation=%(operation)s --allow-principal=%(principal)s" % {
                'cmd_prefix': kafka.kafka_acls_cmd_with_optional_security_settings(node, force_use_zk_connection),
                'operation': operation,
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

        cmd = "%(cmd_prefix)s --add --topic=%(topic)s --group=%(group)s --consumer --allow-principal=%(principal)s" % {
            'cmd_prefix': kafka.kafka_acls_cmd_with_optional_security_settings(node, force_use_zk_connection),
            'topic': topic,
            'group': group,
            'principal': principal
        }
        kafka.run_cli_tool(node, cmd)

