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

import time

class ACLs:
    def __init__(self, context):
        self.context = context

    def add_cluster_acl(self, kafka, principal, additional_cluster_operations_to_grant = [], security_protocol=None):
        """
        :param kafka: Kafka cluster upon which ClusterAction ACL is created
        :param principal: principal for which ClusterAction ACL is created
        :param node: Node to use when determining connection settings
        :param additional_cluster_operations_to_grant may be set to ['Alter', 'Create'] if the cluster is secured since these are required
               to create SCRAM credentials and topics, respectively
        :param security_protocol set it to explicitly determine whether we use client or broker credentials, otherwise
                we use the the client security protocol unless inter-broker security protocol is PLAINTEXT, in which case we use PLAINTEXT.
                Then we use the broker's credentials if the selected security protocol matches the inter-broker security protocol,
                otherwise we use the client's credentials.
        """
        node = kafka.nodes[0]

        for operation in ['ClusterAction'] + additional_cluster_operations_to_grant:
            cmd = "%(cmd_prefix)s --add --cluster --operation=%(operation)s --allow-principal=%(principal)s" % {
                'cmd_prefix': kafka.kafka_acls_cmd_with_optional_security_settings(node, security_protocol),
                'operation': operation,
                'principal': principal
            }
            kafka.run_cli_tool(node, cmd)

    def remove_cluster_acl(self, kafka, principal, additional_cluster_operations_to_remove = [], security_protocol=None):
        """
        :param kafka: Kafka cluster upon which ClusterAction ACL is deleted
        :param principal: principal for which ClusterAction ACL is deleted
        :param node: Node to use when determining connection settings
        :param additional_cluster_operations_to_remove may be set to ['Alter', 'Create'] if the cluster is secured since these are required
               to create SCRAM credentials and topics, respectively
        :param security_protocol set it to explicitly determine whether we use client or broker credentials, otherwise
                we use the the client security protocol unless inter-broker security protocol is PLAINTEXT, in which case we use PLAINTEXT.
                Then we use the broker's credentials if the selected security protocol matches the inter-broker security protocol,
                otherwise we use the client's credentials.
        """
        node = kafka.nodes[0]

        for operation in ['ClusterAction'] + additional_cluster_operations_to_remove:
            cmd = "%(cmd_prefix)s --remove --force --cluster --operation=%(operation)s --allow-principal=%(principal)s" % {
                'cmd_prefix': kafka.kafka_acls_cmd_with_optional_security_settings(node, security_protocol),
                'operation': operation,
                'principal': principal
            }
            kafka.logger.info(cmd)
            kafka.run_cli_tool(node, cmd)
