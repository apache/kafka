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

from kafkatest.services.kafka.directory import kafka_dir

class ACLs():

    def __init__(self):
        pass

    def set_acls(self, protocol, kafka, zk, topic, group):
        node = kafka.nodes[0]
        setting = zk.connect_setting()

        # Set server ACLs
        kafka_principal = "User:CN=systemtest" if protocol == "SSL" else "User:kafka"
        self.acls_command(node, ACLs.add_cluster_acl(setting, kafka_principal))
        self.acls_command(node, ACLs.broker_read_acl(setting, "*", kafka_principal))

        # Set client ACLs
        client_principal = "User:CN=systemtest" if protocol == "SSL" else "User:client"
        self.acls_command(node, ACLs.produce_acl(setting, topic, client_principal))
        self.acls_command(node, ACLs.consume_acl(setting, topic, group, client_principal))

    def acls_command(self, node, properties):
        cmd = "/opt/%s/bin/kafka-acls.sh %s" % (kafka_dir(node), properties)
        node.account.ssh(cmd)

    @staticmethod
    def add_cluster_acl(zk_connect, principal="User:kafka"):
        return "--authorizer-properties zookeeper.connect=%(zk_connect)s --add --cluster " \
               "--operation=ClusterAction --allow-principal=%(principal)s " % {
            'zk_connect': zk_connect,
            'principal': principal
        }

    @staticmethod
    def broker_read_acl(zk_connect, topic, principal="User:kafka"):
        return "--authorizer-properties zookeeper.connect=%(zk_connect)s --add --topic=%(topic)s " \
               "--operation=Read --allow-principal=%(principal)s " % {
            'zk_connect': zk_connect,
            'topic': topic,
            'principal': principal
        }

    @staticmethod
    def produce_acl(zk_connect, topic, principal="User:client"):
        return "--authorizer-properties zookeeper.connect=%(zk_connect)s --add --topic=%(topic)s " \
               "--producer --allow-principal=%(principal)s " % {
            'zk_connect': zk_connect,
            'topic': topic,
            'principal': principal
        }

    @staticmethod
    def consume_acl(zk_connect, topic, group, principal="User:client"):
        return "--authorizer-properties zookeeper.connect=%(zk_connect)s --add --topic=%(topic)s " \
               "--group=%(group)s --consumer --allow-principal=%(principal)s " % {
            'zk_connect': zk_connect,
            'topic': topic,
            'group': group,
            'principal': principal
        }