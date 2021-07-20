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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.security.kafka_acls import ACLs

class ZooKeeperAuthorizerTest(Test):
    """Tests that the ZooKeeper-based Authorizer works wth both ZooKeeper-based and KRaft clusters.
    Alters client quotas, making sure it works.
    Rolls Kafka with an authorizer.
    Alters client quotas, making sure it fails.
    Adds ACLs with super-user broker credentials.
    Alters client quotas, making sure it now works again.
    Removes ACLs.

    Note that we intend to have separate test explicitly for the
    KRaft-based replacement for the ZooKeeper-based authorizer.
    """

    def __init__(self, test_context):
        super(ZooKeeperAuthorizerTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        # setup ZooKeeper even with KRaft
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  topics={self.topic: {"partitions": 1, "replication-factor": 1}},
                                  controller_num_nodes_override=1, allow_zk_with_kraft=True)
    def setUp(self):
        # start ZooKeeper even with KRaft
        self.zk.start()
        self.acls = ACLs(self.test_context)

    @cluster(num_nodes=4)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_authorizer(self, metadata_quorum):
        broker_security_protocol = "SSL"
        broker_principal = "User:CN=systemtest"
        client_security_protocol = "SASL_PLAINTEXT"
        client_sasl_mechanism = 'GSSAPI'
        client_principal = "User:client"
        self.kafka.interbroker_security_protocol = broker_security_protocol
        self.kafka.security_protocol = client_security_protocol
        self.kafka.client_sasl_mechanism = client_sasl_mechanism
        if self.kafka.quorum_info.using_kraft:
            controller_quorum = self.kafka.controller_quorum
            controller_quorum.controller_security_protocol = broker_security_protocol
            controller_quorum.intercontroller_security_protocol = broker_security_protocol
        self.kafka.start()

        # alter client quotas
        node = self.kafka.nodes[0]
        alter_client_quotas_cmd = "%s --entity-name foo --entity-type clients --alter --add-config consumer_byte_rate=10000" % \
               (self.kafka.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection=False))
        alter_client_quotas_cmd_log_msg = "Running alter client quotas command with client/non-broker credentials...\n%s" % alter_client_quotas_cmd

        self.logger.info(alter_client_quotas_cmd_log_msg)
        node.account.ssh(alter_client_quotas_cmd)

        # set authorizer, restart with broker as super user
        if (metadata_quorum == quorum.remote_kraft):
            # we need to explicitly reconfigure/restart any remote controller quorum
            self.kafka.logger.info("Restarting Remote KRaft Controller with authorizer and broker principal as super user")
            controller_quorum = self.kafka.controller_quorum
            controller_quorum.authorizer_class_name = KafkaService.ACL_AUTHORIZER
            controller_quorum.server_prop_overrides = [["super.users", broker_principal]] # for broker to work with an authorizer
            controller_quorum.restart_cluster()
        self.kafka.logger.info("Restarting Kafka with authorizer and broker principal as super user")
        self.kafka.authorizer_class_name = KafkaService.ACL_AUTHORIZER
        self.kafka.server_prop_overrides = [["super.users", broker_principal]] # for broker to work with an authorizer
        self.kafka.restart_cluster()

        # the alter client quotas command should now fail
        try:
            self.logger.info("Expecting this to fail: %s" % alter_client_quotas_cmd_log_msg)
            node.account.ssh(alter_client_quotas_cmd)
            raise Exception("Expected alter client quotas command to fail with an authorization error, but it succeeded")
        except RemoteCommandError as e:
            if "ClusterAuthorizationException: Cluster authorization failed." not in str(e):
                self.logger.error("Expected alter client quotas command to fail with an authorization error, but it failed with some other issue")
                raise e
            self.logger.info("alter client quotas command failed with an authorization error as expected")

        # add ACLs
        self.logger.info("Adding ACLs with broker credentials so that alter client quotas command will succeed")
        self.acls.add_cluster_acl(self.kafka, client_principal, force_use_zk_connection=False,
                                  additional_cluster_operations_to_grant=['AlterConfigs'], security_protocol=broker_security_protocol)

        # the alter client quotas command should now succeed again
        self.logger.info(alter_client_quotas_cmd_log_msg)
        node.account.ssh(alter_client_quotas_cmd)

        # remove ACLs
        self.logger.info("Removing ACLs with broker credentials so ensure this operation works")
        self.acls.remove_cluster_acl(self.kafka, client_principal, additional_cluster_operations_to_remove=['AlterConfigs'],
                                     security_protocol=broker_security_protocol)
