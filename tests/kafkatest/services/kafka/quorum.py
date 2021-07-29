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

# the types of metadata quorums we support
zk = 'ZK' # ZooKeeper, used before/during the KIP-500 bridge release(s)
colocated_kraft = 'COLOCATED_KRAFT' # co-located Controllers in KRaft mode, used during/after the KIP-500 bridge release(s)
remote_kraft = 'REMOTE_KRAFT' # separate Controllers in KRaft mode, used during/after the KIP-500 bridge release(s)

# How we will parameterize tests that exercise all quorum styles
#   [“ZK”, “REMOTE_KRAFT”, "COLOCATED_KRAFT"] during the KIP-500 bridge release(s)
#   [“REMOTE_KRAFT”, "COLOCATED_KRAFT”] after the KIP-500 bridge release(s)
all = [zk, remote_kraft, colocated_kraft]
# How we will parameterize tests that exercise all KRaft quorum styles
all_kraft = [remote_kraft, colocated_kraft]
# How we will parameterize tests that are unrelated to upgrades:
#   [“ZK”] before the KIP-500 bridge release(s)
#   [“ZK”, “REMOTE_KRAFT”] during the KIP-500 bridge release(s) and in preview releases
#   [“REMOTE_KRAFT”] after the KIP-500 bridge release(s)
all_non_upgrade = [zk, remote_kraft]

def for_test(test_context):
    # A test uses ZooKeeper if it doesn't specify a metadata quorum or if it explicitly specifies ZooKeeper
    default_quorum_type = zk
    arg_name = 'metadata_quorum'
    retval = default_quorum_type if not test_context.injected_args else test_context.injected_args.get(arg_name, default_quorum_type)
    if retval not in all:
        raise Exception("Unknown %s value provided for the test: %s" % (arg_name, retval))
    return retval

class ServiceQuorumInfo:
    """
    Exposes quorum-related information for a KafkaService

    Kafka can use either ZooKeeper or a KRaft (Kafka Raft) Controller quorum for
    its metadata.  KRaft Controllers can either be co-located with Kafka in
    the same JVM or remote in separate JVMs.  The choice is made via
    the 'metadata_quorum' parameter defined for the system test: if it
    is not explicitly defined, or if it is set to 'ZK', then ZooKeeper
    is used.  If it is explicitly set to 'COLOCATED_KRAFT' then KRaft
    controllers will be co-located with the brokers; the value
    `REMOTE_KRAFT` indicates remote controllers.

    Attributes
    ----------

    kafka : KafkaService
        The service for which this instance exposes quorum-related
        information
    quorum_type : str
        COLOCATED_KRAFT, REMOTE_KRAFT, or ZK
    using_zk : bool
        True iff quorum_type==ZK
    using_kraft : bool
        False iff quorum_type==ZK
    has_brokers : bool
        Whether there is at least one node with process.roles
        containing 'broker'.  True iff using_kraft and the Kafka
        service doesn't itself have a remote Kafka service (meaning
        it is not a remote controller quorum).
    has_controllers : bool
        Whether there is at least one node with process.roles
        containing 'controller'.  True iff quorum_type ==
        COLOCATED_KRAFT or the Kafka service itself has a remote Kafka
        service (meaning it is a remote controller quorum).
    has_brokers_and_controllers :
        True iff quorum_type==COLOCATED_KRAFT
    """

    def __init__(self, kafka, context):
        """

        :param kafka : KafkaService
            The service for which this instance exposes quorum-related
            information
        :param context : TestContext
            The test context within which the this instance and the
            given Kafka service is being instantiated
        """

        quorum_type = for_test(context)
        if quorum_type != zk and kafka.zk and not kafka.allow_zk_with_kraft:
            raise Exception("Cannot use ZooKeeper while specifying a KRaft metadata quorum unless explicitly allowing it")
        if kafka.remote_kafka and quorum_type != remote_kraft:
            raise Exception("Cannot specify a remote Kafka service unless using a remote KRaft metadata quorum (should not happen)")
        self.kafka = kafka
        self.quorum_type = quorum_type
        self.using_zk = quorum_type == zk
        self.using_kraft = not self.using_zk
        self.has_brokers = self.using_kraft and not kafka.remote_kafka
        self.has_controllers = quorum_type == colocated_kraft or kafka.remote_kafka
        self.has_brokers_and_controllers = quorum_type == colocated_kraft

class NodeQuorumInfo:
    """
    Exposes quorum-related information for a node in a KafkaService

    Attributes
    ----------
    service_quorum_info : ServiceQuorumInfo
        The quorum information about the service to which the node
        belongs
    has_broker_role : bool
        True iff using_kraft and the Kafka service doesn't itself have
        a remote Kafka service (meaning it is not a remote controller)
    has_controller_role : bool
        True iff quorum_type==COLOCATED_KRAFT and the node is one of
        the first N in the cluster where N is the number of nodes
        that have a controller role; or the Kafka service itself has a
        remote Kafka service (meaning it is a remote controller
        quorum).
    has_combined_broker_and_controller_roles :
        True iff has_broker_role==True and has_controller_role==true
    """

    def __init__(self, service_quorum_info, node):
        """
        :param service_quorum_info : ServiceQuorumInfo
            The quorum information about the service to which the node
            belongs
        :param node : Node
            The particular node for which this information applies.
            In the co-located case, whether or not a node's broker's
            process.roles contains 'controller' may vary based on the
            particular node if the number of controller nodes is less
            than the number of nodes in the service.
        """

        self.service_quorum_info = service_quorum_info
        self.has_broker_role = self.service_quorum_info.has_brokers
        idx = self.service_quorum_info.kafka.nodes.index(node)
        self.has_controller_role = self.service_quorum_info.kafka.num_nodes_controller_role > idx
        self.has_combined_broker_and_controller_roles = self.has_broker_role and self.has_controller_role
