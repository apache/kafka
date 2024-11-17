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

import json
import math
import os.path
import re
import signal
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from ducktape.cluster.remoteaccount import RemoteCommandError

from .config import KafkaConfig
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import config_property, quorum
from kafkatest.services.monitor.jmx import JmxMixin
from kafkatest.services.security.minikdc import MiniKdc
from kafkatest.services.security.listener_security_config import ListenerSecurityConfig
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH
from kafkatest.version import KafkaVersion
from kafkatest.services.kafka.util import fix_opts_for_new_jvm


class KafkaListener:

    def __init__(self, name, port_number, security_protocol, open=False, sasl_mechanism = None):
        self.name = name
        self.port_number = port_number
        self.security_protocol = security_protocol
        self.open = open
        self.sasl_mechanism = sasl_mechanism

    def listener(self):
        return "%s://:%s" % (self.name, str(self.port_number))

    def advertised_listener(self, node):
        return "%s://%s:%s" % (self.name, node.account.hostname, str(self.port_number))

    def listener_security_protocol(self):
        return "%s:%s" % (self.name, self.security_protocol)

class KafkaService(KafkaPathResolverMixin, JmxMixin, Service):
    """
    Ducktape system test service for Brokers and KRaft Controllers

    Metadata Quorums
    ----------------
    Kafka can use either ZooKeeper or a KRaft Controller quorum for its
    metadata.  See the kafkatest.services.kafka.quorum.ServiceQuorumInfo
    class for details.

    Attributes
    ----------

    quorum_info : kafkatest.services.kafka.quorum.ServiceQuorumInfo
        Information about the service and it's metadata quorum
    num_nodes_broker_role : int
        The number of nodes in the service that include 'broker'
        in process.roles (0 when using Zookeeper)
    num_nodes_controller_role : int
        The number of nodes in the service that include 'controller'
        in process.roles (0 when using Zookeeper)
    controller_quorum : KafkaService
        None when using ZooKeeper, otherwise the Kafka service for the
        combined case or the isolated controller quorum service
        instance for the isolated case
    isolated_controller_quorum : KafkaService
        None for the combined case or when using ZooKeeper, otherwise
        the isolated controller quorum service instance

    Kafka Security Protocols
    ------------------------
    The security protocol advertised to clients and the inter-broker
    security protocol can be set in the constructor and can be changed
    afterwards as well.  Set these attributes to make changes; they
    take effect when starting each node:

    security_protocol : str
        default PLAINTEXT
    client_sasl_mechanism : str
        default GSSAPI, ignored unless using SASL_PLAINTEXT or SASL_SSL
    interbroker_security_protocol : str
        default PLAINTEXT
    interbroker_sasl_mechanism : str
        default GSSAPI, ignored unless using SASL_PLAINTEXT or SASL_SSL

    ZooKeeper
    ---------
    Create an instance of ZookeeperService when metadata_quorum is ZK
    (ZK is the default if metadata_quorum is not a test parameter).

    KRaft Quorums
    ------------
    Set metadata_quorum accordingly (to COMBINED_KRAFT or ISOLATED_KRAFT).
    Do not instantiate a ZookeeperService instance.

    Starting Kafka will cause any isolated controller quorum to
    automatically start first.  Explicitly stopping Kafka does not stop
    any isolated controller quorum, but Ducktape will stop both when
    tearing down the test (it will stop Kafka first).

    KRaft Security Protocols
    --------------------------------
    The broker-to-controller and inter-controller security protocols
    will both initially be set to the inter-broker security protocol.
    The broker-to-controller and inter-controller security protocols
    must be identical for the combined case (an exception will be
    thrown when trying to start the service if they are not identical).
    The broker-to-controller and inter-controller security protocols
    can differ in the isolated case.

    Set these attributes for the combined case.  Changes take effect
    when starting each node:

    controller_security_protocol : str
        default PLAINTEXT
    controller_sasl_mechanism : str
        default GSSAPI, ignored unless using SASL_PLAINTEXT or SASL_SSL
    intercontroller_security_protocol : str
        default PLAINTEXT
    intercontroller_sasl_mechanism : str
        default GSSAPI, ignored unless using SASL_PLAINTEXT or SASL_SSL

    Set the same attributes for the isolated case (changes take effect
    when starting each quorum node), but you must first obtain the
    service instance for the isolated quorum via one of the
    'controller_quorum' or 'isolated_controller_quorum' attributes as
    defined above.

    """
    PERSISTENT_ROOT = "/mnt/kafka"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "server-start-stdout-stderr.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "kafka-log4j.properties")
    # Logs such as controller.log, server.log, etc all go here
    OPERATIONAL_LOG_DIR = os.path.join(PERSISTENT_ROOT, "kafka-operational-logs")
    OPERATIONAL_LOG_INFO_DIR = os.path.join(OPERATIONAL_LOG_DIR, "info")
    OPERATIONAL_LOG_DEBUG_DIR = os.path.join(OPERATIONAL_LOG_DIR, "debug")
    # Kafka log segments etc go here
    DATA_LOG_DIR_PREFIX = os.path.join(PERSISTENT_ROOT, "kafka-data-logs")
    DATA_LOG_DIR_1 = "%s-1" % (DATA_LOG_DIR_PREFIX)
    DATA_LOG_DIR_2 = "%s-2" % (DATA_LOG_DIR_PREFIX)
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "kafka.properties")
    METADATA_LOG_DIR = os.path.join (PERSISTENT_ROOT, "kafka-metadata-logs")
    METADATA_SNAPSHOT_SEARCH_STR = "%s/__cluster_metadata-0/*.checkpoint" % METADATA_LOG_DIR
    METADATA_FIRST_LOG = "%s/__cluster_metadata-0/00000000000000000000.log" % METADATA_LOG_DIR
    # Kafka Authorizer
    KRAFT_ACL_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer"
    HEAP_DUMP_FILE = os.path.join(PERSISTENT_ROOT, "kafka_heap_dump.bin")
    INTERBROKER_LISTENER_NAME = 'INTERNAL'
    JAAS_CONF_PROPERTY = "java.security.auth.login.config=/mnt/security/jaas.conf"
    ADMIN_CLIENT_AS_BROKER_JAAS_CONF_PROPERTY = "java.security.auth.login.config=/mnt/security/admin_client_as_broker_jaas.conf"
    KRB5_CONF = "java.security.krb5.conf=/mnt/security/krb5.conf"
    SECURITY_PROTOCOLS = [SecurityConfig.PLAINTEXT, SecurityConfig.SSL, SecurityConfig.SASL_PLAINTEXT, SecurityConfig.SASL_SSL]

    logs = {
        "kafka_server_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True},
        "kafka_operational_logs_info": {
            "path": OPERATIONAL_LOG_INFO_DIR,
            "collect_default": True},
        "kafka_operational_logs_debug": {
            "path": OPERATIONAL_LOG_DEBUG_DIR,
            "collect_default": False},
        "kafka_data_1": {
            "path": DATA_LOG_DIR_1,
            "collect_default": False},
        "kafka_data_2": {
            "path": DATA_LOG_DIR_2,
            "collect_default": False},
        "kafka_cluster_metadata": {
            "path": METADATA_LOG_DIR,
            "collect_default": False},
        "kafka_heap_dump_file": {
            "path": HEAP_DUMP_FILE,
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, zk, security_protocol=SecurityConfig.PLAINTEXT,
                 interbroker_security_protocol=SecurityConfig.PLAINTEXT,
                 client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI, interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_GSSAPI,
                 authorizer_class_name=None, topics=None, version=DEV_BRANCH, jmx_object_names=None,
                 jmx_attributes=None, zk_connect_timeout=18000, zk_session_timeout=18000, server_prop_overrides=None, zk_chroot=None,
                 zk_client_secure=False,
                 listener_security_config=ListenerSecurityConfig(), per_node_server_prop_overrides=None,
                 extra_kafka_opts="", tls_version=None,
                 isolated_kafka=None,
                 controller_num_nodes_override=0,
                 allow_zk_with_kraft=False,
                 quorum_info_provider=None,
                 use_new_coordinator=None,
                 consumer_group_migration_policy=None,
                 dynamicRaftQuorum=False,
                 ):
        """
        :param context: test context
        :param int num_nodes: the number of nodes in the service.  There are 4 possibilities:
            1) Zookeeper quorum:
                The number of brokers is defined by this parameter.
                The broker.id values will be 1..num_nodes.
            2) Combined KRaft quorum:
                The number of nodes having a broker role is defined by this parameter.
                The node.id values will be 1..num_nodes
                The number of nodes having a controller role will by default be 1, 3, or 5 depending on num_nodes
                (1 if num_nodes < 3, otherwise 3 if num_nodes < 5, otherwise 5).  This calculation
                can be overridden via controller_num_nodes_override, which must be between 1 and num_nodes,
                inclusive, when non-zero.  Here are some possibilities:
                num_nodes = 1:
                    broker having node.id=1: broker.roles=broker+controller
                num_nodes = 2:
                    broker having node.id=1: broker.roles=broker+controller
                    broker having node.id=2: broker.roles=broker
                num_nodes = 3:
                    broker having node.id=1: broker.roles=broker+controller
                    broker having node.id=2: broker.roles=broker+controller
                    broker having node.id=3: broker.roles=broker+controller
                num_nodes = 3, controller_num_nodes_override = 1
                    broker having node.id=1: broker.roles=broker+controller
                    broker having node.id=2: broker.roles=broker
                    broker having node.id=3: broker.roles=broker
            3) Isolated KRaft quorum when instantiating the broker service:
                The number of nodes, all of which will have broker.roles=broker, is defined by this parameter.
                The node.id values will be 1..num_nodes
            4) Isolated KRaft quorum when instantiating the controller service:
                The number of nodes, all of which will have broker.roles=controller, is defined by this parameter.
                The node.id values will be 3001..(3000 + num_nodes)
                The value passed in is determined by the broker service when that is instantiated, and it uses the
                same algorithm as described above: 1, 3, or 5 unless controller_num_nodes_override is provided.
        :param ZookeeperService zk:
        :param dict topics: which topics to create automatically
        :param str security_protocol: security protocol for clients to use
        :param str tls_version: version of the TLS protocol.
        :param str interbroker_security_protocol: security protocol to use for broker-to-broker (and KRaft controller-to-controller) communication
        :param str client_sasl_mechanism: sasl mechanism for clients to use
        :param str interbroker_sasl_mechanism: sasl mechanism to use for broker-to-broker (and to-controller) communication
        :param str authorizer_class_name: which authorizer class to use
        :param str version: which kafka version to use. Defaults to "dev" branch
        :param jmx_object_names:
        :param jmx_attributes:
        :param int zk_connect_timeout:
        :param int zk_session_timeout:
        :param list[list] server_prop_overrides: overrides for kafka.properties file
            e.g: [["config1", "true"], ["config2", "1000"]]
        :param str zk_chroot:
        :param bool zk_client_secure: connect to Zookeeper over secure client port (TLS) when True
        :param ListenerSecurityConfig listener_security_config: listener config to use
        :param dict per_node_server_prop_overrides: overrides for kafka.properties file keyed by 1-based node number
            e.g: {1: [["config1", "true"], ["config2", "1000"]], 2: [["config1", "false"], ["config2", "0"]]}
        :param str extra_kafka_opts: jvm args to add to KAFKA_OPTS variable
        :param KafkaService isolated_kafka: process.roles=controller for this cluster when not None; ignored when using ZooKeeper
        :param int controller_num_nodes_override: the number of controller nodes to use in the cluster, instead of 5, 3, or 1 based on num_nodes, if positive, not using ZooKeeper, and isolated_kafka is not None; ignored otherwise
        :param bool allow_zk_with_kraft: if True, then allow a KRaft broker or controller to also use ZooKeeper
        :param quorum_info_provider: A function that takes this KafkaService as an argument and returns a ServiceQuorumInfo. If this is None, then the ServiceQuorumInfo is generated from the test context
        :param use_new_coordinator: When true, use the new implementation of the group coordinator as per KIP-848. If this is None, the default existing group coordinator is used.
        :param consumer_group_migration_policy: The config that enables converting the non-empty classic group using the consumer embedded protocol to the non-empty consumer group using the consumer group protocol and vice versa.
        :param dynamicRaftQuorum: When true, controller_quorum_bootstrap_servers, and bootstraps the first controller using the standalone flag
        """

        self.zk = zk
        self.isolated_kafka = isolated_kafka
        self.allow_zk_with_kraft = allow_zk_with_kraft
        if quorum_info_provider is None:
            self.quorum_info = quorum.ServiceQuorumInfo.from_test_context(self, context)
        else:
            self.quorum_info = quorum_info_provider(self)
        self.controller_quorum = None # will define below if necessary
        self.isolated_controller_quorum = None # will define below if necessary
        self.dynamicRaftQuorum = False

        # Set use_new_coordinator based on context and arguments.
        # If not specified, the default config is used.
        if use_new_coordinator is None:
            arg_name = 'use_new_coordinator'
            if context.injected_args is not None:
                use_new_coordinator = context.injected_args.get(arg_name)
            if use_new_coordinator is None:
                use_new_coordinator = context.globals.get(arg_name)
        
        # Assign the determined value.
        self.use_new_coordinator = use_new_coordinator

        # Set consumer_group_migration_policy based on context and arguments.
        if consumer_group_migration_policy is None:
            arg_name = 'consumer_group_migration_policy'
            if context.injected_args is not None:
                consumer_group_migration_policy = context.injected_args.get(arg_name)
            if consumer_group_migration_policy is None:
                consumer_group_migration_policy = context.globals.get(arg_name)
        self.consumer_group_migration_policy = consumer_group_migration_policy

        if num_nodes < 1:
            raise Exception("Must set a positive number of nodes: %i" % num_nodes)
        self.num_nodes_broker_role = 0
        self.num_nodes_controller_role = 0

        if self.quorum_info.using_kraft:
            self.dynamicRaftQuorum = dynamicRaftQuorum
            # Used to ensure not more than one controller bootstraps with the standalone flag
            self.standalone_controller_bootstrapped = False
            if self.quorum_info.has_brokers:
                num_nodes_broker_role = num_nodes
                if self.quorum_info.has_controllers:
                    self.num_nodes_controller_role = self.num_kraft_controllers(num_nodes_broker_role, controller_num_nodes_override)
                    if self.isolated_kafka:
                        raise Exception("Must not specify isolated Kafka service with combined Controller quorum")
            else:
                self.num_nodes_controller_role = num_nodes
                if not self.isolated_kafka:
                    raise Exception("Must specify isolated Kafka service when instantiating isolated Controller service (should not happen)")

            # Initially use the inter-broker security protocol for both
            # broker-to-controller and inter-controller communication. Both can be explicitly changed later if desired.
            # Note, however, that the two must the same if the controller quorum is combined with the
            # brokers.  Different security protocols for the two are only supported with a isolated controller quorum.
            self.controller_security_protocol = interbroker_security_protocol
            self.controller_sasl_mechanism = interbroker_sasl_mechanism
            self.intercontroller_security_protocol = interbroker_security_protocol
            self.intercontroller_sasl_mechanism = interbroker_sasl_mechanism

            # Ducktape tears down services in the reverse order in which they are created,
            # so create a service for the isolated controller quorum (if we need one) first, before
            # invoking Service.__init__(), so that Ducktape will tear down the quorum last; otherwise
            # Ducktape will tear down the controller quorum first, which could lead to problems in
            # Kafka and delays in tearing it down (and who knows what else -- it's simply better
            # to correctly tear down Kafka first, before tearing down the isolated controller).
            if self.quorum_info.has_controllers:
                self.controller_quorum = self
            else:
                num_isolated_controller_nodes = self.num_kraft_controllers(num_nodes, controller_num_nodes_override)
                self.isolated_controller_quorum = KafkaService(
                    context, num_isolated_controller_nodes, self.zk, security_protocol=self.controller_security_protocol,
                    interbroker_security_protocol=self.intercontroller_security_protocol,
                    client_sasl_mechanism=self.controller_sasl_mechanism, interbroker_sasl_mechanism=self.intercontroller_sasl_mechanism,
                    authorizer_class_name=authorizer_class_name, version=version, jmx_object_names=jmx_object_names,
                    jmx_attributes=jmx_attributes,
                    listener_security_config=listener_security_config,
                    extra_kafka_opts=extra_kafka_opts, tls_version=tls_version,
                    isolated_kafka=self, allow_zk_with_kraft=self.allow_zk_with_kraft,
                    server_prop_overrides=server_prop_overrides, dynamicRaftQuorum=self.dynamicRaftQuorum
                )
                self.controller_quorum = self.isolated_controller_quorum

        Service.__init__(self, context, num_nodes)
        JmxMixin.__init__(self, num_nodes=num_nodes, jmx_object_names=jmx_object_names, jmx_attributes=(jmx_attributes or []),
                          root=KafkaService.PERSISTENT_ROOT)

        self.security_protocol = security_protocol
        self.tls_version = tls_version
        self.client_sasl_mechanism = client_sasl_mechanism
        self.topics = topics
        self.minikdc = None
        self.concurrent_start = True # start concurrently by default
        self.authorizer_class_name = authorizer_class_name
        self.zk_set_acl = False
        if server_prop_overrides is None:
            self.server_prop_overrides = []
        else:
            self.server_prop_overrides = server_prop_overrides
        if per_node_server_prop_overrides is None:
            self.per_node_server_prop_overrides = {}
        else:
            self.per_node_server_prop_overrides = per_node_server_prop_overrides
        self.log_level = "DEBUG"
        self.zk_chroot = zk_chroot
        self.zk_client_secure = zk_client_secure
        self.listener_security_config = listener_security_config
        self.extra_kafka_opts = extra_kafka_opts

        #
        # In a heavily loaded and not very fast machine, it is
        # sometimes necessary to give more time for the zk client
        # to have its session established, especially if the client
        # is authenticating and waiting for the SaslAuthenticated
        # in addition to the SyncConnected event.
        #
        # The default value for zookeeper.connect.timeout.ms is
        # 2 seconds and here we increase it to 5 seconds, but
        # it can be overridden by setting the corresponding parameter
        # for this constructor.
        self.zk_connect_timeout = zk_connect_timeout

        # Also allow the session timeout to be provided explicitly,
        # primarily so that test cases can depend on it when waiting
        # e.g. brokers to deregister after a hard kill.
        self.zk_session_timeout = zk_session_timeout

        broker_only_port_mappings = {
            KafkaService.INTERBROKER_LISTENER_NAME:
                KafkaListener(KafkaService.INTERBROKER_LISTENER_NAME, config_property.FIRST_BROKER_PORT + 7, None, False)
        }
        controller_only_port_mappings = {}
        for idx, sec_protocol in enumerate(KafkaService.SECURITY_PROTOCOLS):
            name_for_controller = self.controller_listener_name(sec_protocol)
            broker_only_port_mappings[sec_protocol] = KafkaListener(sec_protocol, config_property.FIRST_BROKER_PORT + idx, sec_protocol, False)
            controller_only_port_mappings[name_for_controller] = KafkaListener(name_for_controller, config_property.FIRST_CONTROLLER_PORT + idx, sec_protocol, False)

        if self.quorum_info.using_zk or self.quorum_info.has_brokers and not self.quorum_info.has_controllers: # ZK or KRaft broker-only
            self.port_mappings = broker_only_port_mappings
        elif self.quorum_info.has_brokers_and_controllers: # KRaft broker+controller
            self.port_mappings = broker_only_port_mappings.copy()
            self.port_mappings.update(controller_only_port_mappings)
        else: # KRaft controller-only
            self.port_mappings = controller_only_port_mappings

        self.interbroker_listener = None
        if self.quorum_info.using_zk or self.quorum_info.has_brokers:
            self.setup_interbroker_listener(interbroker_security_protocol, self.listener_security_config.use_separate_interbroker_listener)
        self.interbroker_sasl_mechanism = interbroker_sasl_mechanism
        self._security_config = None

        for node in self.nodes:
            node_quorum_info = quorum.NodeQuorumInfo(self.quorum_info, node)

            node.version = version
            zk_broker_configs = {
                config_property.PORT: config_property.FIRST_BROKER_PORT,
                config_property.BROKER_ID: self.idx(node),
                config_property.ZOOKEEPER_CONNECTION_TIMEOUT_MS: zk_connect_timeout,
                config_property.ZOOKEEPER_SESSION_TIMEOUT_MS: zk_session_timeout
            }
            kraft_broker_configs = {
                config_property.PORT: config_property.FIRST_BROKER_PORT,
                config_property.NODE_ID: self.idx(node)
            }
            kraft_broker_plus_zk_configs = kraft_broker_configs.copy()
            kraft_broker_plus_zk_configs.update(zk_broker_configs)
            kraft_broker_plus_zk_configs.pop(config_property.BROKER_ID)
            if node_quorum_info.service_quorum_info.using_zk:
                node.config = KafkaConfig(**zk_broker_configs)
            elif not node_quorum_info.has_broker_role: # KRaft controller-only role
                controller_only_configs = {
                    config_property.NODE_ID: self.node_id_as_isolated_controller(node),
                }
                kraft_controller_plus_zk_configs = controller_only_configs.copy()
                kraft_controller_plus_zk_configs.update(zk_broker_configs)
                kraft_controller_plus_zk_configs.pop(config_property.BROKER_ID)
                if self.zk:
                    node.config = KafkaConfig(**kraft_controller_plus_zk_configs)
                else:
                    node.config = KafkaConfig(**controller_only_configs)
            else: # KRaft broker-only role or combined broker+controller roles
                if self.zk:
                    node.config = KafkaConfig(**kraft_broker_plus_zk_configs)
                else:
                    node.config = KafkaConfig(**kraft_broker_configs)
        self.combined_nodes_started = 0
        self.nodes_to_start = self.nodes

    def node_id_as_isolated_controller(self, node):
        """
        Generates the node id for a controller-only node, starting from config_property.FIRST_CONTROLLER_ID so as not  
        to overlap with broker id numbering.
        This method does not do any validation to check this node is actually part of an isolated controller quorum.
        """
        return self.idx(node) + config_property.FIRST_CONTROLLER_ID - 1

    def num_kraft_controllers(self, num_nodes_broker_role, controller_num_nodes_override):
        if controller_num_nodes_override < 0:
            raise Exception("controller_num_nodes_override must not be negative: %i" % controller_num_nodes_override)
        if controller_num_nodes_override > num_nodes_broker_role and self.quorum_info.quorum_type == quorum.combined_kraft:
            raise Exception("controller_num_nodes_override must not exceed the service's node count in the combined case: %i > %i" %
                            (controller_num_nodes_override, num_nodes_broker_role))
        if controller_num_nodes_override:
            return controller_num_nodes_override
        if num_nodes_broker_role < 3:
            return 1
        if num_nodes_broker_role < 5:
            return 3
        return 5

    def set_version(self, version):
        for node in self.nodes:
            node.version = version

    def controller_listener_name(self, security_protocol_name):
        return "CONTROLLER_%s" % security_protocol_name

    @property
    def interbroker_security_protocol(self):
        # TODO: disentangle interbroker and intercontroller protocol information
        return self.interbroker_listener.security_protocol if self.quorum_info.using_zk or self.quorum_info.has_brokers else self.intercontroller_security_protocol

    # this is required for backwards compatibility - there are a lot of tests that set this property explicitly
    # meaning 'use one of the existing listeners that match given security protocol, do not use custom listener'
    @interbroker_security_protocol.setter
    def interbroker_security_protocol(self, security_protocol):
        self.setup_interbroker_listener(security_protocol, use_separate_listener=False)

    def setup_interbroker_listener(self, security_protocol, use_separate_listener=False):
        self.listener_security_config.use_separate_interbroker_listener = use_separate_listener

        if self.listener_security_config.use_separate_interbroker_listener:
            # do not close existing port here since it is not used exclusively for interbroker communication
            self.interbroker_listener = self.port_mappings[KafkaService.INTERBROKER_LISTENER_NAME]
            self.interbroker_listener.security_protocol = security_protocol
        else:
            # close dedicated interbroker port, so it's not dangling in 'listeners' and 'advertised.listeners'
            self.close_port(KafkaService.INTERBROKER_LISTENER_NAME)
            self.interbroker_listener = self.port_mappings[security_protocol]

    @property
    def security_config(self):
        if not self._security_config:
            # we will later change the security protocols to PLAINTEXT if this is an isolated KRaft controller case since
            # those security protocols are irrelevant there and we don't want to falsely indicate the use of SASL or TLS
            security_protocol_to_use=self.security_protocol
            interbroker_security_protocol_to_use=self.interbroker_security_protocol
            # determine uses/serves controller sasl mechanisms
            serves_controller_sasl_mechanism=None
            serves_intercontroller_sasl_mechanism=None
            uses_controller_sasl_mechanism=None
            if self.quorum_info.has_brokers:
                if self.controller_quorum.controller_security_protocol in SecurityConfig.SASL_SECURITY_PROTOCOLS:
                    uses_controller_sasl_mechanism = self.controller_quorum.controller_sasl_mechanism
            if self.quorum_info.has_controllers:
                if self.intercontroller_security_protocol in SecurityConfig.SASL_SECURITY_PROTOCOLS:
                    serves_intercontroller_sasl_mechanism = self.intercontroller_sasl_mechanism
                    uses_controller_sasl_mechanism = self.intercontroller_sasl_mechanism # won't change from above in combined case
                if self.controller_security_protocol in SecurityConfig.SASL_SECURITY_PROTOCOLS:
                    serves_controller_sasl_mechanism = self.controller_sasl_mechanism
            # determine if KRaft uses TLS
            kraft_tls = False
            if self.quorum_info.has_brokers and not self.quorum_info.has_controllers:
                # KRaft broker only
                kraft_tls = self.controller_quorum.controller_security_protocol in SecurityConfig.SSL_SECURITY_PROTOCOLS
            if self.quorum_info.has_controllers:
                # isolated or combined KRaft controller
                kraft_tls = self.controller_security_protocol in SecurityConfig.SSL_SECURITY_PROTOCOLS \
                           or self.intercontroller_security_protocol in SecurityConfig.SSL_SECURITY_PROTOCOLS
            # clear irrelevant security protocols of SASL/TLS implications for the isolated controller quorum case
            if self.quorum_info.has_controllers and not self.quorum_info.has_brokers:
                security_protocol_to_use=SecurityConfig.PLAINTEXT
                interbroker_security_protocol_to_use=SecurityConfig.PLAINTEXT

            self._security_config = SecurityConfig(self.context, security_protocol_to_use, interbroker_security_protocol_to_use,
                                                   zk_sasl=self.zk.zk_sasl if self.quorum_info.using_zk else False, zk_tls=self.zk_client_secure,
                                                   client_sasl_mechanism=self.client_sasl_mechanism,
                                                   interbroker_sasl_mechanism=self.interbroker_sasl_mechanism,
                                                   listener_security_config=self.listener_security_config,
                                                   tls_version=self.tls_version,
                                                   serves_controller_sasl_mechanism=serves_controller_sasl_mechanism,
                                                   serves_intercontroller_sasl_mechanism=serves_intercontroller_sasl_mechanism,
                                                   uses_controller_sasl_mechanism=uses_controller_sasl_mechanism,
                                                   kraft_tls=kraft_tls)
        # Ensure we have the correct client security protocol and SASL mechanism because they may have been mutated
        self._security_config.properties['security.protocol'] = self.security_protocol
        self._security_config.properties['sasl.mechanism'] = self.client_sasl_mechanism
        # Ensure we have the right inter-broker security protocol because it may have been mutated
        # since we cached our security config (ignore if this is an isolated KRaft controller quorum case; the
        # inter-broker security protocol is not used there).
        if (self.quorum_info.using_zk or self.quorum_info.has_brokers):
            # in case inter-broker SASL mechanism has changed without changing the inter-broker security protocol
            self._security_config.properties['sasl.mechanism.inter.broker.protocol'] = self.interbroker_sasl_mechanism
            if self._security_config.interbroker_security_protocol != self.interbroker_security_protocol:
                self._security_config.interbroker_security_protocol = self.interbroker_security_protocol
                self._security_config.calc_has_sasl()
                self._security_config.calc_has_ssl()
        for port in self.port_mappings.values():
            if port.open:
                self._security_config.enable_security_protocol(port.security_protocol, port.sasl_mechanism)
        if self.quorum_info.using_zk:
            if self.zk.zk_sasl:
                self._security_config.enable_sasl()
                self._security_config.zk_sasl = self.zk.zk_sasl
            if self.zk_client_secure:
                self._security_config.enable_ssl()
                self._security_config.zk_tls = self.zk_client_secure
        return self._security_config

    def open_port(self, listener_name):
        self.port_mappings[listener_name].open = True

    def close_port(self, listener_name):
        self.port_mappings[listener_name].open = False

    def start_minikdc_if_necessary(self, add_principals=""):
        has_sasl = self.security_config.has_sasl
        if has_sasl:
            if self.minikdc is None:
                other_service = self.isolated_kafka if self.isolated_kafka else self.controller_quorum if self.quorum_info.using_kraft else None
                if not other_service or not other_service.minikdc:
                    nodes_for_kdc = self.nodes.copy()
                    if other_service and other_service != self:
                        nodes_for_kdc += other_service.nodes
                    self.minikdc = MiniKdc(self.context, nodes_for_kdc, extra_principals = add_principals)
                    self.minikdc.start()
        else:
            self.minikdc = None
            if self.quorum_info.using_kraft:
                self.controller_quorum.minikdc = None
                if self.isolated_kafka:
                    self.isolated_kafka.minikdc = None

    def alive(self, node):
        return len(self.pids(node)) > 0

    def start(self, add_principals="", nodes_to_skip=[], isolated_controllers_to_skip=[], timeout_sec=60, **kwargs):
        """
        Start the Kafka broker and wait until it registers its ID in ZooKeeper
        Startup will be skipped for any nodes in nodes_to_skip. These nodes can be started later via add_broker
        """
        if self.quorum_info.using_zk and self.zk_client_secure and not self.zk.zk_client_secure_port:
            raise Exception("Unable to start Kafka: TLS to Zookeeper requested but Zookeeper secure port not enabled")

        if not all([node in self.nodes for node in nodes_to_skip]):
            raise Exception("nodes_to_skip should be a subset of this service's nodes")

        if self.quorum_info.has_brokers_and_controllers and (
                self.controller_security_protocol != self.intercontroller_security_protocol or
                self.controller_security_protocol in SecurityConfig.SASL_SECURITY_PROTOCOLS and self.controller_sasl_mechanism != self.intercontroller_sasl_mechanism):
            # This is not supported because both the broker and the controller take the first entry from
            # controller.listener.names and the value from sasl.mechanism.controller.protocol;
            # they share a single config, so they must both see/use identical values.
            raise Exception("Combined KRaft Brokers (%s/%s) and Controllers (%s/%s) cannot talk to Controllers via different security protocols" %
                            (self.controller_security_protocol, self.controller_sasl_mechanism,
                             self.intercontroller_security_protocol, self.intercontroller_sasl_mechanism))
        if self.quorum_info.using_zk or self.quorum_info.has_brokers:
            self.open_port(self.security_protocol)
            self.interbroker_listener.open = True
        # we have to wait to decide whether to open the controller port(s)
        # because it could be dependent on the particular node in the
        # combined case where the number of controllers could be less
        # than the number of nodes in the service

        self.start_minikdc_if_necessary(add_principals)

        # save the nodes we want to start in a member variable so we know which nodes to start and which to skip
        # in start_node
        self.nodes_to_start = [node for node in self.nodes if node not in nodes_to_skip]

        if self.quorum_info.using_zk:
            self._ensure_zk_chroot()

        if self.isolated_controller_quorum:
            self.isolated_controller_quorum.start(nodes_to_skip=isolated_controllers_to_skip)

        Service.start(self, **kwargs)

        if self.concurrent_start:
            # We didn't wait while starting each individual node, so wait for them all now
            for node in self.nodes_to_start:
                with node.account.monitor_log(KafkaService.STDOUT_STDERR_CAPTURE) as monitor:
                    monitor.offset = 0
                    self.wait_for_start(node, monitor, timeout_sec)

        if self.quorum_info.using_zk:
            self.logger.info("Waiting for brokers to register at ZK")

            expected_broker_ids = set(self.nodes_to_start)
            wait_until(lambda: {node for node in self.nodes_to_start if self.is_registered(node)} == expected_broker_ids,
                       timeout_sec=30, backoff_sec=1, err_msg="Kafka servers didn't register at ZK within 30 seconds")

        # Create topics if necessary
        if self.topics is not None:
            for topic, topic_cfg in self.topics.items():
                if topic_cfg is None:
                    topic_cfg = {}

                topic_cfg["topic"] = topic
                self.create_topic(topic_cfg)
        self.concurrent_start = False # in case it was True and this method was invoked directly instead of via start_concurrently()

    def start_concurrently(self, add_principals="", timeout_sec=60):
        self.concurrent_start = True # ensure it is True in case it has been explicitly disabled elsewhere
        self.start(add_principals = add_principals, timeout_sec=timeout_sec)
        self.concurrent_start = False

    def add_broker(self, node):
        """
        Starts an individual node. add_broker should only be used for nodes skipped during initial kafka service startup
        """
        if node in self.nodes_to_start:
            raise Exception("Add broker should only be used for nodes that haven't already been started")

        self.logger.debug(self.who_am_i() + ": killing processes and attempting to clean up before starting")
        # Added precaution - kill running processes, clean persistent files
        # try/except for each step, since each of these steps may fail if there are no processes
        # to kill or no files to remove
        try:
            self.stop_node(node)
        except Exception:
            pass

        try:
            self.clean_node(node)
        except Exception:
            pass

        if node not in self.nodes_to_start:
            self.nodes_to_start += [node]
        self.logger.debug("%s: starting node" % self.who_am_i(node))
        # ensure we wait for the broker to start by setting concurrent start to False for the invocation of start_node()
        orig_concurrent_start = self.concurrent_start
        self.concurrent_start = False
        self.start_node(node)
        self.concurrent_start = orig_concurrent_start

    def _ensure_zk_chroot(self):
        self.logger.info("Ensuring zk_chroot %s exists", self.zk_chroot)
        if self.zk_chroot:
            if not self.zk_chroot.startswith('/'):
                raise Exception("Zookeeper chroot must start with '/' but found " + self.zk_chroot)

            parts = self.zk_chroot.split('/')[1:]
            for i in range(len(parts)):
                self.zk.create('/' + '/'.join(parts[:i+1]))

    def set_protocol_and_port(self, node):
        listeners = []
        advertised_listeners = []
        protocol_map = []

        controller_listener_names = self.controller_listener_name_list(node)

        for port in self.port_mappings.values():
            if port.open:
                listeners.append(port.listener())
                if (self.dynamicRaftQuorum and quorum.NodeQuorumInfo(self.quorum_info, node).has_controller_role) or \
                        port.name not in controller_listener_names:
                    advertised_listeners.append(port.advertised_listener(node))
                protocol_map.append(port.listener_security_protocol())
        controller_sec_protocol = self.isolated_controller_quorum.controller_security_protocol if self.isolated_controller_quorum \
            else self.controller_security_protocol if self.quorum_info.has_brokers_and_controllers and not quorum.NodeQuorumInfo(self.quorum_info, node).has_controller_role \
            else None
        if controller_sec_protocol:
            protocol_map.append("%s:%s" % (self.controller_listener_name(controller_sec_protocol), controller_sec_protocol))

        self.listeners = ','.join(listeners)
        self.advertised_listeners = ','.join(advertised_listeners)
        self.listener_security_protocol_map = ','.join(protocol_map)
        if self.quorum_info.using_zk or self.quorum_info.has_brokers:
            self.interbroker_bootstrap_servers = self.__bootstrap_servers(self.interbroker_listener, True)

    def prop_file(self, node):
        self.set_protocol_and_port(node)

        #load template configs as dictionary
        config_template = self.render('kafka.properties', node=node, broker_id=self.idx(node),
                                      security_config=self.security_config, num_nodes=self.num_nodes,
                                      listener_security_config=self.listener_security_config)

        configs = dict( l.rstrip().split('=', 1) for l in config_template.split('\n')
                        if not l.startswith("#") and "=" in l )

        #load specific test override configs
        override_configs = KafkaConfig(**node.config)
        if self.quorum_info.using_zk or self.quorum_info.has_brokers:
            override_configs[config_property.ADVERTISED_HOSTNAME] = node.account.hostname
        if self.quorum_info.using_zk or self.zk:
            override_configs[config_property.ZOOKEEPER_CONNECT] = self.zk_connect_setting()
            if self.zk_client_secure:
                override_configs[config_property.ZOOKEEPER_SSL_CLIENT_ENABLE] = 'true'
                override_configs[config_property.ZOOKEEPER_CLIENT_CNXN_SOCKET] = 'org.apache.zookeeper.ClientCnxnSocketNetty'
            else:
                override_configs[config_property.ZOOKEEPER_SSL_CLIENT_ENABLE] = 'false'

        if self.use_new_coordinator is not None:
            override_configs[config_property.NEW_GROUP_COORDINATOR_ENABLE] = str(self.use_new_coordinator)

        if self.consumer_group_migration_policy is not None:
            override_configs[config_property.CONSUMER_GROUP_MIGRATION_POLICY] = str(self.consumer_group_migration_policy)

        for prop in self.server_prop_overrides:
            override_configs[prop[0]] = prop[1]

        for prop in self.per_node_server_prop_overrides.get(self.idx(node), []):
            override_configs[prop[0]] = prop[1]

        #update template configs with test override configs
        configs.update(override_configs)

        prop_file = self.render_configs(configs)
        return prop_file

    def render_configs(self, configs):
        """Render self as a series of lines key=val\n, and do so in a consistent order. """
        keys = [k for k in configs.keys()]
        keys.sort()

        s = ""
        for k in keys:
            s += "%s=%s\n" % (k, str(configs[k]))
        return s

    def start_cmd(self, node):
        """
        To bring up kafka using native image, pass following in ducktape options
        --globals '{"kafka_mode": "native"}'
        """
        kafka_mode = self.context.globals.get("kafka_mode", "")
        cmd = f"export KAFKA_MODE={kafka_mode}; "
        cmd += "export JMX_PORT=%d; " % self.jmx_port
        cmd += "export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\"; " % self.LOG4J_CONFIG
        heap_kafka_opts = "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s" % \
                          self.logs["kafka_heap_dump_file"]["path"]
        security_kafka_opts = self.security_config.kafka_opts.strip('\"')

        cmd += fix_opts_for_new_jvm(node)
        cmd += "export KAFKA_OPTS=\"%s %s %s\"; " % (heap_kafka_opts, security_kafka_opts, self.extra_kafka_opts)
        cmd += "%s %s 1>> %s 2>> %s &" % \
               (self.path.script("kafka-server-start.sh", node),
                KafkaService.CONFIG_FILE,
                KafkaService.STDOUT_STDERR_CAPTURE,
                KafkaService.STDOUT_STDERR_CAPTURE)
        return cmd

    def controller_listener_name_list(self, node):
        if self.quorum_info.using_zk:
            return []
        broker_to_controller_listener_name = self.controller_listener_name(self.controller_quorum.controller_security_protocol)
        # Brokers always use the first controller listener, so include a second, inter-controller listener if and only if:
        # 1) the node is a controller node
        # 2) the inter-controller listener name differs from the broker-to-controller listener name
        return [broker_to_controller_listener_name, self.controller_listener_name(self.controller_quorum.intercontroller_security_protocol)] \
            if (quorum.NodeQuorumInfo(self.quorum_info, node).has_controller_role and
                self.controller_quorum.intercontroller_security_protocol != self.controller_quorum.controller_security_protocol) \
            else [broker_to_controller_listener_name]

    def start_node(self, node, timeout_sec=60, **kwargs):
        if node not in self.nodes_to_start:
            return
        node.account.mkdirs(KafkaService.PERSISTENT_ROOT)

        self.node_quorum_info = quorum.NodeQuorumInfo(self.quorum_info, node)
        if self.quorum_info.has_controllers:
            for controller_listener in self.controller_listener_name_list(node):
                if self.node_quorum_info.has_controller_role:
                    self.open_port(controller_listener)
                else: # combined case where node doesn't have a controller
                    self.close_port(controller_listener)

        self.security_config.setup_node(node)
        if self.quorum_info.using_zk or self.quorum_info.has_brokers: # TODO: SCRAM currently unsupported for controller quorum
            self.maybe_setup_broker_scram_credentials(node)

        if self.quorum_info.using_kraft:
            # define controller.quorum.bootstrap.servers or controller.quorum.voters text
            security_protocol_to_use = self.controller_quorum.controller_security_protocol
            first_node_id = 1 if self.quorum_info.has_brokers_and_controllers else config_property.FIRST_CONTROLLER_ID
            if self.dynamicRaftQuorum:
                self.controller_quorum_bootstrap_servers = ','.join(["{}:{}".format(node.account.hostname,
                                                                                    config_property.FIRST_CONTROLLER_PORT +
                                                                                    KafkaService.SECURITY_PROTOCOLS.index(security_protocol_to_use))
                                                                     for node in self.controller_quorum.nodes[:self.controller_quorum.num_nodes_controller_role]])
            else:
                self.controller_quorum_voters = ','.join(["{}@{}:{}".format(self.controller_quorum.idx(node) +
                                                                            first_node_id - 1,
                                                                            node.account.hostname,
                                                                            config_property.FIRST_CONTROLLER_PORT +
                                                                            KafkaService.SECURITY_PROTOCOLS.index(security_protocol_to_use))
                                                          for node in self.controller_quorum.nodes[:self.controller_quorum.num_nodes_controller_role]])
            # define controller.listener.names
            self.controller_listener_names = ','.join(self.controller_listener_name_list(node))
            # define sasl.mechanism.controller.protocol to match the isolated quorum if one exists
            if self.isolated_controller_quorum:
                self.controller_sasl_mechanism = self.isolated_controller_quorum.controller_sasl_mechanism

        prop_file = self.prop_file(node)
        self.logger.info("kafka.properties:")
        self.logger.info(prop_file)
        node.account.create_file(KafkaService.CONFIG_FILE, prop_file)
        node.account.create_file(self.LOG4J_CONFIG, self.render('log4j.properties', log_dir=KafkaService.OPERATIONAL_LOG_DIR))

        if self.quorum_info.using_kraft:
            # format log directories if necessary
            kafka_storage_script = self.path.script("kafka-storage.sh", node)
            cmd = "%s format --ignore-formatted --config %s --cluster-id %s" % (kafka_storage_script, KafkaService.CONFIG_FILE, config_property.CLUSTER_ID)
            if self.dynamicRaftQuorum:
                if self.node_quorum_info.has_controller_role:
                    if self.standalone_controller_bootstrapped:
                        cmd += " --no-initial-controllers"
                    else:
                        cmd += " --standalone"
                        self.standalone_controller_bootstrapped = True
            self.logger.info("Running log directory format command...\n%s" % cmd)
            node.account.ssh(cmd)

        cmd = self.start_cmd(node)
        self.logger.debug("Attempting to start KafkaService %s on %s with command: %s" %\
                          ("concurrently" if self.concurrent_start else "serially", str(node.account), cmd))
        if self.node_quorum_info.has_controller_role and self.node_quorum_info.has_broker_role:
            self.combined_nodes_started += 1
        if self.concurrent_start:
            node.account.ssh(cmd) # and then don't wait for the startup message
        else:
            with node.account.monitor_log(KafkaService.STDOUT_STDERR_CAPTURE) as monitor:
                node.account.ssh(cmd)
                self.wait_for_start(node, monitor, timeout_sec)

    def wait_for_start(self, node, monitor, timeout_sec=60):
        # Kafka 1.0.0 and higher don't have a space between "Kafka" and "Server"
        monitor.wait_until("Kafka\s*Server.*started", timeout_sec=timeout_sec, backoff_sec=.25,
                           err_msg="Kafka server didn't finish startup in %d seconds" % timeout_sec)

        if self.quorum_info.using_zk or self.quorum_info.has_brokers: # TODO: SCRAM currently unsupported for controller quorum
            # Credentials for inter-broker communication are created before starting Kafka.
            # Client credentials are created after starting Kafka so that both loading of
            # existing credentials from ZK and dynamic update of credentials in Kafka are tested.
            # We use the admin client and connect as the broker user when creating the client (non-broker) credentials
            # if Kafka supports KIP-554, otherwise we use ZooKeeper.
            self.maybe_setup_client_scram_credentials(node)

        self.start_jmx_tool(self.idx(node), node)
        if not self.pids(node):
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    def upgrade_metadata_version(self, new_version):
        self.run_features_command("upgrade", new_version)

    def downgrade_metadata_version(self, new_version):
        self.run_features_command("downgrade", new_version)

    def run_features_command(self, op, new_version):
        cmd = self.path.script("kafka-features.sh ")
        cmd += "--bootstrap-server %s " % self.bootstrap_servers()
        cmd += "%s --metadata %s" % (op, new_version)
        self.logger.info("Running %s command...\n%s" % (op, cmd))
        self.nodes[0].account.ssh(cmd)

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i %s | grep -v grep | awk '{print $1}'" % self.java_class_name()
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def signal_node(self, node, sig=signal.SIGTERM):
        pids = self.pids(node)
        for pid in pids:
            node.account.signal(pid, sig)

    def signal_leader(self, topic, partition=0, sig=signal.SIGTERM):
        leader = self.leader(topic, partition)
        self.signal_node(leader, sig)

    def controllers_required_for_quorum(self):
        """
        Assume N = the total number of controller nodes in the cluster, and positive
        For N=1, we need 1 controller to be running to have a quorum
        For N=2, we need 2 controllers
        For N=3, we need 2 controllers
        For N=4, we need 3 controllers
        For N=5, we need 3 controllers

        :return: the number of controller nodes that must be started for there to be a quorum
        """
        return math.ceil((1 + self.num_nodes_controller_role) / 2)

    def stop_node(self, node, clean_shutdown=True, timeout_sec=60):
        pids = self.pids(node)
        cluster_has_combined_controllers = self.quorum_info.has_brokers and self.quorum_info.has_controllers
        force_sigkill_due_to_too_few_combined_controllers =\
            clean_shutdown and cluster_has_combined_controllers\
            and self.combined_nodes_started < self.controllers_required_for_quorum()
        if force_sigkill_due_to_too_few_combined_controllers:
            self.logger.info("Forcing node to stop via SIGKILL due to too few combined KRaft controllers: %i/%i" %\
                             (self.combined_nodes_started, self.num_nodes_controller_role))

        sig = signal.SIGTERM if clean_shutdown and not force_sigkill_due_to_too_few_combined_controllers else signal.SIGKILL

        for pid in pids:
            node.account.signal(pid, sig, allow_fail=False)

        node_quorum_info = quorum.NodeQuorumInfo(self.quorum_info, node)
        node_has_combined_controllers = node_quorum_info.has_controller_role and node_quorum_info.has_broker_role
        if pids and node_has_combined_controllers:
            self.combined_nodes_started -= 1

        try:
            wait_until(lambda: not self.pids(node), timeout_sec=timeout_sec,
                       err_msg="Kafka node failed to stop in %d seconds" % timeout_sec)
        except Exception:
            if node_has_combined_controllers:
                # it didn't stop
                self.combined_nodes_started += 1
            self.thread_dump(node)
            raise

    def thread_dump(self, node):
        for pid in self.pids(node):
            try:
                node.account.signal(pid, signal.SIGQUIT, allow_fail=True)
            except:
                self.logger.warn("Could not dump threads on node")

    def clean_node(self, node):
        JmxMixin.clean_node(self, node)
        self.security_config.clean_node(node)
        node.account.kill_process(self.java_class_name(),
                                  clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % KafkaService.PERSISTENT_ROOT, allow_fail=False)

    def kafka_metadata_quorum_cmd(self, node, kafka_security_protocol=None, use_controller_bootstrap=False):
        if kafka_security_protocol is None:
            # it wasn't specified, so use the inter-broker/controller security protocol if it is PLAINTEXT,
            # otherwise use the client security protocol
            if self.interbroker_security_protocol == SecurityConfig.PLAINTEXT:
                security_protocol_to_use = SecurityConfig.PLAINTEXT
            else:
                security_protocol_to_use = self.security_protocol
        else:
            security_protocol_to_use = kafka_security_protocol
        if use_controller_bootstrap:
            bootstrap = "--bootstrap-controller {}".format(
                self.bootstrap_controllers("CONTROLLER_{}".format(security_protocol_to_use)))
        else:
            bootstrap = "--bootstrap-server {}".format(self.bootstrap_servers(security_protocol_to_use))
        kafka_metadata_script = self.path.script("kafka-metadata-quorum.sh", node)
        return "{} {}".format(kafka_metadata_script, bootstrap)

    def kafka_topics_cmd_with_optional_security_settings(self, node, force_use_zk_connection, kafka_security_protocol=None, offline_nodes=[]):
        if self.quorum_info.using_kraft and not self.quorum_info.has_brokers:
            raise Exception("Must invoke kafka-topics against a broker, not a KRaft controller")
        if force_use_zk_connection:
            bootstrap_server_or_zookeeper = "--zookeeper %s" % (self.zk_connect_setting())
            skip_optional_security_settings = True
        else:
            if kafka_security_protocol is None:
                # it wasn't specified, so use the inter-broker security protocol if it is PLAINTEXT,
                # otherwise use the client security protocol
                if self.interbroker_security_protocol == SecurityConfig.PLAINTEXT:
                    security_protocol_to_use = SecurityConfig.PLAINTEXT
                else:
                    security_protocol_to_use = self.security_protocol
            else:
                security_protocol_to_use = kafka_security_protocol
            bootstrap_server_or_zookeeper = "--bootstrap-server %s" % (self.bootstrap_servers(security_protocol_to_use, offline_nodes=offline_nodes))
            skip_optional_security_settings = security_protocol_to_use == SecurityConfig.PLAINTEXT
        if skip_optional_security_settings:
            optional_jass_krb_system_props_prefix = ""
            optional_command_config_suffix = ""
        else:
            # we need security configs because aren't going to ZooKeeper and we aren't using PLAINTEXT
            if (security_protocol_to_use == self.interbroker_security_protocol):
                # configure JAAS to provide the broker's credentials
                # since this is an authenticating cluster and we are going to use the inter-broker security protocol
                jaas_conf_prop = KafkaService.ADMIN_CLIENT_AS_BROKER_JAAS_CONF_PROPERTY
                use_inter_broker_mechanism_for_client = True
            else:
                # configure JAAS to provide the typical client credentials
                jaas_conf_prop = KafkaService.JAAS_CONF_PROPERTY
                use_inter_broker_mechanism_for_client = False
            # We are either using SASL (SASL_SSL or SASL_PLAINTEXT) or we are using SSL
            using_sasl = security_protocol_to_use != "SSL"
            optional_jass_krb_system_props_prefix = "KAFKA_OPTS='-D%s -D%s' " % (jaas_conf_prop, KafkaService.KRB5_CONF) if using_sasl else ""
            optional_command_config_suffix = " --command-config <(echo '%s')" % (self.security_config.client_config(use_inter_broker_mechanism_for_client = use_inter_broker_mechanism_for_client))
        kafka_topic_script = self.path.script("kafka-topics.sh", node)
        return "%s%s %s%s" % \
               (optional_jass_krb_system_props_prefix, kafka_topic_script,
                bootstrap_server_or_zookeeper, optional_command_config_suffix)

    def kafka_configs_cmd_with_optional_security_settings(self, node, force_use_zk_connection, kafka_security_protocol = None):
        if self.quorum_info.using_kraft and not self.quorum_info.has_brokers:
            raise Exception("Must invoke kafka-configs against a broker, not a KRaft controller")
        if force_use_zk_connection:
            # kafka-configs supports a TLS config file, so include it if there is one
            bootstrap_server_or_zookeeper = "--zookeeper %s %s" % (self.zk_connect_setting(), self.zk.zkTlsConfigFileOption())
            skip_optional_security_settings = True
        else:
            if kafka_security_protocol is None:
                # it wasn't specified, so use the inter-broker security protocol if it is PLAINTEXT,
                # otherwise use the client security protocol
                if self.interbroker_security_protocol == SecurityConfig.PLAINTEXT:
                    security_protocol_to_use = SecurityConfig.PLAINTEXT
                else:
                    security_protocol_to_use = self.security_protocol
            else:
                security_protocol_to_use = kafka_security_protocol
            bootstrap_server_or_zookeeper = "--bootstrap-server %s" % (self.bootstrap_servers(security_protocol_to_use))
            skip_optional_security_settings = security_protocol_to_use == SecurityConfig.PLAINTEXT
        if skip_optional_security_settings:
            optional_jass_krb_system_props_prefix = ""
            optional_command_config_suffix = ""
        else:
            # we need security configs because aren't going to ZooKeeper and we aren't using PLAINTEXT
            if (security_protocol_to_use == self.interbroker_security_protocol):
                # configure JAAS to provide the broker's credentials
                # since this is an authenticating cluster and we are going to use the inter-broker security protocol
                jaas_conf_prop = KafkaService.ADMIN_CLIENT_AS_BROKER_JAAS_CONF_PROPERTY
                use_inter_broker_mechanism_for_client = True
            else:
                # configure JAAS to provide the typical client credentials
                jaas_conf_prop = KafkaService.JAAS_CONF_PROPERTY
                use_inter_broker_mechanism_for_client = False
            # We are either using SASL (SASL_SSL or SASL_PLAINTEXT) or we are using SSL
            using_sasl = security_protocol_to_use != "SSL"
            optional_jass_krb_system_props_prefix = "KAFKA_OPTS='-D%s -D%s' " % (jaas_conf_prop, KafkaService.KRB5_CONF) if using_sasl else ""
            optional_command_config_suffix = " --command-config <(echo '%s')" % (self.security_config.client_config(use_inter_broker_mechanism_for_client = use_inter_broker_mechanism_for_client))
        kafka_config_script = self.path.script("kafka-configs.sh", node)
        return "%s%s %s%s" % \
               (optional_jass_krb_system_props_prefix, kafka_config_script,
                bootstrap_server_or_zookeeper, optional_command_config_suffix)

    def maybe_setup_broker_scram_credentials(self, node):
        security_config = self.security_config
        # we only need to create broker credentials when the broker mechanism is SASL/SCRAM
        if security_config.is_sasl(self.interbroker_security_protocol) and security_config.is_sasl_scram(self.interbroker_sasl_mechanism):
            force_use_zk_connection = True # we are bootstrapping these credentials before Kafka is started
            cmd = fix_opts_for_new_jvm(node)
            cmd += "%(kafka_configs_cmd)s --entity-name %(user)s --entity-type users --alter --add-config %(mechanism)s=[password=%(password)s]" % {
                'kafka_configs_cmd': self.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection),
                'user': SecurityConfig.SCRAM_BROKER_USER,
                'mechanism': self.interbroker_sasl_mechanism,
                'password': SecurityConfig.SCRAM_BROKER_PASSWORD
            }
            node.account.ssh(cmd)

    def maybe_setup_client_scram_credentials(self, node):
        security_config = self.security_config
        # we only need to create client credentials when the client mechanism is SASL/SCRAM
        if security_config.is_sasl(self.security_protocol) and security_config.is_sasl_scram(self.client_sasl_mechanism):
            force_use_zk_connection = not self.all_nodes_configs_command_uses_bootstrap_server_scram()
            # ignored if forcing the use of Zookeeper, but we need a value to send, so calculate it anyway
            if self.interbroker_security_protocol == SecurityConfig.PLAINTEXT:
                kafka_security_protocol = self.interbroker_security_protocol
            else:
                kafka_security_protocol = self.security_protocol
            cmd = fix_opts_for_new_jvm(node)
            cmd += "%(kafka_configs_cmd)s --entity-name %(user)s --entity-type users --alter --add-config %(mechanism)s=[password=%(password)s]" % {
                'kafka_configs_cmd': self.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection, kafka_security_protocol),
                'user': SecurityConfig.SCRAM_CLIENT_USER,
                'mechanism': self.client_sasl_mechanism,
                'password': SecurityConfig.SCRAM_CLIENT_PASSWORD
            }
            node.account.ssh(cmd)

    def node_inter_broker_protocol_version(self, node):
        if config_property.INTER_BROKER_PROTOCOL_VERSION in node.config:
            return KafkaVersion(node.config[config_property.INTER_BROKER_PROTOCOL_VERSION])
        return node.version

    def all_nodes_topic_command_supports_bootstrap_server(self):
        for node in self.nodes:
            if not node.version.topic_command_supports_bootstrap_server():
                return False
        return True

    def all_nodes_topic_command_supports_if_not_exists_with_bootstrap_server(self):
        for node in self.nodes:
            if not node.version.topic_command_supports_if_not_exists_with_bootstrap_server():
                return False
        return True

    def all_nodes_configs_command_uses_bootstrap_server(self):
        for node in self.nodes:
            if not node.version.kafka_configs_command_uses_bootstrap_server():
                return False
        return True

    def all_nodes_configs_command_uses_bootstrap_server_scram(self):
        for node in self.nodes:
            if not node.version.kafka_configs_command_uses_bootstrap_server_scram():
                return False
        return True

    def all_nodes_acl_command_supports_bootstrap_server(self):
        for node in self.nodes:
            if not node.version.acl_command_supports_bootstrap_server():
                return False
        return True

    def all_nodes_reassign_partitions_command_supports_bootstrap_server(self):
        for node in self.nodes:
            if not node.version.reassign_partitions_command_supports_bootstrap_server():
                return False
        return True

    def all_nodes_support_topic_ids(self):
        if self.quorum_info.using_kraft: return True
        for node in self.nodes:
            if not self.node_inter_broker_protocol_version(node).supports_topic_ids_when_using_zk():
                return False
        return True

    def create_topic(self, topic_cfg, node=None):
        """Run the admin tool create topic command.
        Specifying node is optional, and may be done if for different kafka nodes have different versions,
        and we care where command gets run.

        If the node is not specified, run the command from self.nodes[0]
        """
        if node is None:
            node = self.nodes[0]
        self.logger.info("Creating topic %s with settings %s",
                         topic_cfg["topic"], topic_cfg)

        force_use_zk_connection = not self.all_nodes_topic_command_supports_bootstrap_server() or\
                            (topic_cfg.get('if-not-exists', False) and not self.all_nodes_topic_command_supports_if_not_exists_with_bootstrap_server())

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%(kafka_topics_cmd)s --create --topic %(topic)s " % {
            'kafka_topics_cmd': self.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection),
            'topic': topic_cfg.get("topic"),
        }
        if 'replica-assignment' in topic_cfg:
            cmd += " --replica-assignment %(replica-assignment)s" % {
                'replica-assignment': topic_cfg.get('replica-assignment')
            }
        else:
            cmd += " --partitions %(partitions)d --replication-factor %(replication-factor)d" % {
                'partitions': topic_cfg.get('partitions', 1),
                'replication-factor': topic_cfg.get('replication-factor', 1)
            }

        if topic_cfg.get('if-not-exists', False):
            cmd += ' --if-not-exists'

        if "configs" in topic_cfg.keys() and topic_cfg["configs"] is not None:
            for config_name, config_value in topic_cfg["configs"].items():
                cmd += " --config %s=%s" % (config_name, str(config_value))

        self.logger.info("Running topic creation command...\n%s" % cmd)
        node.account.ssh(cmd)

    def delete_topic(self, topic, node=None):
        """
        Delete a topic with the topics command
        :param topic:
        :param node:
        :return:
        """
        if node is None:
            node = self.nodes[0]
        self.logger.info("Deleting topic %s" % topic)

        force_use_zk_connection = not self.all_nodes_topic_command_supports_bootstrap_server()

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --topic %s --delete" % \
               (self.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection), topic)
        self.logger.info("Running topic delete command...\n%s" % cmd)
        node.account.ssh(cmd)

    def has_under_replicated_partitions(self):
        """
        Check whether the cluster has under-replicated partitions.

        :return True if there are under-replicated partitions, False otherwise.
        """
        return len(self.describe_under_replicated_partitions()) > 0

    def await_no_under_replicated_partitions(self, timeout_sec=30):
        """
        Wait for all under-replicated partitions to clear.

        :param timeout_sec: the maximum time in seconds to wait
        """
        wait_until(lambda: not self.has_under_replicated_partitions(),
                   timeout_sec = timeout_sec,
                   err_msg="Timed out waiting for under-replicated-partitions to clear")

    def describe_under_replicated_partitions(self):
        """
        Use the topic tool to find the under-replicated partitions in the cluster.

        :return the under-replicated partitions as a list of dictionaries
                (e.g. [{"topic": "foo", "partition": 1}, {"topic": "bar", "partition": 0}, ... ])
        """

        node = self.nodes[0]
        force_use_zk_connection = not node.version.topic_command_supports_bootstrap_server()

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --describe --under-replicated-partitions" % \
            self.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection)

        self.logger.debug("Running topic command to describe under-replicated partitions\n%s" % cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        under_replicated_partitions = self.parse_describe_topic(output)["partitions"]
        self.logger.debug("Found %d under-replicated-partitions" % len(under_replicated_partitions))

        return under_replicated_partitions

    def describe_topic(self, topic, node=None, offline_nodes=[]):
        if node is None:
            node = self.nodes[0]

        force_use_zk_connection = not self.all_nodes_topic_command_supports_bootstrap_server()

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --topic %s --describe" % \
               (self.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection, offline_nodes=offline_nodes), topic)

        self.logger.info("Running topic describe command...\n%s" % cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line
        return output

    def list_topics(self, node=None):
        if node is None:
            node = self.nodes[0]

        force_use_zk_connection = not self.all_nodes_topic_command_supports_bootstrap_server()

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --list" % (self.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection))
        for line in node.account.ssh_capture(cmd):
            if not line.startswith("SLF4J"):
                yield line.rstrip()

    def alter_message_format(self, topic, msg_format_version, node=None):
        if node is None:
            node = self.nodes[0]
        self.logger.info("Altering message format version for topic %s with format %s", topic, msg_format_version)

        force_use_zk_connection = not self.all_nodes_configs_command_uses_bootstrap_server()

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --entity-name %s --entity-type topics --alter --add-config message.format.version=%s" % \
              (self.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection), topic, msg_format_version)
        self.logger.info("Running alter message format command...\n%s" % cmd)
        node.account.ssh(cmd)

    def set_unclean_leader_election(self, topic, value=True, node=None):
        if node is None:
            node = self.nodes[0]
        if value is True:
            self.logger.info("Enabling unclean leader election for topic %s", topic)
        else:
            self.logger.info("Disabling unclean leader election for topic %s", topic)

        force_use_zk_connection = not self.all_nodes_configs_command_uses_bootstrap_server()

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --entity-name %s --entity-type topics --alter --add-config unclean.leader.election.enable=%s" % \
              (self.kafka_configs_cmd_with_optional_security_settings(node, force_use_zk_connection), topic, str(value).lower())
        self.logger.info("Running alter unclean leader command...\n%s" % cmd)
        node.account.ssh(cmd)

    def kafka_acls_cmd_with_optional_security_settings(self, node, force_use_zk_connection, kafka_security_protocol = None, override_command_config = None):
        if self.quorum_info.using_kraft and not self.quorum_info.has_brokers:
            raise Exception("Must invoke kafka-acls against a broker, not a KRaft controller")
        force_use_zk_connection = force_use_zk_connection or not self.all_nodes_acl_command_supports_bootstrap_server
        if force_use_zk_connection:
            bootstrap_server_or_authorizer_zk_props = "--authorizer-properties zookeeper.connect=%s" % (self.zk_connect_setting())
            skip_optional_security_settings = True
        else:
            if kafka_security_protocol is None:
                # it wasn't specified, so use the inter-broker security protocol if it is PLAINTEXT,
                # otherwise use the client security protocol
                if self.interbroker_security_protocol == SecurityConfig.PLAINTEXT:
                    security_protocol_to_use = SecurityConfig.PLAINTEXT
                else:
                    security_protocol_to_use = self.security_protocol
            else:
                security_protocol_to_use = kafka_security_protocol
            bootstrap_server_or_authorizer_zk_props = "--bootstrap-server %s" % (self.bootstrap_servers(security_protocol_to_use))
            skip_optional_security_settings = security_protocol_to_use == SecurityConfig.PLAINTEXT
        if skip_optional_security_settings:
            optional_jass_krb_system_props_prefix = ""
            optional_command_config_suffix = ""
        else:
            # we need security configs because aren't going to ZooKeeper and we aren't using PLAINTEXT
            if (security_protocol_to_use == self.interbroker_security_protocol):
                # configure JAAS to provide the broker's credentials
                # since this is an authenticating cluster and we are going to use the inter-broker security protocol
                jaas_conf_prop = KafkaService.ADMIN_CLIENT_AS_BROKER_JAAS_CONF_PROPERTY
                use_inter_broker_mechanism_for_client = True
            else:
                # configure JAAS to provide the typical client credentials
                jaas_conf_prop = KafkaService.JAAS_CONF_PROPERTY
                use_inter_broker_mechanism_for_client = False
            # We are either using SASL (SASL_SSL or SASL_PLAINTEXT) or we are using SSL
            using_sasl = security_protocol_to_use != "SSL"
            optional_jass_krb_system_props_prefix = "KAFKA_OPTS='-D%s -D%s' " % (jaas_conf_prop, KafkaService.KRB5_CONF) if using_sasl else ""
            if override_command_config is None:
                optional_command_config_suffix = " --command-config <(echo '%s')" % (self.security_config.client_config(use_inter_broker_mechanism_for_client = use_inter_broker_mechanism_for_client))
            else:
                optional_command_config_suffix = " --command-config %s" % (override_command_config)
        kafka_acls_script = self.path.script("kafka-acls.sh", node)
        return "%s%s %s%s" % \
               (optional_jass_krb_system_props_prefix, kafka_acls_script,
                bootstrap_server_or_authorizer_zk_props, optional_command_config_suffix)

    def run_cli_tool(self, node, cmd):
        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            if not line.startswith("SLF4J"):
                output += line
        self.logger.debug(output)
        return output

    def parse_describe_topic(self, topic_description):
        """Parse output of kafka-topics.sh --describe (or describe_topic() method above), which is a string of form
        Topic: test_topic\tTopicId: <topic_id>\tPartitionCount: 2\tReplicationFactor: 2\tConfigs:
            Topic: test_topic\tPartition: 0\tLeader: 3\tReplicas: 3,1\tIsr: 3,1
            Topic: test_topic\tPartition: 1\tLeader: 1\tReplicas: 1,2\tIsr: 1,2
        into a dictionary structure appropriate for use with reassign-partitions tool:
        {
            "partitions": [
                {"topic": "test_topic", "partition": 0, "replicas": [3, 1]},
                {"topic": "test_topic", "partition": 1, "replicas": [1, 2]}
            ]
        }
        """
        lines = map(lambda x: x.strip(), topic_description.split("\n"))
        partitions = []
        for line in lines:
            m = re.match(".*Leader:.*", line)
            if m is None:
                continue

            fields = line.split("\t")
            fields = dict([field.split(": ") for field in fields if len(field.split(": ")) == 2])
            partitions.append(
                {"topic": fields["Topic"],
                 "partition": int(fields["Partition"]),
                 "replicas": list(map(int, fields["Replicas"].split(',')))
                 })

        return {"partitions": partitions}


    def _connect_setting_reassign_partitions(self, node):
        if self.all_nodes_reassign_partitions_command_supports_bootstrap_server():
            return "--bootstrap-server %s " % self.bootstrap_servers(self.security_protocol)
        else:
            return "--zookeeper %s " % self.zk_connect_setting()

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
        cmd = fix_opts_for_new_jvm(node)
        cmd += "echo %s > %s && " % (json_str, json_file)
        cmd += "%s " % self.path.script("kafka-reassign-partitions.sh", node)
        cmd += self._connect_setting_reassign_partitions(node)
        cmd += "--reassignment-json-file %s " % json_file
        cmd += "--verify "
        cmd += "&& sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Verifying partition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug(output)

        if re.match(".*Reassignment of partition.*failed.*",
                    output.replace('\n', '')) is not None:
            return False

        if re.match(".*is still in progress.*",
                    output.replace('\n', '')) is not None:
            return False

        return True

    def execute_reassign_partitions(self, reassignment, node=None,
                                    throttle=None):
        """Run the reassign partitions admin tool in "verify" mode
        """
        if node is None:
            node = self.nodes[0]
        json_file = "/tmp/%s_reassign.json" % str(time.time())

        # reassignment to json
        json_str = json.dumps(reassignment)
        json_str = json.dumps(json_str)

        # create command
        cmd = fix_opts_for_new_jvm(node)
        cmd += "echo %s > %s && " % (json_str, json_file)
        cmd += "%s " % self.path.script( "kafka-reassign-partitions.sh", node)
        cmd += self._connect_setting_reassign_partitions(node)
        cmd += "--reassignment-json-file %s " % json_file
        cmd += "--execute"
        if throttle is not None:
            cmd += " --throttle %d" % throttle
        cmd += " && sleep 1 && rm -f %s" % json_file

        # send command
        self.logger.info("Executing parition reassignment...")
        self.logger.debug(cmd)
        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        self.logger.debug("Verify partition reassignment:")
        self.logger.debug(output)

    def search_data_files(self, topic, messages):
        """Check if a set of messages made it into the Kafka data files. Note that
        this method takes no account of replication. It simply looks for the
        payload in all the partition files of the specified topic. 'messages' should be
        an array of numbers. The list of missing messages is returned.
        """
        payload_match = "payload: " + "$|payload: ".join(str(x) for x in messages) + "$"
        found = set([])
        self.logger.debug("number of unique missing messages we will search for: %d",
                          len(messages))
        for node in self.nodes:
            # Grab all .log files in directories prefixed with this topic
            files = node.account.ssh_capture("find %s* -regex  '.*/%s-.*/[^/]*.log'" % (KafkaService.DATA_LOG_DIR_PREFIX, topic))

            # Check each data file to see if it contains the messages we want
            for log in files:
                cmd = fix_opts_for_new_jvm(node)
                cmd += "%s kafka.tools.DumpLogSegments --print-data-log --files %s | grep -E \"%s\"" % \
                      (self.path.script("kafka-run-class.sh", node), log.strip(), payload_match)

                for line in node.account.ssh_capture(cmd, allow_fail=True):
                    for val in messages:
                        if line.strip().endswith("payload: "+str(val)):
                            self.logger.debug("Found %s in data-file [%s] in line: [%s]" % (val, log.strip(), line.strip()))
                            found.add(val)

        self.logger.debug("Number of unique messages found in the log: %d",
                          len(found))
        missing = list(set(messages) - found)

        if len(missing) > 0:
            self.logger.warn("The following values were not found in the data files: " + str(missing))

        return missing

    def restart_cluster(self, clean_shutdown=True, timeout_sec=60, after_each_broker_restart=None, *args):
        # We do not restart the isolated controller quorum if it exists.
        # This is not widely used -- it typically appears in rolling upgrade tests --
        # so we will let tests explicitly decide if/when to restart any isolated controller quorum.
        for node in self.nodes:
            self.restart_node(node, clean_shutdown=clean_shutdown, timeout_sec=timeout_sec)
            if after_each_broker_restart is not None:
                after_each_broker_restart(*args)

    def restart_node(self, node, clean_shutdown=True, timeout_sec=60):
        """Restart the given node."""
        # ensure we wait for the broker to start by setting concurrent start to False for the invocation of start_node()
        orig_concurrent_start = self.concurrent_start
        self.concurrent_start = False
        self.stop_node(node, clean_shutdown, timeout_sec)
        self.start_node(node, timeout_sec)
        self.concurrent_start = orig_concurrent_start

    def _describe_topic_line_for_partition(self, partition, describe_topic_output):
        # Lines look like this: Topic: test_topic	Partition: 0	Leader: 3	Replicas: 3,2	Isr: 3,2
        grep_for = "Partition: %i\t" % (partition) # be sure to include trailing tab, otherwise 1 might match 10 (for example)
        found_lines = [line for line in describe_topic_output.splitlines() if grep_for in line]
        return None if not found_lines else found_lines[0]

    def isr_idx_list(self, topic, partition=0, node=None, offline_nodes=[]):
        """ Get in-sync replica list the given topic and partition.
        """
        if node is None:
          node = self.nodes[0]
        if not self.all_nodes_topic_command_supports_bootstrap_server():
            self.logger.debug("Querying zookeeper to find in-sync replicas for topic %s and partition %d" % (topic, partition))
            zk_path = "/brokers/topics/%s/partitions/%d/state" % (topic, partition)
            partition_state = self.zk.query(zk_path, chroot=self.zk_chroot)

            if partition_state is None:
                raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))

            partition_state = json.loads(partition_state)
            self.logger.info(partition_state)

            isr_idx_list = partition_state["isr"]
        else:
            self.logger.debug("Querying Kafka Admin API to find in-sync replicas for topic %s and partition %d" % (topic, partition))
            describe_output = self.describe_topic(topic, node, offline_nodes=offline_nodes)
            self.logger.debug(describe_output)
            requested_partition_line = self._describe_topic_line_for_partition(partition, describe_output)
            # e.g. Topic: test_topic	Partition: 0	Leader: 3	Replicas: 3,2	Isr: 3,2
            if not requested_partition_line:
                raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))
            isr_csv = requested_partition_line.split()[9] # 10th column from above
            isr_idx_list = [int(i) for i in isr_csv.split(",")]

        self.logger.info("Isr for topic %s and partition %d is now: %s" % (topic, partition, isr_idx_list))
        return isr_idx_list

    def replicas(self, topic, partition=0):
        """ Get the assigned replicas for the given topic and partition.
        """
        node = self.nodes[0]
        if not self.all_nodes_topic_command_supports_bootstrap_server():
            self.logger.debug("Querying zookeeper to find assigned replicas for topic %s and partition %d" % (topic, partition))
            zk_path = "/brokers/topics/%s" % (topic)
            assignment = self.zk.query(zk_path, chroot=self.zk_chroot)

            if assignment is None:
                raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))

            assignment = json.loads(assignment)
            self.logger.info(assignment)

            replicas = assignment["partitions"][str(partition)]
        else:
            self.logger.debug("Querying Kafka Admin API to find replicas for topic %s and partition %d" % (topic, partition))
            describe_output = self.describe_topic(topic, node)
            self.logger.debug(describe_output)
            requested_partition_line = self._describe_topic_line_for_partition(partition, describe_output)
            # e.g. Topic: test_topic	Partition: 0	Leader: 3	Replicas: 3,2	Isr: 3,2
            if not requested_partition_line:
                raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))
            isr_csv = requested_partition_line.split()[7] # 8th column from above
            replicas = [int(i) for i in isr_csv.split(",")]

        self.logger.info("Assigned replicas for topic %s and partition %d is now: %s" % (topic, partition, replicas))
        return [self.get_node(replica) for replica in replicas]

    def leader(self, topic, partition=0):
        """ Get the leader replica for the given topic and partition.
        """
        node = self.nodes[0]
        if not self.all_nodes_topic_command_supports_bootstrap_server():
            self.logger.debug("Querying zookeeper to find leader replica for topic %s and partition %d" % (topic, partition))
            zk_path = "/brokers/topics/%s/partitions/%d/state" % (topic, partition)
            partition_state = self.zk.query(zk_path, chroot=self.zk_chroot)

            if partition_state is None:
                raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))

            partition_state = json.loads(partition_state)
            self.logger.info(partition_state)

            leader_idx = int(partition_state["leader"])
        else:
            self.logger.debug("Querying Kafka Admin API to find leader for topic %s and partition %d" % (topic, partition))
            describe_output = self.describe_topic(topic, node)
            self.logger.debug(describe_output)
            requested_partition_line = self._describe_topic_line_for_partition(partition, describe_output)
            # e.g. Topic: test_topic	Partition: 0	Leader: 3	Replicas: 3,2	Isr: 3,2
            if not requested_partition_line:
                raise Exception("Error finding partition state for topic %s and partition %d." % (topic, partition))
            leader_idx = int(requested_partition_line.split()[5]) # 6th column from above

        self.logger.info("Leader for topic %s and partition %d is now: %d" % (topic, partition, leader_idx))
        return self.get_node(leader_idx)

    def cluster_id(self):
        """ Get the current cluster id
        """
        if self.quorum_info.using_kraft:
            return config_property.CLUSTER_ID

        self.logger.debug("Querying ZooKeeper to retrieve cluster id")
        cluster = self.zk.query("/cluster/id", chroot=self.zk_chroot)

        try:
            return json.loads(cluster)['id'] if cluster else None
        except:
            self.logger.debug("Data in /cluster/id znode could not be parsed. Data = %s" % cluster)
            raise

    def topic_id(self, topic):
        if self.all_nodes_support_topic_ids():
            node = self.nodes[0]

            force_use_zk_connection = not self.all_nodes_topic_command_supports_bootstrap_server()

            cmd = fix_opts_for_new_jvm(node)
            cmd += "%s --topic %s --describe" % \
               (self.kafka_topics_cmd_with_optional_security_settings(node, force_use_zk_connection), topic)

            self.logger.debug(
                "Querying topic ID by using describe topic command ...\n%s" % cmd
            )
            output = ""
            for line in node.account.ssh_capture(cmd):
                output += line

            lines = map(lambda x: x.strip(), output.split("\n"))
            for line in lines:
                m = re.match(".*TopicId:.*", line)
                if m is None:
                   continue

                fields = line.split("\t")
                # [Topic: test_topic, TopicId: <topic_id>, PartitionCount: 2, ReplicationFactor: 2, ...]
                # -> [test_topic, <topic_id>, 2, 2, ...]
                # -> <topic_id>
                topic_id = list(map(lambda x: x.split(" ")[1], fields))[1]
                self.logger.info("Topic ID assigned for topic %s is %s" % (topic, topic_id))

                return topic_id
            raise Exception("Error finding topic ID for topic %s." % topic)
        else:
            self.logger.info("No topic ID assigned for topic %s" % topic)
            return None

    def check_protocol_errors(self, node):
        """ Checks for common protocol exceptions due to invalid inter broker protocol handling.
            While such errors can and should be checked in other ways, checking the logs is a worthwhile failsafe.
            """
        for node in self.nodes:
            exit_code = node.account.ssh("grep -e 'java.lang.IllegalArgumentException: Invalid version' -e SchemaException %s/*"
                                         % KafkaService.OPERATIONAL_LOG_DEBUG_DIR, allow_fail=True)
            if exit_code != 1:
                return False
        return True

    def list_consumer_groups(self, node=None, command_config=None, state=None, type=None):
        """ Get list of consumer groups.
        """
        if node is None:
            node = self.nodes[0]
        consumer_group_script = self.path.script("kafka-consumer-groups.sh", node)

        if command_config is None:
            command_config = ""
        else:
            command_config = "--command-config " + command_config

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --bootstrap-server %s %s --list" % \
              (consumer_group_script,
               self.bootstrap_servers(self.security_protocol),
               command_config)
        if state is not None:
            cmd += " --state %s" % state
        if type is not None:
            cmd += " --type %s" % type
        return self.run_cli_tool(node, cmd)

    def describe_consumer_group(self, group, node=None, command_config=None):
        """ Describe a consumer group.
        """
        if node is None:
            node = self.nodes[0]
        consumer_group_script = self.path.script("kafka-consumer-groups.sh", node)

        if command_config is None:
            command_config = ""
        else:
            command_config = "--command-config " + command_config

        cmd = fix_opts_for_new_jvm(node)
        cmd += "%s --bootstrap-server %s %s --group %s --describe" % \
              (consumer_group_script,
               self.bootstrap_servers(self.security_protocol),
               command_config, group)

        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            if not (line.startswith("SLF4J") or line.startswith("TOPIC") or line.startswith("Could not fetch offset")):
                output += line
        self.logger.debug(output)
        return output

    def describe_quorum(self, node=None):
        """Run the describe quorum command.
        Specifying node is optional, if not specified the command will be run from self.nodes[0]
        """
        if node is None:
            node = self.nodes[0]
        cmd = fix_opts_for_new_jvm(node)
        cmd += f"{self.kafka_metadata_quorum_cmd(node)} describe --status"
        self.logger.info(f"Running describe quorum command...\n{cmd}")
        node.account.ssh(cmd)

        output = ""
        for line in node.account.ssh_capture(cmd):
            output += line

        return output

    def add_controller(self, controllerId, controller):
        """Run the metadata quorum add controller command. This should be run on the node that is being added.
        """
        command_config_path = os.path.join(KafkaService.PERSISTENT_ROOT, "controller_command_config.properties")

        configs = f"""
{config_property.NODE_ID}={controllerId}
{config_property.PROCESS_ROLES}=controller
{config_property.METADATA_LOG_DIR}={KafkaService.METADATA_LOG_DIR}
{config_property.ADVERTISED_LISTENERS}={self.advertised_listeners}
{config_property.LISTENERS}={self.listeners}
{config_property.CONTROLLER_LISTENER_NAMES}={self.controller_listener_names}"""

        controller.account.create_file(command_config_path, configs)
        cmd = fix_opts_for_new_jvm(controller)
        kafka_metadata_quorum_cmd = self.kafka_metadata_quorum_cmd(controller, use_controller_bootstrap=True)
        cmd += f"{kafka_metadata_quorum_cmd} --command-config {command_config_path} add-controller"
        self.logger.info(f"Running add controller command...\n{cmd}")
        controller.account.ssh(cmd)

    def remove_controller(self, controllerId, directoryId, node=None):
        """Run the admin tool remove controller command.
        Specifying node is optional, if not specified the command will be run from self.nodes[0]
        """
        if node is None:
            node = self.nodes[0]
        cmd = fix_opts_for_new_jvm(node)
        kafka_metadata_quorum_cmd = self.kafka_metadata_quorum_cmd(node, use_controller_bootstrap=True)
        cmd += f"{kafka_metadata_quorum_cmd} remove-controller -i {controllerId} -d {directoryId}"
        self.logger.info(f"Running remove controller command...\n{cmd}")
        node.account.ssh(cmd)

    def zk_connect_setting(self):
        if self.quorum_info.using_kraft and not self.zk:
            raise Exception("No zookeeper connect string available with KRaft unless ZooKeeper is explicitly enabled")
        return self.zk.connect_setting(self.zk_chroot, self.zk_client_secure)

    def __bootstrap_servers(self, port, validate=True, offline_nodes=[]):
        if validate and not port.open:
            raise ValueError(f"We are retrieving bootstrap servers for the port: {str(port.port_number)} "
                             f"which is not currently open.")

        return ','.join([node.account.hostname + ":" + str(port.port_number)
                         for node in self.nodes
                         if node not in offline_nodes])

    def __bootstrap_controllers(self, port, validate=True, offline_nodes=[]):
        if validate and not port.open:
            raise ValueError(f"We are retrieving bootstrap controllers for the port: {str(port.port_number)} "
                             f"which is not currently open.")

        return ','.join([node.account.hostname + ":" + str(port.port_number)
                         for node in self.controller_quorum.nodes[:self.controller_quorum.num_nodes_controller_role]
                         if node not in offline_nodes])

    def bootstrap_servers(self, protocol='PLAINTEXT', validate=True, offline_nodes=[]):
        """Return comma-delimited list of brokers in this cluster formatted as HOSTNAME1:PORT1,HOSTNAME:PORT2,...

        This is the format expected by many config files.
        """
        port_mapping = self.port_mappings[protocol]
        self.logger.info("Bootstrap client port is: " + str(port_mapping.port_number))
        return self.__bootstrap_servers(port_mapping, validate, offline_nodes)

    def bootstrap_controllers(self, protocol='CONTROLLER_PLAINTEXT', validate=True, offline_nodes=[]):
        """Return comma-delimited list of controllers in this cluster formatted as HOSTNAME1:PORT1,HOSTNAME:PORT2,...

        This is the format expected by many config files.
        """
        port_mapping = self.port_mappings[protocol]
        self.logger.info("Bootstrap client port is: " + str(port_mapping.port_number))
        return self.__bootstrap_controllers(port_mapping, validate, offline_nodes)

    def controller(self):
        """ Get the controller node
        """
        if self.quorum_info.using_kraft:
            raise Exception("Cannot obtain Controller node when using KRaft instead of ZooKeeper")
        self.logger.debug("Querying zookeeper to find controller broker")
        controller_info = self.zk.query("/controller", chroot=self.zk_chroot)

        if controller_info is None:
            raise Exception("Error finding controller info")

        controller_info = json.loads(controller_info)
        self.logger.debug(controller_info)

        controller_idx = int(controller_info["brokerid"])
        self.logger.info("Controller's ID: %d" % (controller_idx))
        return self.get_node(controller_idx)

    def is_registered(self, node):
        """
        Check whether a broker is registered in Zookeeper
        """
        if self.quorum_info.using_kraft:
            raise Exception("Cannot obtain broker registration information when using KRaft instead of ZooKeeper")
        self.logger.debug("Querying zookeeper to see if broker %s is registered", str(node))
        broker_info = self.zk.query("/brokers/ids/%s" % self.idx(node), chroot=self.zk_chroot)
        self.logger.debug("Broker info: %s", broker_info)
        return broker_info is not None

    def get_offset_shell(self, time=None, topic=None, partitions=None, topic_partitions=None, exclude_internal_topics=False):
        node = self.nodes[0]

        cmd = fix_opts_for_new_jvm(node)
        cmd += self.path.script("kafka-get-offsets.sh", node)
        cmd += " --bootstrap-server %s" % self.bootstrap_servers(self.security_protocol)

        if time:
            cmd += ' --time %s' % time
        if topic_partitions:
            cmd += ' --topic-partitions %s' % topic_partitions
        if topic:
            cmd += ' --topic %s' % topic
        if partitions:
            cmd += '  --partitions %s' % partitions
        if exclude_internal_topics:
            cmd += ' --exclude-internal-topics'

        cmd += " 2>> %s/get_offset_shell.log" % KafkaService.PERSISTENT_ROOT
        cmd += " | tee -a %s/get_offset_shell.log &" % KafkaService.PERSISTENT_ROOT
        output = ""
        self.logger.debug(cmd)
        for line in node.account.ssh_capture(cmd):
            output += line
        self.logger.debug(output)
        return output

    def java_class_name(self):
        return "kafka.Kafka"
