/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.config;

import org.slf4j.Logger;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.storage.internals.log.LogConfig;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.server.common.MetadataVersion.IBP_2_1_IV0;

public class KafkaConfigValidator {
    AbstractKafkaConfig config;
    Logger log;
    public KafkaConfigValidator(AbstractKafkaConfig config, Logger log) {
        this.config = config;
        this.log = log;
    }

    public void validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker() {
        Set<ListenerName> advertisedListenerNames = config.extractListenerNames(config.effectiveAdvertisedListeners());

        Utils.require(advertisedListenerNames.stream().noneMatch(aln -> config.controllerListenerNames().contains(aln.value())),
                String.format("The advertised.listeners config must not contain KRaft controller listeners from %s when %s contains the broker role because Kafka clients that send requests via advertised listeners do not send requests to KRaft controllers -- they only send requests to KRaft brokers.",
                        KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, KRaftConfigs.PROCESS_ROLES_CONFIG));
    }

    public void validateControllerQuorumVotersMustContainNodeIdForKRaftController(Set<Integer> voterIds) {
        Utils.require(voterIds.contains(config.nodeId()),
                String.format("If %s contains the 'controller' role, the node id %d must be included in the set of voters %s=%s",
                        KRaftConfigs.PROCESS_ROLES_CONFIG, config.nodeId(), QuorumConfig.QUORUM_VOTERS_CONFIG, voterIds));
    }
    public void validateControllerListenerExistsForKRaftController() {
        Utils.require(!config.controllerListeners().isEmpty(),
                String.format("%s must contain at least one value appearing in the '%s' configuration when running the KRaft controller role",
                        KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, SocketServerConfigs.LISTENERS_CONFIG));
    }
    public void validateControllerListenerNamesMustAppearInListenersForKRaftController() {
        Set<String> listenerNameValues = config.listeners().stream()
                .filter(l -> l.listenerName().isPresent())
                .map(l -> l.listenerName().get())
                .collect(Collectors.toSet());
        Utils.require(listenerNameValues.containsAll(config.controllerListenerNames()),
                String.format("%s must only contain values appearing in the '%s' configuration when running the KRaft controller role",
                        KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, SocketServerConfigs.LISTENERS_CONFIG
                ));
    }
    public void validateAdvertisedListenersNonEmptyForBroker() {
        Set<ListenerName> advertisedListenerNames = config.extractListenerNames(config.effectiveAdvertisedListeners());

        Utils.require(!advertisedListenerNames.isEmpty(),
                "There must be at least one advertised listener." + (
                        config.processRoles().contains(ProcessRole.BrokerRole) ?
                                String.format(" Perhaps all listeners appear in %s?", KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG) :
                                ""));
    }
    @SuppressWarnings("deprecation")
    public String createBrokerWarningMessage() {
        return String.format("Broker configuration %s with value %s is ignored because the inter-broker protocol version `%s` is greater or equal than 3.0. " +
                        "This configuration is deprecated and it will be removed in Apache Kafka 4.0.",
                ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG, config.logMessageFormatVersionString(), config.interBrokerProtocolVersionString());
    }

    public void validateLogConfig() {
        Utils.require(config.logRollTimeMillis() >= 1, "log.roll.ms must be greater than or equal to 1");
        Utils.require(config.logRollTimeJitterMillis() >= 0, "log.roll.jitter.ms must be greater than or equal to 0");
        Utils.require(config.logRetentionTimeMillis() >= 1 || config.logRetentionTimeMillis() == -1, "log.retention.ms must be unlimited (-1) or, greater than or equal to 1");
        Utils.require(!config.logDirs().isEmpty(), "At least one log directory must be defined via log.dirs or log.dir.");
        Utils.require(config.logCleanerDedupeBufferSize() / config.logCleanerThreads() > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.");
    }

    private void validateNonEmptyQuorumVotersForKRaft(Set<Integer> voterIds) {
        if (voterIds.isEmpty()) {
            throw new ConfigException(String.format("If using %s, %s must contain a parseable set of voters.", KRaftConfigs.PROCESS_ROLES_CONFIG, QuorumConfig.QUORUM_VOTERS_CONFIG));
        }
    }

    public void validateNonEmptyQuorumVotersForMigration() {
        if (QuorumConfig.parseVoterIds(config.quorumVoters()).isEmpty()) {
            throw new ConfigException(String.format("If using %s, %s must contain a parseable set of voters.", KRaftConfigs.MIGRATION_ENABLED_CONFIG, QuorumConfig.QUORUM_VOTERS_CONFIG));
        }
    }

    public void validateControlPlaneListenerEmptyForKRaft() {
        Utils.require(!config.controlPlaneListenerName().isPresent(),
                SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG + " is not supported in KRaft mode.");
    }

    public void validateKraftBrokerConfig() {
        Set<Integer> voterIds = QuorumConfig.parseVoterIds(config.quorumVoters());
        // KRaft broker-only
        validateNonEmptyQuorumVotersForKRaft(voterIds);
        validateControlPlaneListenerEmptyForKRaft();
        validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker();
        // nodeId must not appear in controller.quorum.voters
        Utils.require(!voterIds.contains(config.nodeId()),
                String.format("If %s contains just the 'broker' role, the node id %d must not be included in the set of voters %s=%s",
                        KRaftConfigs.PROCESS_ROLES_CONFIG, config.nodeId(), QuorumConfig.QUORUM_VOTERS_CONFIG, voterIds));
        // controller.listener.names must be non-empty...
        Utils.require(!config.controllerListenerNames().isEmpty(),
                String.format("%s must contain at least one value when running KRaft with just the broker role", KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG));
        // controller.listener.names are forbidden in listeners...
        Utils.require(config.controllerListeners().isEmpty(),
                String.format("%s must not contain a value appearing in the '%s' configuration when running KRaft with just the broker role",
                        KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, SocketServerConfigs.LISTENERS_CONFIG));
        // controller.listener.names must all appear in listener.security.protocol.map
        config.controllerListenerNames().forEach(name -> {
            ListenerName listenerName = ListenerName.normalised(name);
            if (!config.effectiveListenerSecurityProtocolMap().containsKey(listenerName)) {
                throw new ConfigException(String.format("Controller listener with name %s defined in " +
                                "%s not found in %s  (an explicit security mapping for each controller listener is required if %s is non-empty, or if there are security protocols other than PLAINTEXT in use)",
                        listenerName.value(), KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG));
            }
        });
        // warn that only the first controller listener is used if there is more than one
        if (config.controllerListenerNames().size() > 1) {
            log.warn(String.format("%s has multiple entries; only the first will be used since %s=broker: %s",
                    KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, KRaftConfigs.PROCESS_ROLES_CONFIG, config.controllerListenerNames()));
        }
        validateAdvertisedListenersNonEmptyForBroker();
    }

    public void validateKraftControllerConfig() {
        // KRaft controller-only
        Set<Integer> voterIds = QuorumConfig.parseVoterIds(config.quorumVoters());

        validateNonEmptyQuorumVotersForKRaft(voterIds);
        validateControlPlaneListenerEmptyForKRaft();
        // advertised listeners must be empty when only the controller is configured
        Utils.require(config.getString(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG) == null,
                String.format("The %s config must be empty when %s=controller",
                        SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, KRaftConfigs.PROCESS_ROLES_CONFIG));
        // listeners should only contain listeners also enumerated in the controller listener
        Utils.require(config.effectiveAdvertisedListeners().isEmpty(),
                String.format("The %s config must only contain KRaft controller listeners from %s when %s=controller",
                        SocketServerConfigs.LISTENERS_CONFIG, KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG,
                        KRaftConfigs.PROCESS_ROLES_CONFIG));
        validateControllerQuorumVotersMustContainNodeIdForKRaftController(voterIds);
        validateControllerListenerExistsForKRaftController();
        validateControllerListenerNamesMustAppearInListenersForKRaftController();
    }

    public void validateKraftCombinedModeConfig() {
        // KRaft combined broker and controller
        Set<Integer> voterIds = QuorumConfig.parseVoterIds(config.quorumVoters());

        validateNonEmptyQuorumVotersForKRaft(voterIds);
        validateControlPlaneListenerEmptyForKRaft();
        validateAdvertisedListenersDoesNotContainControllerListenersForKRaftBroker();
        validateControllerQuorumVotersMustContainNodeIdForKRaftController(voterIds);
        validateControllerListenerExistsForKRaftController();
        validateControllerListenerNamesMustAppearInListenersForKRaftController();
        validateAdvertisedListenersNonEmptyForBroker();
    }

    public void validateListenerNames() {
        Set<ListenerName> listenerNames = config.extractListenerNames();
        List<Endpoint> effectiveAdvertisedListeners = config.effectiveAdvertisedListeners();
        Set<ListenerName> advertisedListenerNames = config.extractListenerNames(effectiveAdvertisedListeners);

        Set<ProcessRole> processRoles = config.processRoles();
        if (processRoles.isEmpty() || processRoles.contains(ProcessRole.BrokerRole)) {
            // validations for all broker setups (i.e. ZooKeeper and KRaft broker-only and KRaft co-located)
            validateAdvertisedListenersNonEmptyForBroker();
            Utils.require(advertisedListenerNames.contains(config.interBrokerListenerName()),
                    String.format("%s must be a listener name defined in %s. The valid options based on currently configured listeners are %s",
                            ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
                            String.join(",", advertisedListenerNames.stream().map(ListenerName::value).collect(Collectors.toSet()))));
            Utils.require(listenerNames.containsAll(advertisedListenerNames),
                    String.format("%s listener names must be equal to or a subset of the ones defined in %s. " +
                                    "Found %s The valid options based on the current configuration are %s",
                            SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, SocketServerConfigs.LISTENERS_CONFIG,
                            String.join(",", advertisedListenerNames.stream().map(ListenerName::value).collect(Collectors.toSet())),
                            String.join(", ", listenerNames.stream().map(ListenerName::value).collect(Collectors.toSet()))));
        }

        Utils.require(effectiveAdvertisedListeners.stream().noneMatch(endpoint -> "0.0.0.0".equals(endpoint.host())),
                String.format("%s cannot use the nonroutable meta-address 0.0.0.0. Use a routable IP address.",
                        SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG));
        // validate control.plane.listener.name config
        Optional<ListenerName> controlPlaneListenerName = config.controlPlaneListenerName();
        if (controlPlaneListenerName.isPresent()) {
            Utils.require(advertisedListenerNames.contains(controlPlaneListenerName.get()),
                    String.format("%s must be a listener name defined in %s. The valid options based on currently configured listeners are %s",
                            SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG, SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
                            String.join(",", advertisedListenerNames.stream().map(ListenerName::value).collect(Collectors.toSet()))
                    ));
            // controlPlaneListenerName should be different from interBrokerListenerName
            Utils.require(!controlPlaneListenerName.get().value().equals(config.interBrokerListenerName().value()),
                    String.format("%s, when defined, should have a different value from the inter broker listener name. " +
                                    "Currently they both have the value %s",
                            SocketServerConfigs.CONTROL_PLANE_LISTENER_NAME_CONFIG, controlPlaneListenerName.get()
                    ));
        }
    }

    @SuppressWarnings("deprecation")
    public void validateNewGroupCoordinatorConfigs() {
        Utils.require(config.consumerGroupMaxHeartbeatIntervalMs() >= config.consumerGroupMinHeartbeatIntervalMs(),
                String.format("%s must be greater than or equals to %s",
                        GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG,
                        GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG));
        Utils.require(config.consumerGroupHeartbeatIntervalMs() >= config.consumerGroupMinHeartbeatIntervalMs(),
                String.format("%s must be greater than or equals to %s",
                        GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG,
                        GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG));
        Utils.require(config.consumerGroupHeartbeatIntervalMs() <= config.consumerGroupMaxHeartbeatIntervalMs(),
                String.format("%s must be less than or equals to %s",
                        GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG,
                        GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG));

        Utils.require(config.consumerGroupMaxSessionTimeoutMs() >= config.consumerGroupMinSessionTimeoutMs(),
                String.format("%s must be greater than or equals to %s",
                        GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG,
                        GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        Utils.require(config.consumerGroupSessionTimeoutMs() >= config.consumerGroupMinSessionTimeoutMs(),
                String.format("%s must be greater than or equals to %s",
                        GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG,
                        GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        Utils.require(config.consumerGroupSessionTimeoutMs() <= config.consumerGroupMaxSessionTimeoutMs(),
                String.format("%s must be less than or equals to %s",
                        GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG,
                        GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG));

        if (config.originals().containsKey(GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG)) {
            log.warn(String.format("%s is deprecated and it will be removed in Apache Kafka 4.0.", GroupCoordinatorConfig.OFFSET_COMMIT_REQUIRED_ACKS_CONFIG));
        }
    }

    public void validateSharedGroupConfigs() {
        Utils.require(config.shareGroupMaxHeartbeatIntervalMs() >= config.shareGroupMinHeartbeatIntervalMs(),
                ShareGroupConfigs.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        Utils.require(config.shareGroupHeartbeatIntervalMs() >= config.shareGroupMinHeartbeatIntervalMs(),
                ShareGroupConfigs.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        Utils.require(config.shareGroupHeartbeatIntervalMs() <= config.shareGroupMaxHeartbeatIntervalMs(),
                ShareGroupConfigs.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);

        Utils.require(config.shareGroupMaxSessionTimeoutMs() >= config.shareGroupMinSessionTimeoutMs(),
                ShareGroupConfigs.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        Utils.require(config.shareGroupSessionTimeoutMs() >= config.shareGroupMinSessionTimeoutMs(),
                ShareGroupConfigs.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        Utils.require(config.shareGroupSessionTimeoutMs() <= config.shareGroupMaxSessionTimeoutMs(),
                ShareGroupConfigs.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);

        Utils.require(config.shareGroupMaxRecordLockDurationMs() >= config.shareGroupMinRecordLockDurationMs(),
                ShareGroupConfigs.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        Utils.require(config.shareGroupRecordLockDurationMs() >= config.shareGroupMinRecordLockDurationMs(),
                ShareGroupConfigs.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        Utils.require(config.shareGroupMaxRecordLockDurationMs() >= config.shareGroupRecordLockDurationMs(),
                ShareGroupConfigs.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG + " must be greater than or equals to " +
                        ShareGroupConfigs.SHARE_GROUP_RECORD_LOCK_DURATION_MS_CONFIG);

    }


    @SuppressWarnings("deprecation")
    public void validateMessageFormatConfigs() {
        if (new LogConfig.MessageFormatVersion(config.logMessageFormatVersionString(), config.interBrokerProtocolVersionString()).shouldWarn()) {
            log.warn(createBrokerWarningMessage());
        }

        RecordVersion recordVersion = config.logMessageFormatVersion().highestSupportedRecordVersion();
        Utils.require(config.interBrokerProtocolVersion().highestSupportedRecordVersion().value >= recordVersion.value,
                String.format("log.message.format.version %s can only be used when inter.broker.protocol.version " +
                                "is set to version %s or higher",
                        config.logMessageFormatVersionString(), MetadataVersion.minSupportedFor(recordVersion).shortVersion()));
    }

    public void validateConnectionConfigs() {
        if (config.maxConnectionsPerIp() == 0)
            Utils.require(!config.maxConnectionsPerIpOverrides().isEmpty(), String.format("%s can be set to zero only if" +
                    " %s property is set.", SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG));

        Set<String> invalidAddresses = config.maxConnectionsPerIpOverrides().keySet().stream().filter(address -> !Utils.validHostPattern(address)).collect(Collectors.toSet());
        if (!invalidAddresses.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s contains invalid addresses : %s",
                    SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG,
                    String.join(",", invalidAddresses)));
        }

        if (config.connectionsMaxIdleMs() >= 0) {
            Utils.require(config.failedAuthenticationDelayMs() < config.connectionsMaxIdleMs(), String.format(
                    "%s=%s should always be less than %s=%s to prevent failed authentication responses from timing out",
                    SocketServerConfigs.FAILED_AUTHENTICATION_DELAY_MS_CONFIG, config.failedAuthenticationDelayMs(),
                    SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, config.connectionsMaxIdleMs()));
        }
    }

    public void validateCompressionConfig() {
        if (config.offsetsTopicCompressionType() == CompressionType.ZSTD)
            Utils.require(config.interBrokerProtocolVersion().highestSupportedRecordVersion().value >= IBP_2_1_IV0.highestSupportedRecordVersion().value,
                    "offsets.topic.compression.codec zstd can only be used when inter.broker.protocol.version " +
                            "is set to version " + IBP_2_1_IV0.shortVersion() + " or higher");
    }

    public void validateInterBrokerSecurityConfig() {
        Boolean interBrokerUsesSasl = config.interBrokerSecurityProtocol() == SecurityProtocol.SASL_PLAINTEXT ||
                config.interBrokerSecurityProtocol() == SecurityProtocol.SASL_SSL;
        Utils.require(!interBrokerUsesSasl || config.saslInterBrokerHandshakeRequestEnable() || config.saslMechanismInterBrokerProtocol() == SaslConfigs.GSSAPI_MECHANISM,
                "Only GSSAPI mechanism is supported for inter-broker communication with SASL when inter.broker.protocol.version is set to " + config.interBrokerProtocolVersionString());
        Utils.require(!interBrokerUsesSasl || config.saslEnabledMechanisms(config.interBrokerListenerName()).contains(config.saslMechanismInterBrokerProtocol()),
                String.format("%s must be included in %s when SASL is used for inter-broker communication",
                        BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG));

        Class<?> principalBuilderClass = config.getClass(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG);
        Utils.require(principalBuilderClass != null, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG + " must be non-null");
        Utils.require(KafkaPrincipalSerde.class.isAssignableFrom(principalBuilderClass),
                BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG + " must implement KafkaPrincipalSerde");
    }

    public void validateQueueMaxByte() {
        Utils.require(config.queuedMaxBytes() <= 0 || config.queuedMaxBytes() >= config.socketRequestMaxBytes(),
                ServerConfigs.QUEUED_MAX_BYTES_CONFIG + " must be larger or equal to " + SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG);
    }

    public void validateReplicaFetchConfigs() {
        Utils.require(config.replicaFetchWaitMaxMs() <= config.replicaSocketTimeoutMs(), "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
                " to prevent unnecessary socket timeouts");
        Utils.require(config.replicaFetchWaitMaxMs() <= config.replicaLagTimeMaxMs(), "replica.fetch.wait.max.ms should always be less than or equal to replica.lag.time.max.ms" +
                " to prevent frequent changes in ISR");
    }

    @SuppressWarnings("deprecation")
    public void validateOffsetCommitAcks() {
        Utils.require(config.offsetCommitRequiredAcks() >= -1 && config.offsetCommitRequiredAcks() <= config.offsetsTopicReplicationFactor(),
                "offsets.commit.required.acks must be greater or equal -1 and less or equal to offsets.topic.replication.factor");
    }

    public void validateNodeAndBrokerId() {
        if (config.nodeId() != config.brokerId()) {
            throw new ConfigException(String.format("You must set `%s` to the same value as `%s`.",
                    KRaftConfigs.NODE_ID_CONFIG, ServerConfigs.BROKER_ID_CONFIG));
        }
    }

    /**
     * Validate some configurations for new MetadataVersion. A new MetadataVersion can take place when
     * a FeatureLevelRecord for "metadata.version" is read from the cluster metadata.
     */
    public void validateWithMetadataVersion(MetadataVersion metadataVersion) {
        if (config.processRoles().contains(ProcessRole.BrokerRole) && config.logDirs().size() > 1) {
            Utils.require(metadataVersion.isDirectoryAssignmentSupported(),
                    "Multiple log directories (aka JBOD) are not supported in the current MetadataVersion " + metadataVersion + ". Need " +
                            MetadataVersion.IBP_3_7_IV2 + " or higher");
        }
    }
}
