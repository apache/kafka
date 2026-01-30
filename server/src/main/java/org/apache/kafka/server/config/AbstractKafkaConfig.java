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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.AddPartitionsToTxnConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.server.util.Csv;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.apache.commons.validator.routines.InetAddressValidator;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * During moving {@link kafka.server.KafkaConfig} out of core AbstractKafkaConfig will be the future KafkaConfig
 * so any new getters, or updates to `CONFIG_DEF` will be defined here.
 * Any code depends on kafka.server.KafkaConfig will keep for using kafka.server.KafkaConfig for the time being until we move it out of core
 * For more details check KAFKA-15853
 */
public abstract class AbstractKafkaConfig extends AbstractConfig {

    private static final InetAddressValidator INET_ADDRESS_VALIDATOR = InetAddressValidator.getInstance();

    public static final ConfigDef CONFIG_DEF = Utils.mergeConfigs(List.of(
        RemoteLogManagerConfig.configDef(),
        ServerConfigs.CONFIG_DEF,
        KRaftConfigs.CONFIG_DEF,
        MetadataLogConfig.CONFIG_DEF,
        SocketServerConfigs.CONFIG_DEF,
        ReplicationConfigs.CONFIG_DEF,
        GroupCoordinatorConfig.CONFIG_DEF,
        CleanerConfig.CONFIG_DEF,
        LogConfig.SERVER_CONFIG_DEF,
        ShareGroupConfig.CONFIG_DEF,
        ShareCoordinatorConfig.CONFIG_DEF,
        TransactionLogConfig.CONFIG_DEF,
        TransactionStateManagerConfig.CONFIG_DEF,
        QuorumConfig.CONFIG_DEF,
        MetricConfigs.CONFIG_DEF,
        QuotaConfig.CONFIG_DEF,
        BrokerSecurityConfigs.CONFIG_DEF,
        DelegationTokenManagerConfigs.CONFIG_DEF,
        AddPartitionsToTxnConfig.CONFIG_DEF
    ));

    public AbstractKafkaConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    public List<String> logDirs() {
        return Optional.ofNullable(getList(ServerLogConfigs.LOG_DIRS_CONFIG)).orElse(getList(ServerLogConfigs.LOG_DIR_CONFIG));
    }

    public int numIoThreads() {
        return getInt(ServerConfigs.NUM_IO_THREADS_CONFIG);
    }

    public int numReplicaFetchers() {
        return getInt(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG);
    }

    public int numRecoveryThreadsPerDataDir() {
        return getInt(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG);
    }

    public int backgroundThreads() {
        return getInt(ServerConfigs.BACKGROUND_THREADS_CONFIG);
    }

    public int brokerId() {
        return getInt(ServerConfigs.BROKER_ID_CONFIG);
    }

    public int requestTimeoutMs() {
        return getInt(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG);
    }

    public List<String> controllerListenerNames() {
        return getList(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG);
    }

    public ListenerName interBrokerListenerName() {
        return interBrokerListenerNameAndSecurityProtocol().getKey();
    }

    public SecurityProtocol interBrokerSecurityProtocol() {
        return interBrokerListenerNameAndSecurityProtocol().getValue();
    }

    public int initialRegistrationTimeoutMs() {
        return getInt(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG);
    }

    public int brokerHeartbeatIntervalMs() {
        return getInt(KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG);
    }

    public Optional<String> rack() {
        return Optional.ofNullable(getString(ServerConfigs.BROKER_RACK_CONFIG));
    }

    public int nodeId() {
        return getInt(KRaftConfigs.NODE_ID_CONFIG);
    }

    public Map<ListenerName, SecurityProtocol> effectiveListenerSecurityProtocolMap() {
        Map<ListenerName, SecurityProtocol> mapValue =
                getMap(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
                        getString(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG))
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                e -> ListenerName.normalised(e.getKey()),
                                e -> securityProtocol(
                                        e.getValue(),
                                        SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)));

        if (!originals().containsKey(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)) {
            // Using the default configuration since listener.security.protocol.map is not explicitly set.
            // Before adding default PLAINTEXT mappings for controller listeners, verify that:
            // 1. No SSL or SASL protocols are used in controller listeners
            // 2. No SSL or SASL protocols are used in regular listeners (Note: controller listeners
            //    are not included in 'listeners' config when process.roles=broker)
            if (controllerListenerNames().stream().anyMatch(AbstractKafkaConfig::isSslOrSasl) ||
                    getList(SocketServerConfigs.LISTENERS_CONFIG).stream()
                            .anyMatch(listenerName -> isSslOrSasl(parseListenerName(listenerName)))) {
                return mapValue;
            } else {
                // Add the PLAINTEXT mappings for all controller listener names that are not explicitly PLAINTEXT
                mapValue.putAll(controllerListenerNames().stream()
                        .filter(listenerName -> !SecurityProtocol.PLAINTEXT.name.equals(listenerName))
                        .collect(Collectors.toMap(ListenerName::new, ignored -> SecurityProtocol.PLAINTEXT)));
                return mapValue;
            }
        } else {
            return mapValue;
        }
    }

    public static Map<String, String> getMap(String propName, String propValue) {
        try {
            return Csv.parseCsvMap(propValue);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Error parsing configuration property '%s': %s", propName, e.getMessage()));
        }
    }

    public static List<Endpoint> listenerListToEndPoints(List<String> listeners, Map<ListenerName, SecurityProtocol> securityProtocolMap) {
        return listenerListToEndPoints(listeners, securityProtocolMap, true);
    }

    public static List<Endpoint> listenerListToEndPoints(List<String> listeners, Map<ListenerName, SecurityProtocol> securityProtocolMap, boolean requireDistinctPorts) {
        try {
            List<Endpoint> endPoints = SocketServerConfigs.listenerListToEndPoints(listeners, securityProtocolMap);
            validate(endPoints, listeners, requireDistinctPorts);
            return endPoints;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Error creating broker listeners from '%s': %s", listeners, e.getMessage()), e);
        }
    }

    private static void validate(List<Endpoint> endPoints, List<String> listeners, boolean requireDistinctPorts) {
        long distinctListenerNames = endPoints.stream().map(Endpoint::listener).distinct().count();
        if (distinctListenerNames != endPoints.size()) {
            throw new IllegalArgumentException("Each listener must have a different name, listeners: " + listeners);
        }

        if (!requireDistinctPorts) return;

        endPoints.stream()
            .filter(ep -> ep.port() != 0) // filter port 0 for unit tests
            .collect(Collectors.groupingBy(Endpoint::port))
            .entrySet().stream()
            .filter(entry -> entry.getValue().size() > 1)
            .forEach(entry -> {
                // Iterate through every grouping of duplicates by port to see if they are valid
                int port = entry.getKey();
                List<Endpoint> eps = entry.getValue();
                // Exception case, let's allow duplicate ports if one host is on IPv4 and the other one is on IPv6
                Map<Boolean, List<Endpoint>> partitionedByValidIp = eps.stream()
                        .collect(Collectors.partitioningBy(ep -> ep.host() != null && INET_ADDRESS_VALIDATOR.isValid(ep.host())));

                List<Endpoint> duplicatesWithIpHosts = partitionedByValidIp.get(true);
                List<Endpoint> duplicatesWithoutIpHosts = partitionedByValidIp.get(false);

                checkDuplicateListenerPorts(duplicatesWithoutIpHosts, listeners);

                if (duplicatesWithIpHosts.isEmpty()) return;
                if (duplicatesWithIpHosts.size() == 2) {
                    String errorMessage = "If you have two listeners on the same port then one needs to be IPv4 and the other IPv6, listeners: " + listeners + ", port: " + port;
                    Endpoint ep1 = duplicatesWithIpHosts.get(0);
                    Endpoint ep2 = duplicatesWithIpHosts.get(1);
                    if (!validateOneIsIpv4AndOtherIpv6(ep1.host(), ep2.host())) {
                        throw new IllegalArgumentException(errorMessage);
                    }

                    // If we reach this point it means that even though duplicatesWithIpHosts in isolation can be valid, if
                    // there happens to be ANOTHER listener on this port without an IP host (such as a null host) then its
                    // not valid.
                    if (!duplicatesWithoutIpHosts.isEmpty()) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                    return;
                }
                // Having more than 2 duplicate endpoints doesn't make sense since we only have 2 IP stacks (one is IPv4
                // and the other is IPv6)
                throw new IllegalArgumentException("Each listener must have a different port unless exactly one listener has an IPv4 address and the other IPv6 address, listeners: " + listeners + ", port: " + port);
            });
    }

    private static boolean validateOneIsIpv4AndOtherIpv6(String first, String second) {
        return (INET_ADDRESS_VALIDATOR.isValidInet4Address(first) && INET_ADDRESS_VALIDATOR.isValidInet6Address(second)) ||
                (INET_ADDRESS_VALIDATOR.isValidInet6Address(first) && INET_ADDRESS_VALIDATOR.isValidInet4Address(second));
    }

    private static void checkDuplicateListenerPorts(List<Endpoint> endpoints, List<String> listeners) {
        Set<Integer> distinctPorts = endpoints.stream().map(Endpoint::port).collect(Collectors.toSet());
        if (endpoints.size() != distinctPorts.size()) {
            throw new IllegalArgumentException("Each listener must have a different port, listeners: " + listeners);
        }
    }

    private static SecurityProtocol securityProtocol(String protocolName, String configName) {
        try {
            return SecurityProtocol.forName(protocolName);
        } catch (IllegalArgumentException e) {
            throw new ConfigException(
                    String.format("Invalid security protocol `%s` defined in %s", protocolName, configName));
        }
    }

    private Map.Entry<ListenerName, SecurityProtocol> interBrokerListenerNameAndSecurityProtocol() {
        String interBrokerListenerName = getString(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG);
        if (interBrokerListenerName != null) {
            if (originals().containsKey(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG)) {
                throw new ConfigException(String.format("Only one of %s and %s should be set.",
                        ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG,
                        ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG));
            }
            ListenerName listenerName = ListenerName.normalised(interBrokerListenerName);
            SecurityProtocol securityProtocol = effectiveListenerSecurityProtocolMap().get(listenerName);
            if (securityProtocol == null) {
                throw new ConfigException("Listener with name " + listenerName.value() + " defined in " +
                        ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG + " not found in " +
                        SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG + ".");
            }
            return Map.entry(listenerName, securityProtocol);
        } else {
            SecurityProtocol securityProtocol = securityProtocol(
                    getString(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG),
                    ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG);
            return Map.entry(ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
        }
    }

    private static boolean isSslOrSasl(String name) {
        return name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) ||
                name.equals(SecurityProtocol.SASL_PLAINTEXT.name);
    }

    private static String parseListenerName(String connectionString) {
        int firstColon = connectionString.indexOf(':');
        if (firstColon < 0) {
            throw new KafkaException("Unable to parse a listener name from " + connectionString);
        }
        return connectionString.substring(0, firstColon).toUpperCase(Locale.ROOT);
    }

    /**
     * Registers a component for dynamic reconfiguration notifications.
     * <p>
     * This method exists to support migration from kafka.server.KafkaConfig (Scala/core) to AbstractKafkaConfig (Java/server).
     * When migrating code, replace KafkaConfig references with AbstractKafkaConfig.
     *
     * @param reconfigurable the component to register for configuration updates
     */
    public abstract void addReconfigurable(Reconfigurable reconfigurable);

    /**
     * Unregisters a component from dynamic reconfiguration notifications.
     * <p>
     * This method exists to support migration from kafka.server.KafkaConfig (Scala/core) to AbstractKafkaConfig (Java/server).
     * When migrating code, replace KafkaConfig references with AbstractKafkaConfig.
     *
     * @param reconfigurable the component to unregister
     */
    public abstract void removeReconfigurable(Reconfigurable reconfigurable);
}
