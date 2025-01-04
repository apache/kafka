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

package org.apache.kafka.common.test.api;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_BROKER_LISTENER_NAME;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_BROKER_SECURITY_PROTOCOL;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_CONTROLLER_LISTENER_NAME;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_CONTROLLER_SECURITY_PROTOCOL;

/**
 * Represents an immutable requested configuration of a Kafka cluster for integration testing.
 */
public class ClusterConfig {

    private final Set<Type> types;
    private final int brokers;
    private final int controllers;
    private final int disksPerBroker;
    private final boolean autoStart;
    private final SecurityProtocol brokerSecurityProtocol;
    private final ListenerName brokerListenerName;
    private final SecurityProtocol controllerSecurityProtocol;
    private final ListenerName controllerListenerName;
    private final File trustStoreFile;
    private final MetadataVersion metadataVersion;

    private final Map<String, String> serverProperties;
    private final Map<String, String> producerProperties;
    private final Map<String, String> consumerProperties;
    private final Map<String, String> adminClientProperties;
    private final Map<String, String> saslServerProperties;
    private final Map<String, String> saslClientProperties;
    private final List<String> tags;
    private final Map<Integer, Map<String, String>> perServerProperties;
    private final Map<Feature, Short> features;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private ClusterConfig(Set<Type> types, int brokers, int controllers, int disksPerBroker, boolean autoStart,
                  SecurityProtocol brokerSecurityProtocol, ListenerName brokerListenerName,
                  SecurityProtocol controllerSecurityProtocol, ListenerName controllerListenerName, File trustStoreFile,
                  MetadataVersion metadataVersion, Map<String, String> serverProperties, Map<String, String> producerProperties,
                  Map<String, String> consumerProperties, Map<String, String> adminClientProperties, Map<String, String> saslServerProperties,
                  Map<String, String> saslClientProperties, Map<Integer, Map<String, String>> perServerProperties, List<String> tags,
                  Map<Feature, Short> features) {
        // do fail fast. the following values are invalid for kraft modes.
        if (brokers < 0) throw new IllegalArgumentException("Number of brokers must be greater or equal to zero.");
        if (controllers < 0) throw new IllegalArgumentException("Number of controller must be greater or equal to zero.");
        if (disksPerBroker <= 0) throw new IllegalArgumentException("Number of disks must be greater than zero.");

        this.types = Objects.requireNonNull(types);
        this.brokers = brokers;
        this.controllers = controllers;
        this.disksPerBroker = disksPerBroker;
        this.autoStart = autoStart;
        this.brokerSecurityProtocol = Objects.requireNonNull(brokerSecurityProtocol);
        this.brokerListenerName = Objects.requireNonNull(brokerListenerName);
        this.controllerSecurityProtocol = Objects.requireNonNull(controllerSecurityProtocol);
        this.controllerListenerName = Objects.requireNonNull(controllerListenerName);
        this.trustStoreFile = trustStoreFile;
        this.metadataVersion = Objects.requireNonNull(metadataVersion);
        this.serverProperties = Objects.requireNonNull(serverProperties);
        this.producerProperties = Objects.requireNonNull(producerProperties);
        this.consumerProperties = Objects.requireNonNull(consumerProperties);
        this.adminClientProperties = Objects.requireNonNull(adminClientProperties);
        this.saslServerProperties = Objects.requireNonNull(saslServerProperties);
        this.saslClientProperties = Objects.requireNonNull(saslClientProperties);
        this.perServerProperties = Objects.requireNonNull(perServerProperties);
        this.tags = Objects.requireNonNull(tags);
        this.features = Objects.requireNonNull(features);
    }

    public Set<Type> clusterTypes() {
        return types;
    }

    public int numBrokers() {
        return brokers;
    }

    public int numControllers() {
        return controllers;
    }

    public int numDisksPerBroker() {
        return disksPerBroker;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public Map<String, String> serverProperties() {
        return serverProperties;
    }

    public Map<String, String> producerProperties() {
        return producerProperties;
    }

    public Map<String, String> consumerProperties() {
        return consumerProperties;
    }

    public Map<String, String> adminClientProperties() {
        return adminClientProperties;
    }

    public Map<String, String> saslServerProperties() {
        return saslServerProperties;
    }

    public Map<String, String> saslClientProperties() {
        return saslClientProperties;
    }

    public SecurityProtocol brokerSecurityProtocol() {
        return brokerSecurityProtocol;
    }

    public ListenerName controllerListenerName() {
        return controllerListenerName;
    }

    public SecurityProtocol controllerSecurityProtocol() {
        return controllerSecurityProtocol;
    }

    public ListenerName brokerListenerName() {
        return brokerListenerName;
    }

    public Optional<File> trustStoreFile() {
        return Optional.ofNullable(trustStoreFile);
    }

    public MetadataVersion metadataVersion() {
        return metadataVersion;
    }

    public Map<Integer, Map<String, String>> perServerOverrideProperties() {
        return perServerProperties;
    }

    public List<String> tags() {
        return tags;
    }

    public Map<Feature, Short> features() {
        return features;
    }

    public Set<String> displayTags() {
        Set<String> displayTags = new LinkedHashSet<>(tags);
        displayTags.add("MetadataVersion=" + metadataVersion);
        displayTags.add("BrokerSecurityProtocol=" + brokerSecurityProtocol.name());
        displayTags.add("BrokerListenerName=" + brokerListenerName);
        displayTags.add("ControllerSecurityProtocol=" + controllerSecurityProtocol.name());
        displayTags.add("ControllerListenerName=" + controllerListenerName);
        return displayTags;
    }

    public static Builder defaultBuilder() {
        return new Builder()
                .setTypes(Stream.of(Type.KRAFT, Type.CO_KRAFT).collect(Collectors.toSet()))
                .setBrokers(1)
                .setControllers(1)
                .setDisksPerBroker(1)
                .setAutoStart(true)
                .setBrokerSecurityProtocol(DEFAULT_BROKER_SECURITY_PROTOCOL)
                .setBrokerListenerName(ListenerName.normalised(DEFAULT_BROKER_LISTENER_NAME))
                .setControllerSecurityProtocol(DEFAULT_CONTROLLER_SECURITY_PROTOCOL)
                .setControllerListenerName(ListenerName.normalised(DEFAULT_CONTROLLER_LISTENER_NAME))
                .setMetadataVersion(MetadataVersion.latestTesting());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ClusterConfig clusterConfig) {
        return new Builder()
                .setTypes(clusterConfig.types)
                .setBrokers(clusterConfig.brokers)
                .setControllers(clusterConfig.controllers)
                .setDisksPerBroker(clusterConfig.disksPerBroker)
                .setAutoStart(clusterConfig.autoStart)
                .setBrokerSecurityProtocol(clusterConfig.brokerSecurityProtocol)
                .setBrokerListenerName(clusterConfig.brokerListenerName)
                .setControllerSecurityProtocol(clusterConfig.controllerSecurityProtocol)
                .setControllerListenerName(clusterConfig.controllerListenerName)
                .setTrustStoreFile(clusterConfig.trustStoreFile)
                .setMetadataVersion(clusterConfig.metadataVersion)
                .setServerProperties(clusterConfig.serverProperties)
                .setProducerProperties(clusterConfig.producerProperties)
                .setConsumerProperties(clusterConfig.consumerProperties)
                .setAdminClientProperties(clusterConfig.adminClientProperties)
                .setSaslServerProperties(clusterConfig.saslServerProperties)
                .setSaslClientProperties(clusterConfig.saslClientProperties)
                .setPerServerProperties(clusterConfig.perServerProperties)
                .setTags(clusterConfig.tags)
                .setFeatures(clusterConfig.features);
    }

    public static class Builder {
        private Set<Type> types;
        private int brokers;
        private int controllers;
        private int disksPerBroker;
        private boolean autoStart;
        private SecurityProtocol brokerSecurityProtocol;
        private ListenerName brokerListenerName;
        private SecurityProtocol controllerSecurityProtocol;
        private ListenerName controllerListenerName;
        private File trustStoreFile;
        private MetadataVersion metadataVersion;
        private Map<String, String> serverProperties = Collections.emptyMap();
        private Map<String, String> producerProperties = Collections.emptyMap();
        private Map<String, String> consumerProperties = Collections.emptyMap();
        private Map<String, String> adminClientProperties = Collections.emptyMap();
        private Map<String, String> saslServerProperties = Collections.emptyMap();
        private Map<String, String> saslClientProperties = Collections.emptyMap();
        private Map<Integer, Map<String, String>> perServerProperties = Collections.emptyMap();
        private List<String> tags = Collections.emptyList();
        private Map<Feature, Short> features = Collections.emptyMap();

        private Builder() {}

        public Builder setTypes(Set<Type> types) {
            this.types = Set.copyOf(types);
            return this;
        }

        public Builder setBrokers(int brokers) {
            this.brokers = brokers;
            return this;
        }

        public Builder setControllers(int controllers) {
            this.controllers = controllers;
            return this;
        }

        public Builder setDisksPerBroker(int disksPerBroker) {
            this.disksPerBroker = disksPerBroker;
            return this;
        }

        public Builder setAutoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        public Builder setBrokerSecurityProtocol(SecurityProtocol securityProtocol) {
            this.brokerSecurityProtocol = securityProtocol;
            return this;
        }

        public Builder setBrokerListenerName(ListenerName listenerName) {
            this.brokerListenerName = listenerName;
            return this;
        }

        public Builder setControllerSecurityProtocol(SecurityProtocol securityProtocol) {
            this.controllerSecurityProtocol = securityProtocol;
            return this;
        }

        public Builder setControllerListenerName(ListenerName listenerName) {
            this.controllerListenerName = listenerName;
            return this;
        }

        public Builder setTrustStoreFile(File trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            return this;
        }

        public Builder setMetadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        public Builder setServerProperties(Map<String, String> serverProperties) {
            this.serverProperties = Map.copyOf(serverProperties);
            return this;
        }

        public Builder setConsumerProperties(Map<String, String> consumerProperties) {
            this.consumerProperties = Map.copyOf(consumerProperties);
            return this;
        }

        public Builder setProducerProperties(Map<String, String> producerProperties) {
            this.producerProperties = Map.copyOf(producerProperties);
            return this;
        }

        public Builder setAdminClientProperties(Map<String, String> adminClientProperties) {
            this.adminClientProperties = Map.copyOf(adminClientProperties);
            return this;
        }

        public Builder setSaslServerProperties(Map<String, String> saslServerProperties) {
            this.saslServerProperties = Map.copyOf(saslServerProperties);
            return this;
        }

        public Builder setSaslClientProperties(Map<String, String> saslClientProperties) {
            this.saslClientProperties = Map.copyOf(saslClientProperties);
            return this;
        }

        public Builder setPerServerProperties(Map<Integer, Map<String, String>> perServerProperties) {
            this.perServerProperties = Collections.unmodifiableMap(
                    perServerProperties.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> Map.copyOf(e.getValue()))));
            return this;
        }

        public Builder setTags(List<String> tags) {
            this.tags = List.copyOf(tags);
            return this;
        }

        public Builder setFeatures(Map<Feature, Short> features) {
            this.features = Collections.unmodifiableMap(features);
            return this;
        }

        public ClusterConfig build() {
            return new ClusterConfig(types, brokers, controllers, disksPerBroker, autoStart,
                    brokerSecurityProtocol, brokerListenerName, controllerSecurityProtocol, controllerListenerName,
                    trustStoreFile, metadataVersion, serverProperties, producerProperties, consumerProperties,
                    adminClientProperties, saslServerProperties, saslClientProperties,
                    perServerProperties, tags, features);
        }
    }
}
