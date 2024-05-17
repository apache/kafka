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

package kafka.test;

import kafka.test.annotation.Type;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents an immutable requested configuration of a Kafka cluster for integration testing.
 */
public class ClusterConfig {

    private final Set<Type> types;
    private final int brokers;
    private final int controllers;
    private final int disksPerBroker;
    private final boolean autoStart;
    private final SecurityProtocol securityProtocol;
    private final String listenerName;
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

    @SuppressWarnings("checkstyle:ParameterNumber")
    private ClusterConfig(Set<Type> types, int brokers, int controllers, int disksPerBroker, boolean autoStart,
                  SecurityProtocol securityProtocol, String listenerName, File trustStoreFile,
                  MetadataVersion metadataVersion, Map<String, String> serverProperties, Map<String, String> producerProperties,
                  Map<String, String> consumerProperties, Map<String, String> adminClientProperties, Map<String, String> saslServerProperties,
                  Map<String, String> saslClientProperties, Map<Integer, Map<String, String>> perServerProperties, List<String> tags) {
        // do fail fast. the following values are invalid for both zk and kraft modes.
        if (brokers < 0) throw new IllegalArgumentException("Number of brokers must be greater or equal to zero.");
        if (controllers < 0) throw new IllegalArgumentException("Number of controller must be greater or equal to zero.");
        if (disksPerBroker <= 0) throw new IllegalArgumentException("Number of disks must be greater than zero.");

        this.types = Objects.requireNonNull(types);
        this.brokers = brokers;
        this.controllers = controllers;
        this.disksPerBroker = disksPerBroker;
        this.autoStart = autoStart;
        this.securityProtocol = Objects.requireNonNull(securityProtocol);
        this.listenerName = listenerName;
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

    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    public Optional<String> listenerName() {
        return Optional.ofNullable(listenerName);
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

    public Set<String> displayTags() {
        Set<String> displayTags = new LinkedHashSet<>(tags);
        displayTags.add("MetadataVersion=" + metadataVersion);
        displayTags.add("Security=" + securityProtocol.name());
        listenerName().ifPresent(listener -> displayTags.add("Listener=" + listener));
        return displayTags;
    }

    public static Builder defaultBuilder() {
        return new Builder()
                .setTypes(Stream.of(Type.ZK, Type.KRAFT, Type.CO_KRAFT).collect(Collectors.toSet()))
                .setBrokers(1)
                .setControllers(1)
                .setDisksPerBroker(1)
                .setAutoStart(true)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT)
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
                .setSecurityProtocol(clusterConfig.securityProtocol)
                .setListenerName(clusterConfig.listenerName)
                .setTrustStoreFile(clusterConfig.trustStoreFile)
                .setMetadataVersion(clusterConfig.metadataVersion)
                .setServerProperties(clusterConfig.serverProperties)
                .setProducerProperties(clusterConfig.producerProperties)
                .setConsumerProperties(clusterConfig.consumerProperties)
                .setAdminClientProperties(clusterConfig.adminClientProperties)
                .setSaslServerProperties(clusterConfig.saslServerProperties)
                .setSaslClientProperties(clusterConfig.saslClientProperties)
                .setPerServerProperties(clusterConfig.perServerProperties)
                .setTags(clusterConfig.tags);
    }

    public static class Builder {
        private Set<Type> types;
        private int brokers;
        private int controllers;
        private int disksPerBroker;
        private boolean autoStart;
        private SecurityProtocol securityProtocol;
        private String listenerName;
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

        private Builder() {}

        public Builder setTypes(Set<Type> types) {
            this.types = Collections.unmodifiableSet(new HashSet<>(types));
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

        public Builder setSecurityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder setListenerName(String listenerName) {
            this.listenerName = listenerName;
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
            this.serverProperties = Collections.unmodifiableMap(new HashMap<>(serverProperties));
            return this;
        }

        public Builder setConsumerProperties(Map<String, String> consumerProperties) {
            this.consumerProperties = Collections.unmodifiableMap(new HashMap<>(consumerProperties));
            return this;
        }

        public Builder setProducerProperties(Map<String, String> producerProperties) {
            this.producerProperties = Collections.unmodifiableMap(new HashMap<>(producerProperties));
            return this;
        }

        public Builder setAdminClientProperties(Map<String, String> adminClientProperties) {
            this.adminClientProperties = Collections.unmodifiableMap(new HashMap<>(adminClientProperties));
            return this;
        }

        public Builder setSaslServerProperties(Map<String, String> saslServerProperties) {
            this.saslServerProperties = Collections.unmodifiableMap(new HashMap<>(saslServerProperties));
            return this;
        }

        public Builder setSaslClientProperties(Map<String, String> saslClientProperties) {
            this.saslClientProperties = Collections.unmodifiableMap(new HashMap<>(saslClientProperties));
            return this;
        }

        public Builder setPerServerProperties(Map<Integer, Map<String, String>> perServerProperties) {
            this.perServerProperties = Collections.unmodifiableMap(
                    perServerProperties.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableMap(new HashMap<>(e.getValue())))));
            return this;
        }

        public Builder setTags(List<String> tags) {
            this.tags = Collections.unmodifiableList(new ArrayList<>(tags));
            return this;
        }

        public ClusterConfig build() {
            return new ClusterConfig(types, brokers, controllers, disksPerBroker, autoStart, securityProtocol, listenerName,
                    trustStoreFile, metadataVersion, serverProperties, producerProperties, consumerProperties,
                    adminClientProperties, saslServerProperties, saslClientProperties,
                    perServerProperties, tags);
        }
    }
}
