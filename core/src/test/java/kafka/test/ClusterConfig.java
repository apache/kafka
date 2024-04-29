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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents an immutable requested configuration of a Kafka cluster for integration testing.
 */
public class ClusterConfig {

    private final Type type;
    private final int brokers;
    private final int controllers;
    private final String name;
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
    private final Map<Integer, Map<String, String>> perBrokerOverrideProperties;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private ClusterConfig(Type type, int brokers, int controllers, String name, boolean autoStart,
                  SecurityProtocol securityProtocol, String listenerName, File trustStoreFile,
                  MetadataVersion metadataVersion, Map<String, String> serverProperties, Map<String, String> producerProperties,
                  Map<String, String> consumerProperties, Map<String, String> adminClientProperties, Map<String, String> saslServerProperties,
                  Map<String, String> saslClientProperties, Map<Integer, Map<String, String>> perBrokerOverrideProperties) {
        this.type = Objects.requireNonNull(type);
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
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
        this.perBrokerOverrideProperties = Objects.requireNonNull(perBrokerOverrideProperties);
    }

    public Type clusterType() {
        return type;
    }

    public int numBrokers() {
        return brokers;
    }

    public int numControllers() {
        return controllers;
    }

    public Optional<String> name() {
        return Optional.ofNullable(name);
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

    public Map<Integer, Map<String, String>> perBrokerOverrideProperties() {
        return perBrokerOverrideProperties;
    }

    public Map<String, String> nameTags() {
        Map<String, String> tags = new LinkedHashMap<>(4);
        name().ifPresent(name -> tags.put("Name", name));
        tags.put("MetadataVersion", metadataVersion.toString());
        tags.put("Security", securityProtocol.name());
        listenerName().ifPresent(listener -> tags.put("Listener", listener));
        return tags;
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ClusterConfig clusterConfig = (ClusterConfig) object;
        return Objects.equals(type, clusterConfig.type)
                && Objects.equals(brokers, clusterConfig.brokers)
                && Objects.equals(controllers, clusterConfig.controllers)
                && Objects.equals(name, clusterConfig.name)
                && Objects.equals(autoStart, clusterConfig.autoStart)
                && Objects.equals(securityProtocol, clusterConfig.securityProtocol)
                && Objects.equals(listenerName, clusterConfig.listenerName)
                && Objects.equals(trustStoreFile, clusterConfig.trustStoreFile)
                && Objects.equals(metadataVersion, clusterConfig.metadataVersion)
                && Objects.equals(serverProperties, clusterConfig.serverProperties)
                && Objects.equals(producerProperties, clusterConfig.producerProperties)
                && Objects.equals(consumerProperties, clusterConfig.consumerProperties)
                && Objects.equals(adminClientProperties, clusterConfig.adminClientProperties)
                && Objects.equals(saslServerProperties, clusterConfig.saslServerProperties)
                && Objects.equals(saslClientProperties, clusterConfig.saslClientProperties)
                && Objects.equals(perBrokerOverrideProperties, clusterConfig.perBrokerOverrideProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, brokers, controllers, name, autoStart, securityProtocol, listenerName,
                trustStoreFile, metadataVersion, serverProperties, producerProperties, consumerProperties,
                adminClientProperties, saslServerProperties, saslClientProperties, perBrokerOverrideProperties);
    }

    public static Builder defaultBuilder() {
        return new Builder()
                .setType(Type.ZK)
                .setBrokers(1)
                .setControllers(1)
                .setAutoStart(true)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .setMetadataVersion(MetadataVersion.latestTesting());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ClusterConfig clusterConfig) {
        return new Builder()
                .setType(clusterConfig.type)
                .setBrokers(clusterConfig.brokers)
                .setControllers(clusterConfig.controllers)
                .setName(clusterConfig.name)
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
                .setPerBrokerProperties(clusterConfig.perBrokerOverrideProperties);
    }

    public static class Builder {
        private Type type;
        private int brokers;
        private int controllers;
        private String name;
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
        private Map<Integer, Map<String, String>> perBrokerOverrideProperties = Collections.emptyMap();

        private Builder() {}

        public Builder setType(Type type) {
            this.type = type;
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

        public Builder setName(String name) {
            this.name = name;
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

        public Builder setPerBrokerProperties(Map<Integer, Map<String, String>> perBrokerOverrideProperties) {
            this.perBrokerOverrideProperties = Collections.unmodifiableMap(
                    perBrokerOverrideProperties.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableMap(new HashMap<>(e.getValue())))));
            return this;
        }

        public ClusterConfig build() {
            return new ClusterConfig(type, brokers, controllers, name, autoStart, securityProtocol, listenerName,
                    trustStoreFile, metadataVersion, serverProperties, producerProperties, consumerProperties,
                    adminClientProperties, saslServerProperties, saslClientProperties,
                    perBrokerOverrideProperties);
        }
    }
}
