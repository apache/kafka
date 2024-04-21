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
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents a requested configuration of a Kafka cluster for integration testing
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
    ClusterConfig(Type type, int brokers, int controllers, String name, boolean autoStart,
                  SecurityProtocol securityProtocol, String listenerName, File trustStoreFile,
                  MetadataVersion metadataVersion, Map<String, String> serverProperties, Map<String, String> producerProperties,
                  Map<String, String> consumerProperties, Map<String, String> adminClientProperties, Map<String, String> saslServerProperties,
                  Map<String, String> saslClientProperties, Map<Integer, Map<String, String>> perBrokerOverrideProperties) {
        this.type = type;
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
        this.autoStart = autoStart;
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
        this.trustStoreFile = trustStoreFile;
        this.metadataVersion = metadataVersion;
        this.serverProperties = Collections.unmodifiableMap(serverProperties);
        this.producerProperties = Collections.unmodifiableMap(producerProperties);
        this.consumerProperties = Collections.unmodifiableMap(consumerProperties);
        this.adminClientProperties = Collections.unmodifiableMap(adminClientProperties);
        this.saslServerProperties = Collections.unmodifiableMap(saslServerProperties);
        this.saslClientProperties = Collections.unmodifiableMap(saslClientProperties);
        this.perBrokerOverrideProperties = Collections.unmodifiableMap(
                perBrokerOverrideProperties.entrySet().stream()
                        .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), Collections.unmodifiableMap(e.getValue())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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

    public Map<String, String> brokerServerProperties(int brokerId) {
        return perBrokerOverrideProperties.getOrDefault(brokerId, Collections.emptyMap());
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

    public static Builder defaultBuilder() {
        return new Builder()
                .type(Type.ZK)
                .brokers(1)
                .controllers(1)
                .autoStart(true)
                .securityProtocol(SecurityProtocol.PLAINTEXT)
                .metadataVersion(MetadataVersion.latestTesting());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(ClusterConfig clusterConfig) {
        return new Builder(clusterConfig);
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
        private Map<String, String> serverProperties = new HashMap<>();
        private Map<String, String> producerProperties = new HashMap<>();
        private Map<String, String> consumerProperties = new HashMap<>();
        private Map<String, String> adminClientProperties = new HashMap<>();
        private Map<String, String> saslServerProperties = new HashMap<>();
        private Map<String, String> saslClientProperties = new HashMap<>();
        private Map<Integer, Map<String, String>> perBrokerOverrideProperties = new HashMap<>();

        Builder() {}

        Builder(ClusterConfig clusterConfig) {
            this.type = clusterConfig.type;
            this.brokers = clusterConfig.brokers;
            this.controllers = clusterConfig.controllers;
            this.name = clusterConfig.name;
            this.autoStart = clusterConfig.autoStart;
            this.securityProtocol = clusterConfig.securityProtocol;
            this.listenerName = clusterConfig.listenerName;
            this.trustStoreFile = clusterConfig.trustStoreFile;
            this.metadataVersion = clusterConfig.metadataVersion;
            this.serverProperties = new HashMap<>(clusterConfig.serverProperties);
            this.producerProperties = new HashMap<>(clusterConfig.producerProperties);
            this.consumerProperties = new HashMap<>(clusterConfig.consumerProperties);
            this.adminClientProperties = new HashMap<>(clusterConfig.adminClientProperties);
            this.saslServerProperties = new HashMap<>(clusterConfig.saslServerProperties);
            this.saslClientProperties = new HashMap<>(clusterConfig.saslClientProperties);
            Map<Integer, Map<String, String>> perBrokerOverrideProps = new HashMap<>();
            clusterConfig.perBrokerOverrideProperties.forEach((k, v) -> perBrokerOverrideProps.put(k, new HashMap<>(v)));
            this.perBrokerOverrideProperties = perBrokerOverrideProps;
        }

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder brokers(int brokers) {
            this.brokers = brokers;
            return this;
        }

        public Builder controllers(int controllers) {
            this.controllers = controllers;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder autoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        public Builder securityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder listenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public Builder trustStoreFile(File trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            return this;
        }

        public Builder metadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        public Builder putServerProperty(String key, String value) {
            serverProperties.put(key, value);
            return this;
        }

        public Builder putConsumerProperty(String key, String value) {
            consumerProperties.put(key, value);
            return this;
        }

        public Builder putProducerProperty(String key, String value) {
            producerProperties.put(key, value);
            return this;
        }

        public Builder putAdminClientProperty(String key, String value) {
            adminClientProperties.put(key, value);
            return this;
        }

        public Builder putSaslServerProperty(String key, String value) {
            saslServerProperties.put(key, value);
            return this;
        }

        public Builder putSaslClientProperty(String key, String value) {
            saslClientProperties.put(key, value);
            return this;
        }

        public Builder putPerBrokerProperty(int id, String key, String value) {
            perBrokerOverrideProperties.computeIfAbsent(id, ignored -> new HashMap<>()).put(key, value);
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
