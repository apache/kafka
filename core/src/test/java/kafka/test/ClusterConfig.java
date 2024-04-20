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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

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

    private final Properties serverProperties;
    private final Properties producerProperties;
    private final Properties consumerProperties;
    private final Properties adminClientProperties;
    private final Properties saslServerProperties;
    private final Properties saslClientProperties;
    private final Map<Integer, Properties> perBrokerOverrideProperties = new HashMap<>();

    @SuppressWarnings("checkstyle:ParameterNumber")
    ClusterConfig(Type type, int brokers, int controllers, String name, boolean autoStart,
                  SecurityProtocol securityProtocol, String listenerName, File trustStoreFile,
                  MetadataVersion metadataVersion, Properties serverProperties, Properties producerProperties,
                  Properties consumerProperties, Properties adminClientProperties, Properties saslServerProperties,
                  Properties saslClientProperties, Map<Integer, Properties> perBrokerOverrideProperties) {
        this.type = type;
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
        this.autoStart = autoStart;
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
        this.trustStoreFile = trustStoreFile;
        this.metadataVersion = metadataVersion;
        this.serverProperties = copyOf(serverProperties);
        this.producerProperties = copyOf(producerProperties);
        this.consumerProperties = copyOf(consumerProperties);
        this.adminClientProperties = copyOf(adminClientProperties);
        this.saslServerProperties = copyOf(saslServerProperties);
        this.saslClientProperties = copyOf(saslClientProperties);
        perBrokerOverrideProperties.forEach((brokerId, props) -> this.perBrokerOverrideProperties.put(brokerId, copyOf(props)));
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

    public Properties serverProperties() {
        return copyOf(serverProperties);
    }

    public Properties producerProperties() {
        return copyOf(producerProperties);
    }

    public Properties consumerProperties() {
        return copyOf(consumerProperties);
    }

    public Properties adminClientProperties() {
        return copyOf(adminClientProperties);
    }

    public Properties saslServerProperties() {
        return copyOf(saslServerProperties);
    }

    public Properties saslClientProperties() {
        return copyOf(saslClientProperties);
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

    public Properties brokerServerProperties(int brokerId) {
        Properties brokerOverrideProperties = perBrokerOverrideProperties.get(brokerId);
        if (brokerOverrideProperties == null) {
            return new Properties();
        }
        return copyOf(brokerOverrideProperties);
    }

    public Map<Integer, Properties> perBrokerOverrideProperties() {
        Map<Integer, Properties> properties = new HashMap<>();
        perBrokerOverrideProperties.forEach((brokerId, props) -> properties.put(brokerId, copyOf(props)));
        return properties;
    }

    public Map<String, String> nameTags() {
        Map<String, String> tags = new LinkedHashMap<>(4);
        name().ifPresent(name -> tags.put("Name", name));
        tags.put("MetadataVersion", metadataVersion.toString());
        tags.put("Security", securityProtocol.name());
        listenerName().ifPresent(listener -> tags.put("Listener", listener));
        return tags;
    }

    public static Builder defaultClusterBuilder() {
        return new Builder()
                .type(Type.ZK)
                .brokers(1)
                .controllers(1)
                .autoStart(true)
                .securityProtocol(SecurityProtocol.PLAINTEXT)
                .metadataVersion(MetadataVersion.latestTesting());
    }

    public static Builder clusterBuilder() {
        return new Builder();
    }

    public static Builder clusterBuilder(ClusterConfig clusterConfig) {
        ClusterConfig.Builder builder = new Builder()
                .type(clusterConfig.type)
                .brokers(clusterConfig.brokers)
                .controllers(clusterConfig.controllers)
                .name(clusterConfig.name)
                .autoStart(clusterConfig.autoStart)
                .securityProtocol(clusterConfig.securityProtocol)
                .listenerName(clusterConfig.listenerName)
                .trustStoreFile(clusterConfig.trustStoreFile)
                .metadataVersion(clusterConfig.metadataVersion);
        builder.serverProperties = copyOf(clusterConfig.serverProperties);
        builder.producerProperties = copyOf(clusterConfig.producerProperties);
        builder.consumerProperties = copyOf(clusterConfig.consumerProperties);
        builder.adminClientProperties = copyOf(clusterConfig.adminClientProperties);
        builder.saslServerProperties = copyOf(clusterConfig.saslServerProperties);
        builder.saslClientProperties = copyOf(clusterConfig.saslClientProperties);
        clusterConfig.perBrokerOverrideProperties.forEach((brokerId, props) ->
                builder.perBrokerOverrideProperties.put(brokerId, copyOf(props)));
        return builder;
    }

    private static Properties copyOf(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
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
        private Properties serverProperties = new Properties();
        private Properties producerProperties = new Properties();
        private Properties consumerProperties = new Properties();
        private Properties adminClientProperties = new Properties();
        private Properties saslServerProperties = new Properties();
        private Properties saslClientProperties = new Properties();
        private final Map<Integer, Properties> perBrokerOverrideProperties = new HashMap<>();

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
            perBrokerOverrideProperties.computeIfAbsent(id, ignored -> new Properties()).put(key, value);
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
