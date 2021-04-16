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

import java.io.File;
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

    private final Properties serverProperties = new Properties();
    private final Properties producerProperties = new Properties();
    private final Properties consumerProperties = new Properties();
    private final Properties adminClientProperties = new Properties();
    private final Properties saslServerProperties = new Properties();
    private final Properties saslClientProperties = new Properties();

    ClusterConfig(Type type, int brokers, int controllers, String name, boolean autoStart,
                  SecurityProtocol securityProtocol, String listenerName, File trustStoreFile) {
        this.type = type;
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
        this.autoStart = autoStart;
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
        this.trustStoreFile = trustStoreFile;
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
        return serverProperties;
    }

    public Properties producerProperties() {
        return producerProperties;
    }

    public Properties consumerProperties() {
        return consumerProperties;
    }

    public Properties adminClientProperties() {
        return adminClientProperties;
    }

    public Properties saslServerProperties() {
        return saslServerProperties;
    }

    public Properties saslClientProperties() {
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

    public Map<String, String> nameTags() {
        Map<String, String> tags = new LinkedHashMap<>(3);
        name().ifPresent(name -> tags.put("Name", name));
        tags.put("Security", securityProtocol.name());
        listenerName().ifPresent(listener -> tags.put("Listener", listener));
        return tags;
    }

    public ClusterConfig copyOf() {
        ClusterConfig copy = new ClusterConfig(type, brokers, controllers, name, autoStart, securityProtocol, listenerName, trustStoreFile);
        copy.serverProperties.putAll(serverProperties);
        copy.producerProperties.putAll(producerProperties);
        copy.consumerProperties.putAll(consumerProperties);
        copy.saslServerProperties.putAll(saslServerProperties);
        copy.saslClientProperties.putAll(saslClientProperties);
        return copy;
    }

    public static Builder defaultClusterBuilder() {
        return new Builder(Type.ZK, 1, 1, true, SecurityProtocol.PLAINTEXT);
    }

    public static Builder clusterBuilder(Type type, int brokers, int controllers, boolean autoStart, SecurityProtocol securityProtocol) {
        return new Builder(type, brokers, controllers, autoStart, securityProtocol);
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

        Builder(Type type, int brokers, int controllers, boolean autoStart, SecurityProtocol securityProtocol) {
            this.type = type;
            this.brokers = brokers;
            this.controllers = controllers;
            this.autoStart = autoStart;
            this.securityProtocol = securityProtocol;
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

        public ClusterConfig build() {
            return new ClusterConfig(type, brokers, controllers, name, autoStart, securityProtocol, listenerName, trustStoreFile);
        }
    }
}
