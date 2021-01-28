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

import kafka.server.KafkaConfig;
import kafka.test.annotation.Type;
import org.apache.kafka.common.security.auth.SecurityProtocol;

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
    private final Properties serverProperties;
    private final String ibp;
    private final String securityProtocol;
    private final String listenerName;

    private final Properties adminClientProperties = new Properties();
    private final Properties saslServerProperties = new Properties();
    private final Properties saslClientProperties = new Properties();

    ClusterConfig(Type type, int brokers, int controllers, String name, boolean autoStart,
                  Properties serverProperties, String ibp,
                  String securityProtocol, String listenerName) {
        this.type = type;
        this.brokers = brokers;
        this.controllers = controllers;
        this.name = name;
        this.autoStart = autoStart;
        this.serverProperties = serverProperties;
        this.ibp = ibp;
        this.securityProtocol = securityProtocol;
        this.listenerName = listenerName;
    }

    public Type clusterType() {
        return type;
    }

    public int brokers() {
        return brokers;
    }

    public int controllers() {
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

    public Properties adminClientProperties() {
        return adminClientProperties;
    }

    public Optional<String> ibp() {
        return Optional.ofNullable(ibp);
    }

    public String securityProtocol() {
        return securityProtocol;
    }

    public Optional<String> listenerName() {
        return Optional.ofNullable(listenerName);
    }

    public Properties saslServerProperties() {
        return saslServerProperties;
    }

    public Properties saslClientProperties() {
        return saslClientProperties;
    }

    public Map<String, String> nameTags() {
        Map<String, String> tags = new LinkedHashMap<>(3);
        name().ifPresent(name -> tags.put("Name", name));
        ibp().ifPresent(ibp -> tags.put("IBP", ibp));
        tags.put("security", securityProtocol);
        listenerName().ifPresent(listener -> tags.put("listener", listener));
        return tags;
    }

    public ClusterConfig copyOf() {
        Properties props = new Properties();
        props.putAll(serverProperties);
        return new ClusterConfig(type, brokers, controllers, name, autoStart, props, ibp, securityProtocol, listenerName);
    }

    public static Builder defaultClusterBuilder() {
        return new Builder(Type.Zk, 1, 1, true, SecurityProtocol.PLAINTEXT.name);
    }

    public static Builder clusterBuilder(Type type, int brokers, int controllers, boolean autoStart, String securityProtocol) {
        return new Builder(type, brokers, controllers, autoStart, securityProtocol);
    }

    public static class Builder {
        private Type type;
        private int brokers;
        private int controllers;
        private String name;
        private boolean autoStart;
        private Properties serverProperties;
        private String ibp;
        private String securityProtocol;
        private String listenerName;

        Builder(Type type, int brokers, int controllers, boolean autoStart, String securityProtocol) {
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

        public Builder serverProperties(Properties serverProperties) {
            this.serverProperties = serverProperties;
            return this;
        }

        public Builder ibp(String interBrokerProtocol) {
            this.ibp = interBrokerProtocol;
            return this;
        }

        public Builder securityProtocol(String securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder listenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public ClusterConfig build() {
            Properties props = new Properties();
            if (serverProperties != null) {
                props.putAll(serverProperties);
            }
            if (ibp != null) {
                props.put(KafkaConfig.InterBrokerProtocolVersionProp(), ibp);
            }
            return new ClusterConfig(type, brokers, controllers, name, autoStart, props, ibp, securityProtocol, listenerName);
        }
    }
}
