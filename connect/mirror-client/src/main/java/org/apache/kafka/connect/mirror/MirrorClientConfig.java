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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MirrorClientConfig extends AbstractConfig {
    public static final String REPLICATION_POLICY_CLASS = "replication.policy.class";
    private static final String REPLICATION_POLICY_CLASS_DOC = "Class which defines the remote topic naming convention.";
    public static final Class REPLICATION_POLICY_CLASS_DEFAULT = DefaultReplicationPolicy.class;
    public static final String REPLICATION_POLICY_SEPARATOR = "replication.policy.separator";
    private static final String REPLICATION_POLICY_SEPARATOR_DOC = "Separator used in remote topic naming convention.";
    public static final String REPLICATION_POLICY_SEPARATOR_DEFAULT =
        DefaultReplicationPolicy.SEPARATOR_DEFAULT;
    
    public static final String ADMIN_CLIENT_PREFIX = "admin.";
    public static final String CONSUMER_CLIENT_PREFIX = "consumer.";
    public static final String PRODUCER_CLIENT_PREFIX = "producer.";

    static final String CHECKPOINTS_TOPIC_SUFFIX = ".checkpoints.internal"; // internal so not replicated
    static final String HEARTBEATS_TOPIC = "heartbeats";
 
    MirrorClientConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, false);
    }

    public ReplicationPolicy replicationPolicy() {
        return getConfiguredInstance(REPLICATION_POLICY_CLASS, ReplicationPolicy.class);
    }

    public Map<String, Object> adminConfig() {
        return clientConfig(ADMIN_CLIENT_PREFIX);
    }

    public Map<String, Object> consumerConfig() {
        return clientConfig(CONSUMER_CLIENT_PREFIX);
    }

    public Map<String, Object> producerConfig() {
        return clientConfig(PRODUCER_CLIENT_PREFIX);
    }

    private Map<String, Object> clientConfig(String prefix) {
        Set<String> names = MirrorMakerConfig.CLIENT_CONFIG_DEF.names();
        return valuesWithPrefixOverride(prefix).entrySet().stream()
            .filter(x -> names.contains(x.getKey()))
            .filter(x -> x.getValue() != null)
            .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            Type.STRING,
            null,
            Importance.HIGH,
            CommonClientConfigs.BOOTSTRAP_SERVERS_DOC) 
        .define(
            REPLICATION_POLICY_CLASS,
            ConfigDef.Type.CLASS,
            REPLICATION_POLICY_CLASS_DEFAULT,
            ConfigDef.Importance.LOW,
            REPLICATION_POLICY_CLASS_DOC)
        .define(
            REPLICATION_POLICY_SEPARATOR,
            ConfigDef.Type.STRING,
            REPLICATION_POLICY_SEPARATOR_DEFAULT,
            ConfigDef.Importance.LOW,
            REPLICATION_POLICY_SEPARATOR_DOC)
        .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .withClientSslSupport()
        .withClientSaslSupport();
}
