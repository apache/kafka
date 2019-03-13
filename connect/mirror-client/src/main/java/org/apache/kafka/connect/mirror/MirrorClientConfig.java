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

public class MirrorClientConfig extends AbstractConfig {
    public static final String REPLICATION_POLICY_CLASS = "replication.policy.class";
    private static final String REPLICATION_POLICY_CLASS_DOC = "replication.policy.class";
    public static final String REPLICATION_POLICY_SEPARATOR = "replication.policy.separator";
    private static final String REPLICATION_POLICY_SEPARATOR_DOC = "replication.policy.separator";
    public static final String REPLICATION_POLICY_SEPARATOR_DEFAULT =
        DefaultReplicationPolicy.SEPARATOR_DEFAULT;
    
    public static final String ADMIN_CLIENT_PREFIX = "admin.";
    public static final String CONSUMER_CLIENT_PREFIX = "consumer.";

    static final String CHECKPOINTS_TOPIC = "checkpoints-internal"; // internal so not replicated
    static final String HEARTBEATS_TOPIC = "heartbeats";
 
    MirrorClientConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props, false);
    }

    ReplicationPolicy replicationPolicy() {
        return getConfiguredInstance(REPLICATION_POLICY_CLASS, ReplicationPolicy.class);
    }

    long maxOffsetLag() {
        // hard-coded b/c we don't expose this property yet
        return 100;
    }

    Map<String, Object> adminConfig() {
        return valuesWithPrefixOverride(ADMIN_CLIENT_PREFIX);
    }

    Map<String, Object> consumerConfig() {
        return valuesWithPrefixOverride(CONSUMER_CLIENT_PREFIX);
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
            DefaultReplicationPolicy.class.getName(),
            ConfigDef.Importance.LOW,
            REPLICATION_POLICY_CLASS_DOC)
        .withClientSslSupport()
        .withClientSaslSupport();
}
