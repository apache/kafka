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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public class QuotaConfig {
    public static final String NUM_QUOTA_SAMPLES_CONFIG = "quota.window.num";
    public static final String NUM_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for client quotas";
    public static final String NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG = "controller.quota.window.num";
    public static final String NUM_CONTROLLER_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for controller mutation quotas";
    public static final String NUM_REPLICATION_QUOTA_SAMPLES_CONFIG = "replication.quota.window.num";
    public static final String NUM_REPLICATION_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for replication quotas";
    public static final String NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG = "alter.log.dirs.replication.quota.window.num";
    public static final String NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC = "The number of samples to retain in memory for alter log dirs replication quotas";

    // Always have 10 whole windows + 1 current window
    public static final int NUM_QUOTA_SAMPLES_DEFAULT = 11;

    public static final String QUOTA_WINDOW_SIZE_SECONDS_CONFIG = "quota.window.size.seconds";
    public static final String QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for client quotas";
    public static final String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG = "controller.quota.window.size.seconds";
    public static final String CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for controller mutations quotas";
    public static final String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG = "replication.quota.window.size.seconds";
    public static final String REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for replication quotas";
    public static final String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG = "alter.log.dirs.replication.quota.window.size.seconds";
    public static final String ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC = "The time span of each sample for alter log dirs replication quotas";
    public static final int QUOTA_WINDOW_SIZE_SECONDS_DEFAULT = 1;

    public static final String CLIENT_QUOTA_CALLBACK_CLASS_CONFIG = "client.quota.callback.class";
    public static final String CLIENT_QUOTA_CALLBACK_CLASS_DOC = "The fully qualified name of a class that implements the ClientQuotaCallback interface, " +
            "which is used to determine quota limits applied to client requests. By default, the &lt;user&gt; and &lt;client-id&gt; " +
            "quotas that are stored in ZooKeeper are applied. For any given request, the most specific quota that matches the user principal " +
            "of the session and the client-id of the request is applied.";

    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_DOC = "A list of replicas for which log replication should be throttled on " +
            "the leader side. The list should describe a set of replicas in the form " +
            "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
            "all replicas for this topic.";
    public static final List<String> LEADER_REPLICATION_THROTTLED_REPLICAS_DEFAULT = Collections.emptyList();

    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC = "A list of replicas for which log replication should be throttled on " +
            "the follower side. The list should describe a set of " + "replicas in the form " +
            "[PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle " +
            "all replicas for this topic.";
    public static final List<String> FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DEFAULT = Collections.emptyList();


    public static final String LEADER_REPLICATION_THROTTLED_RATE_CONFIG = "leader.replication.throttled.rate";
    public static final String LEADER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
            String.format("property %s (for each topic). This property can be only set dynamically. It is suggested that the ", LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG) +
            "limit be kept above 1MB/s for accurate behaviour.";

    public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG = "follower.replication.throttled.rate";
    public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_DOC = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
            String.format("property %s (for each topic). This property can be only set dynamically. It is suggested that the ", FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG) +
            "limit be kept above 1MB/s for accurate behaviour.";
    public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG = "replica.alter.log.dirs.io.max.bytes.per.second";
    public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC = "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between log directories on the same broker. " +
            "This property can be only set dynamically. It is suggested that the limit be kept above 1MB/s for accurate behaviour.";
    public static final long QUOTA_BYTES_PER_SECOND_DEFAULT = Long.MAX_VALUE;

    public static final String PRODUCER_BYTE_RATE_OVERRIDE_CONFIG = "producer_byte_rate";
    public static final String CONSUMER_BYTE_RATE_OVERRIDE_CONFIG = "consumer_byte_rate";
    public static final String REQUEST_PERCENTAGE_OVERRIDE_CONFIG = "request_percentage";
    public static final String CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG = "controller_mutation_rate";
    public static final String IP_CONNECTION_RATE_OVERRIDE_CONFIG = "connection_creation_rate";
    public static final String PRODUCER_BYTE_RATE_DOC = "A rate representing the upper bound (bytes/sec) for producer traffic.";
    public static final String CONSUMER_BYTE_RATE_DOC = "A rate representing the upper bound (bytes/sec) for consumer traffic.";
    public static final String REQUEST_PERCENTAGE_DOC = "A percentage representing the upper bound of time spent for processing requests.";
    public static final String CONTROLLER_MUTATION_RATE_DOC = "The rate at which mutations are accepted for the create " +
            "topics request, the create partitions request and the delete topics request. The rate is accumulated by " +
            "the number of partitions created or deleted.";
    public static final String IP_CONNECTION_RATE_DOC = "An int representing the upper bound of connections accepted " +
            "for the specified IP.";

    public static final int IP_CONNECTION_RATE_DEFAULT = Integer.MAX_VALUE;

    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(QuotaConfig.NUM_QUOTA_SAMPLES_CONFIG, INT, QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfig.NUM_QUOTA_SAMPLES_DOC)
            .define(QuotaConfig.NUM_REPLICATION_QUOTA_SAMPLES_CONFIG, INT, QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfig.NUM_REPLICATION_QUOTA_SAMPLES_DOC)
            .define(QuotaConfig.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG, INT, QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfig.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_DOC)
            .define(QuotaConfig.NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG, INT, QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT, atLeast(1), LOW, QuotaConfig.NUM_CONTROLLER_QUOTA_SAMPLES_DOC)
            .define(QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DOC)
            .define(QuotaConfig.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfig.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
            .define(QuotaConfig.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfig.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_DOC)
            .define(QuotaConfig.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG, INT, QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT, atLeast(1), LOW, QuotaConfig.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_DOC)
            .define(QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, CLASS, null, LOW, QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_DOC);
    private static final Set<String> USER_AND_CLIENT_QUOTA_NAMES = Set.of(
            PRODUCER_BYTE_RATE_OVERRIDE_CONFIG,
            CONSUMER_BYTE_RATE_OVERRIDE_CONFIG,
            REQUEST_PERCENTAGE_OVERRIDE_CONFIG,
            CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG
    );

    private static void buildUserClientQuotaConfigDef(ConfigDef configDef) {
        configDef.define(PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, ConfigDef.Type.LONG, Long.MAX_VALUE,
                ConfigDef.Importance.MEDIUM, PRODUCER_BYTE_RATE_DOC);

        configDef.define(CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, ConfigDef.Type.LONG, Long.MAX_VALUE,
                ConfigDef.Importance.MEDIUM, CONSUMER_BYTE_RATE_DOC);

        configDef.define(REQUEST_PERCENTAGE_OVERRIDE_CONFIG, ConfigDef.Type.DOUBLE,
                Integer.valueOf(Integer.MAX_VALUE).doubleValue(),
                ConfigDef.Importance.MEDIUM, REQUEST_PERCENTAGE_DOC);

        configDef.define(CONTROLLER_MUTATION_RATE_OVERRIDE_CONFIG, ConfigDef.Type.DOUBLE,
                Integer.valueOf(Integer.MAX_VALUE).doubleValue(),
                ConfigDef.Importance.MEDIUM, CONTROLLER_MUTATION_RATE_DOC);
    }

    private final int numQuotaSamples;
    private final int quotaWindowSizeSeconds;
    private final int numReplicationQuotaSamples;
    private final int replicationQuotaWindowSizeSeconds;
    private final int numAlterLogDirsReplicationQuotaSamples;
    private final int alterLogDirsReplicationQuotaWindowSizeSeconds;
    private final int numControllerQuotaSamples;
    private final int controllerQuotaWindowSizeSeconds;

    public QuotaConfig(AbstractConfig config) {
        this.numQuotaSamples = config.getInt(QuotaConfig.NUM_QUOTA_SAMPLES_CONFIG);
        this.quotaWindowSizeSeconds = config.getInt(QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
        this.numReplicationQuotaSamples = config.getInt(QuotaConfig.NUM_REPLICATION_QUOTA_SAMPLES_CONFIG);
        this.replicationQuotaWindowSizeSeconds = config.getInt(QuotaConfig.REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
        this.numAlterLogDirsReplicationQuotaSamples = config.getInt(QuotaConfig.NUM_ALTER_LOG_DIRS_REPLICATION_QUOTA_SAMPLES_CONFIG);
        this.alterLogDirsReplicationQuotaWindowSizeSeconds = config.getInt(QuotaConfig.ALTER_LOG_DIRS_REPLICATION_QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
        this.numControllerQuotaSamples = config.getInt(QuotaConfig.NUM_CONTROLLER_QUOTA_SAMPLES_CONFIG);
        this.controllerQuotaWindowSizeSeconds = config.getInt(QuotaConfig.CONTROLLER_QUOTA_WINDOW_SIZE_SECONDS_CONFIG);
    }

   /**
     * Gets the number of samples to retain in memory for client quotas.
     */
    public int numQuotaSamples() {
        return numQuotaSamples;
    }

    /**
     * Gets the time span of each sample for client quotas.
     */
    public int quotaWindowSizeSeconds() {
        return quotaWindowSizeSeconds;
    }

    /**
     * Gets the number of samples to retain in memory for replication quotas.
     */
    public int numReplicationQuotaSamples() {
        return numReplicationQuotaSamples;
    }

    /**
     * Gets the time span of each sample for replication quotas.
     */
    public int replicationQuotaWindowSizeSeconds() {
        return replicationQuotaWindowSizeSeconds;
    }

    /**
     * Gets the number of samples to retain in memory for alter log dirs replication quotas.
     */
    public int numAlterLogDirsReplicationQuotaSamples() {
        return numAlterLogDirsReplicationQuotaSamples;
    }

    /**
     * Gets the time span of each sample for alter log dirs replication quotas.
     */
    public int alterLogDirsReplicationQuotaWindowSizeSeconds() {
        return alterLogDirsReplicationQuotaWindowSizeSeconds;
    }

    /**
     * Gets the number of samples to retain in memory for controller mutation quotas.
     */
    public int numControllerQuotaSamples() {
        return numControllerQuotaSamples;
    }

    /**
     * Gets the time span of each sample for controller mutations quotas.
     */
    public int controllerQuotaWindowSizeSeconds() {
        return controllerQuotaWindowSizeSeconds;
    }

    public static ConfigDef brokerQuotaConfigs() {
        return new ConfigDef()
                // Round minimum value down, to make it easier for users.
                .define(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ConfigDef.Type.LONG,
                        QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.MEDIUM, QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_DOC)
                .define(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ConfigDef.Type.LONG,
                        QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.MEDIUM, QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_DOC)
                .define(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ConfigDef.Type.LONG,
                        QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT, ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.MEDIUM, QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC);
    }

    public static ConfigDef userAndClientQuotaConfigs() {
        ConfigDef configDef = new ConfigDef();
        buildUserClientQuotaConfigDef(configDef);
        return configDef;
    }

    public static ConfigDef scramMechanismsPlusUserAndClientQuotaConfigs() {
        ConfigDef configDef = new ConfigDef();
        ScramMechanism.mechanismNames().forEach(mechanismName -> {
            configDef.define(mechanismName, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "User credentials for SCRAM mechanism " + mechanismName);
        });
        buildUserClientQuotaConfigDef(configDef);
        return configDef;
    }

    public static ConfigDef ipConfigs() {
        ConfigDef configDef = new ConfigDef();
        configDef.define(IP_CONNECTION_RATE_OVERRIDE_CONFIG, ConfigDef.Type.INT, Integer.MAX_VALUE,
                ConfigDef.Range.atLeast(0), ConfigDef.Importance.MEDIUM, IP_CONNECTION_RATE_DOC);
        return configDef;
    }

    public static Boolean isClientOrUserQuotaConfig(String name) {
        return USER_AND_CLIENT_QUOTA_NAMES.contains(name);
    }
}
