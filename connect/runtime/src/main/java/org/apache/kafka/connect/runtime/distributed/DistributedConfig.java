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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class DistributedConfig extends WorkerConfig {
    private static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */

    /**
     * <code>group.id</code>
     */
    public static final String GROUP_ID_CONFIG = "group.id";
    private static final String GROUP_ID_DOC = "A unique string that identifies the Connect cluster group this worker belongs to.";

    /**
     * <code>session.timeout.ms</code>
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
    private static final String SESSION_TIMEOUT_MS_DOC = "The timeout used to detect worker failures. " +
            "The worker sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are " +
            "received by the broker before the expiration of this session timeout, then the broker will remove the " +
            "worker from the group and initiate a rebalance. Note that the value must be in the allowable range as " +
            "configured in the broker configuration by <code>group.min.session.timeout.ms</code> " +
            "and <code>group.max.session.timeout.ms</code>.";

    /**
     * <code>heartbeat.interval.ms</code>
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
    private static final String HEARTBEAT_INTERVAL_MS_DOC = "The expected time between heartbeats to the group " +
            "coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the " +
            "worker's session stays active and to facilitate rebalancing when new members join or leave the group. " +
            "The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher " +
            "than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.";

    /**
     * <code>rebalance.timeout.ms</code>
     */
    public static final String REBALANCE_TIMEOUT_MS_CONFIG = "rebalance.timeout.ms";
    private static final String REBALANCE_TIMEOUT_MS_DOC = "The maximum allowed time for each worker to join the group " +
            "once a rebalance has begun. This is basically a limit on the amount of time needed for all tasks to " +
            "flush any pending data and commit offsets. If the timeout is exceeded, then the worker will be removed " +
            "from the group, which will cause offset commit failures.";

    /**
     * <code>worker.sync.timeout.ms</code>
     */
    public static final String WORKER_SYNC_TIMEOUT_MS_CONFIG = "worker.sync.timeout.ms";
    private static final String WORKER_SYNC_TIMEOUT_MS_DOC = "When the worker is out of sync with other workers and needs" +
            " to resynchronize configurations, wait up to this amount of time before giving up, leaving the group, and" +
            " waiting a backoff period before rejoining.";

    /**
     * <code>group.unsync.timeout.ms</code>
     */
    public static final String WORKER_UNSYNC_BACKOFF_MS_CONFIG = "worker.unsync.backoff.ms";
    private static final String WORKER_UNSYNC_BACKOFF_MS_DOC = "When the worker is out of sync with other workers and " +
            " fails to catch up within worker.sync.timeout.ms, leave the Connect cluster for this long before rejoining.";
    public static final int WORKER_UNSYNC_BACKOFF_MS_DEFAULT = 5 * 60 * 1000;

    /**
     * <code>offset.storage.topic</code>
     */
    public static final String OFFSET_STORAGE_TOPIC_CONFIG = "offset.storage.topic";
    private static final String OFFSET_STORAGE_TOPIC_CONFIG_DOC = "The name of the Kafka topic where connector offsets are stored";

    /**
     * <code>offset.storage.partitions</code>
     */
    public static final String OFFSET_STORAGE_PARTITIONS_CONFIG = "offset.storage.partitions";
    private static final String OFFSET_STORAGE_PARTITIONS_CONFIG_DOC = "The number of partitions used when creating the offset storage topic";

    /**
     * <code>offset.storage.replication.factor</code>
     */
    public static final String OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG = "offset.storage.replication.factor";
    private static final String OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used when creating the offset storage topic";

    /**
     * <code>config.storage.topic</code>
     */
    public static final String CONFIG_TOPIC_CONFIG = "config.storage.topic";
    private static final String CONFIG_TOPIC_CONFIG_DOC = "The name of the Kafka topic where connector configurations are stored";

    /**
     * <code>config.storage.replication.factor</code>
     */
    public static final String CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG = "config.storage.replication.factor";
    private static final String CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used when creating the configuration storage topic";

    /**
     * <code>status.storage.topic</code>
     */
    public static final String STATUS_STORAGE_TOPIC_CONFIG = "status.storage.topic";
    public static final String STATUS_STORAGE_TOPIC_CONFIG_DOC = "The name of the Kafka topic where connector and task status are stored";

    /**
     * <code>status.storage.partitions</code>
     */
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG = "status.storage.partitions";
    private static final String STATUS_STORAGE_PARTITIONS_CONFIG_DOC = "The number of partitions used when creating the status storage topic";

    /**
     * <code>status.storage.replication.factor</code>
     */
    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG = "status.storage.replication.factor";
    private static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "Replication factor used when creating the status storage topic";

    static {
        CONFIG = baseConfigDef()
                .define(GROUP_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        GROUP_ID_DOC)
                .define(SESSION_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        10000,
                        ConfigDef.Importance.HIGH,
                        SESSION_TIMEOUT_MS_DOC)
                .define(REBALANCE_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        60000,
                        ConfigDef.Importance.HIGH,
                        REBALANCE_TIMEOUT_MS_DOC)
                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                        ConfigDef.Type.INT,
                        3000,
                        ConfigDef.Importance.HIGH,
                        HEARTBEAT_INTERVAL_MS_DOC)
                .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                        ConfigDef.Type.LONG,
                        5 * 60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                .define(CommonClientConfigs.CLIENT_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.CLIENT_ID_DOC)
                .define(CommonClientConfigs.SEND_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        128 * 1024,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SEND_BUFFER_DOC)
                .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                        ConfigDef.Type.INT,
                        32 * 1024,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        50L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        1000L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        100L,
                        atLeast(0L),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        40 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
                        /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                // security support
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport()
                .define(WORKER_SYNC_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        3000,
                        ConfigDef.Importance.MEDIUM,
                        WORKER_SYNC_TIMEOUT_MS_DOC)
                .define(WORKER_UNSYNC_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.INT,
                        WORKER_UNSYNC_BACKOFF_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        WORKER_UNSYNC_BACKOFF_MS_DOC)
                .define(OFFSET_STORAGE_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        OFFSET_STORAGE_TOPIC_CONFIG_DOC)
                .define(OFFSET_STORAGE_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        25,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        OFFSET_STORAGE_PARTITIONS_CONFIG_DOC)
                .define(OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        (short) 3,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
                .define(CONFIG_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CONFIG_TOPIC_CONFIG_DOC)
                .define(CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        (short) 3,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
                .define(STATUS_STORAGE_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        STATUS_STORAGE_TOPIC_CONFIG_DOC)
                .define(STATUS_STORAGE_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        5,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        STATUS_STORAGE_PARTITIONS_CONFIG_DOC)
                .define(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        (short) 3,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC);
    }

    public DistributedConfig(Map<String, String> props) {
        super(CONFIG, props);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }
}
