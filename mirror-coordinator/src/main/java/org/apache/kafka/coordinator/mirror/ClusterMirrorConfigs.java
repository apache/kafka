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
package org.apache.kafka.coordinator.mirror;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;

/**
 * This class provides proper validation, defaults, and documentation
 * for all configurations supported by the Cluster Mirroring feature.
 */
public class ClusterMirrorConfigs {
    // ---------------------------------------------------------------
    // Broker level configs. Set when starting/rebooting the broker. Stored in server.properties or dynamic broker config.
    // ---------------------------------------------------------------

    public static final String MIRROR_COORDINATOR_THREADS_CONFIG = "mirror.coordinator.threads";
    private static final int MIRROR_COORDINATOR_THREADS_DEFAULT = 1;
    private static final String MIRROR_COORDINATOR_THREADS_DOC = "The number of threads used to process coordinator events for cluster mirroring.";

    public static final String MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_CONFIG = "mirror.coordinator.write.timeout.ms";
    private static final long MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DEFAULT = 5000L;
    private static final String MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DOC = "The timeout in milliseconds for write operations on the mirror state coordinator.";

    public static final String MIRROR_COORDINATOR_APPEND_LINGER_MS_CONFIG = "mirror.coordinator.append.linger.ms";
    private static final int MIRROR_COORDINATOR_APPEND_LINGER_MS_DEFAULT = 10;
    private static final String MIRROR_COORDINATOR_APPEND_LINGER_MS_DOC = "The linger time in milliseconds for batching records before flushing to the mirror state topic.";

    public static final String MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_CONFIG = "mirror.coordinator.load.buffer.size";
    private static final int MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DEFAULT = 5 * 1024 * 1024;
    private static final String MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DOC = "The buffer size in bytes for loading mirror state records during coordinator startup.";

    public static final String MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG = "mirror.state.topic.num.partitions";
    private static final int MIRROR_STATE_TOPIC_NUM_PARTITIONS_DEFAULT = 50;
    private static final String MIRROR_STATE_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions for the internal topic (should not change after deployment).";

    public static final String MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG = "mirror.state.topic.replication.factor";
    private static final short MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    private static final String MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the internal topic. " +
            "Topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String MIRROR_NUM_REPLICA_FETCHERS_CONFIG = "mirror.num.replica.fetchers";
    private static final int MIRROR_NUM_REPLICA_FETCHERS_DEFAULT = 1;
    private static final String MIRROR_NUM_REPLICA_FETCHERS_DOC = "Number of fetcher threads used to replicate records from each source broker in a cluster mirror. " +
            "The total number of mirror fetcher threads on a broker equals this value multiplied by the number of distinct source brokers and the number of cluster mirrors. " +
            "A higher value increases I/O parallelism for cross cluster replication at the cost of higher CPU and memory utilization.";

    public static final String MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG = "mirror.metadata.refresh.interval.ms";
    private static final long MIRROR_METADATA_REFRESH_INTERVAL_MS_DEFAULT = 60000L;
    private static final String MIRROR_METADATA_REFRESH_INTERVAL_MS_DOC = "The interval in milliseconds at which the coordinator refreshes metadata from source clusters. " +
            "This controls how frequently the coordinator polls source clusters to detect new topics and metadata changes.";

    public static final String MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG = "mirror.failed.retry.initial.backoff.ms";
    private static final long MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DEFAULT = 100L;
    private static final String MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DOC = "The initial backoff time in milliseconds before retrying a mirror partition " +
            "in FAILED state. The actual delay uses full jitter: a uniform random value in [0, backoff].";

    public static final String MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG = "mirror.failed.retry.max.backoff.ms";
    private static final long MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DEFAULT = 300000L;
    private static final String MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DOC = "The maximum backoff time in milliseconds for retrying a mirror partition in FAILED state.";

    public static final String MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG = "mirror.failed.retry.max.attempts";
    private static final int MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DEFAULT = 20;
    private static final String MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DOC = "The maximum number of automatic retry attempts for a mirror partition in FAILED state. " +
            "After this limit is reached, manual intervention is required via the start-mirror-topics command. " +
            "Set to 0 to disable automatic retries.";

    private static final ConfigDef BROKER_CONFIG_DEF = new ConfigDef()
            .defineInternal(MIRROR_COORDINATOR_THREADS_CONFIG, INT, MIRROR_COORDINATOR_THREADS_DEFAULT, atLeast(1), LOW, MIRROR_COORDINATOR_THREADS_DOC)
            .defineInternal(MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_CONFIG, LONG, MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DEFAULT, atLeast(1L), LOW, MIRROR_COORDINATOR_WRITE_TIMEOUT_MS_DOC)
            .defineInternal(MIRROR_COORDINATOR_APPEND_LINGER_MS_CONFIG, INT, MIRROR_COORDINATOR_APPEND_LINGER_MS_DEFAULT, atLeast(0), LOW, MIRROR_COORDINATOR_APPEND_LINGER_MS_DOC)
            .defineInternal(MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_CONFIG, INT, MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), LOW, MIRROR_COORDINATOR_LOAD_BUFFER_SIZE_DOC)
            .defineInternal(MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG, INT, MIRROR_STATE_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, MIRROR_STATE_TOPIC_NUM_PARTITIONS_DOC)
            .defineInternal(MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, MIRROR_STATE_TOPIC_REPLICATION_FACTOR_DOC)
            .defineInternal(MIRROR_NUM_REPLICA_FETCHERS_CONFIG, INT, MIRROR_NUM_REPLICA_FETCHERS_DEFAULT, atLeast(1), HIGH, MIRROR_NUM_REPLICA_FETCHERS_DOC)
            .defineInternal(MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG, LONG, MIRROR_METADATA_REFRESH_INTERVAL_MS_DEFAULT, atLeast(0L), MEDIUM, MIRROR_METADATA_REFRESH_INTERVAL_MS_DOC)
            .defineInternal(MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_CONFIG, LONG, MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DEFAULT, atLeast(1L), MEDIUM, MIRROR_FAILED_RETRY_INITIAL_BACKOFF_MS_DOC)
            .defineInternal(MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_CONFIG, LONG, MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DEFAULT, atLeast(1L), MEDIUM, MIRROR_FAILED_RETRY_MAX_BACKOFF_MS_DOC)
            .defineInternal(MIRROR_FAILED_RETRY_MAX_ATTEMPTS_CONFIG, INT, MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DEFAULT, between(0, Short.MAX_VALUE), MEDIUM, MIRROR_FAILED_RETRY_MAX_ATTEMPTS_DOC);

    /**
     * Broker-level mirror configurations.
     * This is merged into AbstractKafkaConfig for validation.
     */
    public static ConfigDef brokerConfigDef() {
        return new ConfigDef(BROKER_CONFIG_DEF);
    }
}
