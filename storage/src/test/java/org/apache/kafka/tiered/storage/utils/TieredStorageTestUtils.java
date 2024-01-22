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
package org.apache.kafka.tiered.storage.utils;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_PROP;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorage.DELETE_ON_CLOSE_CONFIG;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorage.STORAGE_DIR_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP;

public class TieredStorageTestUtils {

    /**
     * InitialTaskDelayMs is set to 30 seconds for the delete-segment scheduler in Apache Kafka.
     * Hence, we need to wait at least that amount of time before segments eligible for deletion
     * gets physically removed.
     */
    public static final Integer STORAGE_WAIT_TIMEOUT_SEC = 35;
    // The default value of log cleanup interval is 30 secs, and it increases the test execution time.
    private static final Integer LOG_CLEANUP_INTERVAL_MS = 500;
    private static final Integer RLM_TASK_INTERVAL_MS = 500;
    private static final Integer RLMM_INIT_RETRY_INTERVAL_MS = 300;

    public static TopicDescription describeTopic(TieredStorageTestContext context, String topic)
            throws ExecutionException, InterruptedException {
        return describeTopics(context, Collections.singletonList(topic)).get(topic);
    }

    public static Map<String, TopicDescription> describeTopics(TieredStorageTestContext context,
                                                                List<String> topics)
            throws ExecutionException, InterruptedException {
        return context.admin()
                .describeTopics(topics)
                .allTopicNames()
                .get();
    }

    /**
     * Get the records found in the local tiered storage.
     * Snapshot does not sort the filesets by base offset.
     * @param context The test context.
     * @param topicPartition The topic-partition of the records.
     * @return The records found in the local tiered storage.
     */
    public static List<Record> tieredStorageRecords(TieredStorageTestContext context,
                                                    TopicPartition topicPartition) {
        return context.takeTieredStorageSnapshot()
                .getFilesets(topicPartition)
                .stream()
                .map(fileset -> {
                    try {
                        return fileset.getRecords();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sorted(Comparator.comparingLong(records -> records.get(0).offset()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public static Properties createPropsForRemoteStorage(String testClassName,
                                                         String storageDirPath,
                                                         int brokerCount,
                                                         int numRemoteLogMetadataPartitions,
                                                         Properties overridingProps) {
        Assertions.assertTrue(STORAGE_WAIT_TIMEOUT_SEC > TimeUnit.MILLISECONDS.toSeconds(RLM_TASK_INTERVAL_MS),
                "STORAGE_WAIT_TIMEOUT_SEC should be greater than RLM_TASK_INTERVAL_MS");

        // Configure the tiered storage in Kafka. Set an interval of 1 second for the remote log manager background
        // activity to ensure the tiered storage has enough room to be exercised within the lifetime of a test.
        //
        // The replication factor of the remote log metadata topic needs to be chosen so that in resiliency
        // tests, metadata can survive the loss of one replica for its topic-partitions.
        //
        // The second-tier storage system is mocked via the LocalTieredStorage instance which persists transferred
        // data files on the local file system.
        overridingProps.setProperty(REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        overridingProps.setProperty(REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, LocalTieredStorage.class.getName());
        overridingProps.setProperty(REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
                TopicBasedRemoteLogMetadataManager.class.getName());
        overridingProps.setProperty(REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, RLM_TASK_INTERVAL_MS.toString());
        overridingProps.setProperty(REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "PLAINTEXT");

        overridingProps.setProperty(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, storageConfigPrefix(testClassName, ""));
        overridingProps.setProperty(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, metadataConfigPrefix(testClassName, ""));

        overridingProps.setProperty(
                metadataConfigPrefix(testClassName, TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP),
                String.valueOf(numRemoteLogMetadataPartitions));
        overridingProps.setProperty(
                metadataConfigPrefix(testClassName, TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP),
                String.valueOf(brokerCount));
        // This configuration ensures inactive log segments are deleted fast enough so that
        // the integration tests can confirm a given log segment is present only in the second-tier storage.
        // Note that this does not impact the eligibility of a log segment to be offloaded to the
        // second-tier storage.
        overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp(), LOG_CLEANUP_INTERVAL_MS.toString());
        // The directory of the second-tier storage needs to be constant across all instances of storage managers
        // in every broker and throughout the test. Indeed, as brokers are restarted during the test.
        // You can override this property with a fixed path of your choice if you wish to use a non-temporary
        // directory to access its content after a test terminated.
        overridingProps.setProperty(storageConfigPrefix(testClassName, STORAGE_DIR_CONFIG), storageDirPath);
        // This configuration will remove all the remote files when close is called in remote storage manager.
        // Storage manager close is being called while the server is actively processing the socket requests,
        // so enabling this config can break the existing tests.
        // NOTE: When using TestUtils#tempDir(), the folder gets deleted when VM terminates.
        overridingProps.setProperty(storageConfigPrefix(testClassName, DELETE_ON_CLOSE_CONFIG), "false");
        // Set a small number of retry interval for retrying RemoteLogMetadataManager resources initialization to speed up the test
        overridingProps.setProperty(metadataConfigPrefix(testClassName, REMOTE_LOG_METADATA_INITIALIZATION_RETRY_INTERVAL_MS_PROP), RLMM_INIT_RETRY_INTERVAL_MS.toString());
        return overridingProps;
    }

    public static Map<String, String> createTopicConfigForRemoteStorage(boolean enableRemoteStorage,
            int maxRecordBatchPerSegment) {
        Map<String, String> topicProps = new HashMap<>();
        // Enables remote log storage for this topic.
        topicProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, String.valueOf(enableRemoteStorage));
        // Ensure offset and time indexes are generated for every record.
        topicProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, "1");
        // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
        // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
        // time index. Hence, since the topic is configured to generate index entries for every record with, for
        // a "small" number of records (i.e. such that the average record size times the number of records is
        // much less than the segment size), the number of records which hold in a segment is the multiple of 12
        // defined below.
        topicProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, String.valueOf(12 * maxRecordBatchPerSegment));
        // To verify records physically absent from Kafka's storage can be consumed via the second tier storage, we
        // want to delete log segments as soon as possible. When tiered storage is active, an inactive log
        // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
        // should be offloaded before deletion, and their consumption is possible thereafter.
        topicProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1");
        return topicProps;
    }

    private static String storageConfigPrefix(String testClassName, String key) {
        return "rsm.config." + testClassName + "." + key;
    }

    private static String metadataConfigPrefix(String testClassName, String key) {
        return "rlmm.config." + testClassName + "." + key;
    }
}
