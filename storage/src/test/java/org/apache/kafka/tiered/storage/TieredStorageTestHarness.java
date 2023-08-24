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
package org.apache.kafka.tiered.storage;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;
import kafka.api.IntegrationTestHarness;
import kafka.log.remote.RemoteLogManager;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.replica.ReplicaSelector;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP;

import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP;
import static org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorage.DELETE_ON_CLOSE_CONFIG;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorage.STORAGE_DIR_CONFIG;

/**
 * Base class for integration tests exercising the tiered storage functionality in Apache Kafka.
 * This uses a {@link LocalTieredStorage} instance as the second-tier storage system and
 * {@link TopicBasedRemoteLogMetadataManager} as the remote log metadata manager.
 */
@Tag("integration")
public abstract class TieredStorageTestHarness extends IntegrationTestHarness {

    /**
     * InitialTaskDelayMs is set to 30 seconds for the delete-segment scheduler in Apache Kafka.
     * Hence, we need to wait at least that amount of time before segments eligible for deletion
     * gets physically removed.
     */
    private static final Integer STORAGE_WAIT_TIMEOUT_SEC = 35;
    // The default value of log cleanup interval is 30 secs, and it increases the test execution time.
    private static final Integer LOG_CLEANUP_INTERVAL_MS = 500;
    private static final Integer RLM_TASK_INTERVAL_MS = 500;

    protected int numRemoteLogMetadataPartitions = 5;
    private TieredStorageTestContext context;
    private String testClassName = "";

    @SuppressWarnings("deprecation")
    @Override
    public void modifyConfigs(Seq<Properties> props) {
        for (Properties p : JavaConverters.seqAsJavaList(props)) {
            p.putAll(overridingProps());
        }
    }

    public Properties overridingProps() {
        Assertions.assertTrue(STORAGE_WAIT_TIMEOUT_SEC > TimeUnit.MILLISECONDS.toSeconds(RLM_TASK_INTERVAL_MS),
                "STORAGE_WAIT_TIMEOUT_SEC should be greater than RLM_TASK_INTERVAL_MS");

        Properties overridingProps = new Properties();
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

        overridingProps.setProperty(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, storageConfigPrefix(""));
        overridingProps.setProperty(REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, metadataConfigPrefix(""));

        overridingProps.setProperty(
                metadataConfigPrefix(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP),
                String.valueOf(numRemoteLogMetadataPartitions));
        overridingProps.setProperty(
                metadataConfigPrefix(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP),
                String.valueOf(brokerCount()));
        // This configuration ensures inactive log segments are deleted fast enough so that
        // the integration tests can confirm a given log segment is present only in the second-tier storage.
        // Note that this does not impact the eligibility of a log segment to be offloaded to the
        // second-tier storage.
        overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp(), LOG_CLEANUP_INTERVAL_MS.toString());
        // This can be customized to read remote log segments from followers.
        readReplicaSelectorClass()
                .ifPresent(c -> overridingProps.put(KafkaConfig.ReplicaSelectorClassProp(), c.getName()));
        // The directory of the second-tier storage needs to be constant across all instances of storage managers
        // in every broker and throughout the test. Indeed, as brokers are restarted during the test.
        // You can override this property with a fixed path of your choice if you wish to use a non-temporary
        // directory to access its content after a test terminated.
        overridingProps.setProperty(storageConfigPrefix(STORAGE_DIR_CONFIG),
                TestUtils.tempDirectory("kafka-remote-tier-" + testClassName).getAbsolutePath());
        // This configuration will remove all the remote files when close is called in remote storage manager.
        // Storage manager close is being called while the server is actively processing the socket requests,
        // so enabling this config can break the existing tests.
        // NOTE: When using TestUtils#tempDir(), the folder gets deleted when VM terminates.
        overridingProps.setProperty(storageConfigPrefix(DELETE_ON_CLOSE_CONFIG), "false");
        return overridingProps;
    }

    protected Optional<Class<ReplicaSelector>> readReplicaSelectorClass() {
        return Optional.empty();
    }

    protected abstract void writeTestSpecifications(TieredStorageTestBuilder builder);

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) {
        testClassName = testInfo.getTestClass().get().getSimpleName().toLowerCase(Locale.getDefault());
        super.setUp(testInfo);
        context = new TieredStorageTestContext(this);
    }

    @Test
    public void executeTieredStorageTest() {
        TieredStorageTestBuilder builder = new TieredStorageTestBuilder();
        writeTestSpecifications(builder);
        try {
            for (TieredStorageTestAction action : builder.complete()) {
                action.execute(context);
            }
        } catch (Exception ex) {
            throw new AssertionError("Could not build test specifications. No test was executed.", ex);
        }
    }

    @AfterEach
    @Override
    public void tearDown() {
        try {
            Utils.closeQuietly(context, "TieredStorageTestContext");
            super.tearDown();
            context.printReport(System.out);
        } catch (Exception ex) {
            throw new AssertionError("Failed to close the tear down the test harness.", ex);
        }
    }

    private String storageConfigPrefix(String key) {
        return "rsm.config." + testClassName + "." + key;
    }

    private String metadataConfigPrefix(String key) {
        return "rlmm.config." + testClassName + "." + key;
    }

    @SuppressWarnings("deprecation")
    public static List<LocalTieredStorage> remoteStorageManagers(Seq<KafkaBroker> brokers) {
        List<LocalTieredStorage> storages = new ArrayList<>();
        JavaConverters.seqAsJavaList(brokers).forEach(broker -> {
            if (broker.remoteLogManagerOpt().isDefined()) {
                RemoteLogManager remoteLogManager = broker.remoteLogManagerOpt().get();
                RemoteStorageManager storageManager = remoteLogManager.storageManager();
                if (storageManager instanceof ClassLoaderAwareRemoteStorageManager) {
                    ClassLoaderAwareRemoteStorageManager loaderAwareRSM =
                            (ClassLoaderAwareRemoteStorageManager) storageManager;
                    if (loaderAwareRSM.delegate() instanceof LocalTieredStorage) {
                        storages.add((LocalTieredStorage) loaderAwareRSM.delegate());
                    }
                } else if (storageManager instanceof LocalTieredStorage) {
                    storages.add((LocalTieredStorage) storageManager);
                }
            } else {
                throw new AssertionError("Broker " + broker.config().brokerId()
                        + " does not have a remote log manager.");
            }
        });
        return storages;
    }

    @SuppressWarnings("deprecation")
    public static List<BrokerLocalStorage> localStorages(Seq<KafkaBroker> brokers) {
        return JavaConverters.seqAsJavaList(brokers).stream()
                .map(b -> new BrokerLocalStorage(b.config().brokerId(), b.config().logDirs().head(),
                        STORAGE_WAIT_TIMEOUT_SEC))
                .collect(Collectors.toList());
    }
}
