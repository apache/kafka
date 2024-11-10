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

import kafka.api.IntegrationTestHarness;
import kafka.log.remote.RemoteLogManager;
import kafka.server.KafkaBroker;

import org.apache.kafka.common.replica.ReplicaSelector;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.STORAGE_WAIT_TIMEOUT_SEC;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createPropsForRemoteStorage;
/**
 * Base class for integration tests exercising the tiered storage functionality in Apache Kafka.
 * This uses a {@link LocalTieredStorage} instance as the second-tier storage system and
 * {@link TopicBasedRemoteLogMetadataManager} as the remote log metadata manager.
 */
@Tag("integration")
public abstract class TieredStorageTestHarness extends IntegrationTestHarness {

    private TieredStorageTestContext context;
    private String testClassName = "";
    private String storageDirPath = "";

    @Override
    public void modifyConfigs(Seq<Properties> props) {
        for (Properties p : CollectionConverters.asJava(props)) {
            p.putAll(overridingProps());
        }
    }

    @Override
    public Seq<Properties> kraftControllerConfigs(TestInfo testInfo) {
        return CollectionConverters.asScala(Collections.singletonList(overridingProps())).toSeq();
    }

    protected int numRemoteLogMetadataPartitions() {
        return 5;
    }

    public Properties overridingProps() {
        Properties overridingProps = createPropsForRemoteStorage(testClassName, storageDirPath, brokerCount(),
                numRemoteLogMetadataPartitions(), new Properties());
        readReplicaSelectorClass()
                .ifPresent(c -> overridingProps.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, c.getName()));
        return overridingProps;
    }

    protected Optional<Class<ReplicaSelector>> readReplicaSelectorClass() {
        return Optional.empty();
    }

    protected abstract void writeTestSpecifications(TieredStorageTestBuilder builder);

    protected void overrideConsumerConfig(Properties consumerConfig) {
    }

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) {
        testClassName = testInfo.getTestClass().get().getSimpleName().toLowerCase(Locale.getDefault());
        storageDirPath = TestUtils.tempDirectory("kafka-remote-tier-" + testClassName).getAbsolutePath();
        super.setUp(testInfo);
        overrideConsumerConfig(consumerConfig());
        context = new TieredStorageTestContext(this);
    }

    // NOTE: Not able to refer TestInfoUtils#TestWithParameterizedQuorumName() in the ParameterizedTest name.
    @ParameterizedTest(name = "{displayName}.quorum={0}.groupProtocol={1}")
    @MethodSource("getTestQuorumAndGroupProtocolParametersAll")
    public void executeTieredStorageTest(String quorum, String groupProtocol) {
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

    public static List<LocalTieredStorage> remoteStorageManagers(Seq<KafkaBroker> brokers) {
        List<LocalTieredStorage> storages = new ArrayList<>();
        CollectionConverters.asJava(brokers).forEach(broker -> {
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

    public static List<BrokerLocalStorage> localStorages(Seq<KafkaBroker> brokers) {
        return CollectionConverters.asJava(brokers).stream()
                .map(b -> new BrokerLocalStorage(b.config().brokerId(), CollectionConverters.asJava(b.config().logDirs().toSet()),
                        STORAGE_WAIT_TIMEOUT_SEC))
                .collect(Collectors.toList());
    }
}
