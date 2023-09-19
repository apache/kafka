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
package org.apache.kafka.tiered.storage.integration;

import kafka.api.TransactionsTest;
import kafka.server.HostedPartition;
import kafka.server.KafkaBroker;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createPropsForRemoteStorage;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createTopicConfigForRemoteStorage;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.STORAGE_WAIT_TIMEOUT_SEC;

public class TransactionsWithTieredStoreTest extends TransactionsTest {

    private String testClassName;
    private String storageDirPath;

    @BeforeEach
    @Override
    public void setUp(TestInfo testInfo) {
        testClassName = testInfo.getTestClass().get().getSimpleName().toLowerCase(Locale.getDefault());
        storageDirPath = TestUtils.tempDirectory("kafka-remote-tier-" + testClassName).getAbsolutePath();
        super.setUp(testInfo);
    }

    @Override
    public Properties overridingProps() {
        Properties props = super.overridingProps();
        int numRemoteLogMetadataPartitions = 3;
        return createPropsForRemoteStorage(testClassName, storageDirPath, brokerCount(),
                numRemoteLogMetadataPartitions, props);
    }

    @Override
    public Properties topicConfig() {
        boolean enableRemoteStorage = true;
        int maxBatchCountPerSegment = 1;
        Properties overridingTopicProps = super.topicConfig();
        overridingTopicProps.putAll(createTopicConfigForRemoteStorage(
                enableRemoteStorage, maxBatchCountPerSegment));
        return overridingTopicProps;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void maybeWaitForAtLeastOneSegmentUpload(scala.collection.Seq<TopicPartition> topicPartitions) {
        JavaConverters.seqAsJavaList(topicPartitions).forEach(topicPartition -> {
            List<BrokerLocalStorage> localStorages = JavaConverters.bufferAsJavaList(brokers()).stream()
                    .map(b -> new BrokerLocalStorage(b.config().brokerId(), b.config().logDirs().head(), STORAGE_WAIT_TIMEOUT_SEC))
                    .collect(Collectors.toList());
            localStorages
                    .stream()
                    // Select brokers which are assigned a replica of the topic-partition
                    .filter(s -> isAssignedReplica(topicPartition, s.getBrokerId()))
                    // Filter out inactive brokers, which may still contain log segments we would expect
                    // to be deleted based on the retention configuration.
                    .filter(s -> isAlive(s.getBrokerId()))
                    .forEach(localStorage ->
                            // Wait until the brokers local storage have been cleared from the inactive log segments.
                            localStorage.waitForAtLeastEarliestLocalOffset(topicPartition, 1L));
        });
    }

    @SuppressWarnings("deprecation")
    @Override
    public void maybeVerifyLocalLogStartOffsets(scala.collection.immutable.Map<TopicPartition, Long> partitionLocalStartOffsets) throws InterruptedException {
        Map<Integer, Long> offsets = new HashMap<>();
        TestUtils.waitForCondition(() ->
                JavaConverters.seqAsJavaList(brokers()).stream().allMatch(broker ->
                        JavaConverters.mapAsJavaMapConverter(partitionLocalStartOffsets).asJava()
                                .entrySet().stream().allMatch(entry -> {
                                    long offset = broker.replicaManager().localLog(entry.getKey()).get().localLogStartOffset();
                                    offsets.put(broker.config().brokerId(), offset);
                                    return entry.getValue() == offset;
                                })
                ), () -> "local log start offset doesn't change to the expected position:" + partitionLocalStartOffsets + ", current position:" + offsets);
    }

    @SuppressWarnings("deprecation")
    private boolean isAssignedReplica(TopicPartition topicPartition,
                                      Integer replicaId) {
        Optional<KafkaBroker> brokerOpt = JavaConverters.seqAsJavaList(brokers())
                .stream()
                .filter(b -> b.config().brokerId() == replicaId).findFirst();
        boolean isAssigned = false;
        if (brokerOpt.isPresent()) {
            HostedPartition hostedPartition = brokerOpt.get().replicaManager().getPartition(topicPartition);
            if (hostedPartition instanceof HostedPartition.Online) {
                isAssigned = true;
            }
        }
        return isAssigned;
    }

    private boolean isAlive(Integer brokerId) {
        return aliveBrokers().exists(b -> b.config().brokerId() == brokerId);
    }
}
