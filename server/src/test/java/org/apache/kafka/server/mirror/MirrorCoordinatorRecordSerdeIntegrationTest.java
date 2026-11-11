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

package org.apache.kafka.server.mirror;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.mirror.MirrorCoordinatorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsValue;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.test.TestUtils;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MirrorCoordinatorRecordSerdeIntegrationTest {
    private static final TopicPartition MIRROR_STATE = new TopicPartition("__mirror_state", 0);
    private static final Uuid TOPIC_ID = new Uuid(1L, 2L);

    @ClusterTest(types = Type.KRAFT, brokers = 1, controllers = 1)
    void testRecordsSurviveBrokerRestart(ClusterInstance cluster) throws Exception {
        List<CoordinatorRecord> records = List.of(
            stateRecord(MirrorPartitionState.MIRRORING, null),
            epochRecord(10, 100L),
            stateRecord(MirrorPartitionState.FAILED, "Source is unavailable"),
            epochRecord(11, 4_294_967_313L)
        );

        assertRecordsSurviveRestart(cluster, records);
    }

    @ClusterTest(types = Type.KRAFT, brokers = 1, controllers = 1)
    void testTombstonesSurviveBrokerRestart(ClusterInstance cluster) throws Exception {
        CoordinatorRecord state = stateRecord(MirrorPartitionState.STOPPED, null);
        CoordinatorRecord epochs = epochRecord(10, 100L);
        List<CoordinatorRecord> records = List.of(
            state,
            epochs,
            CoordinatorRecord.tombstone(state.key()),
            CoordinatorRecord.tombstone(epochs.key())
        );

        assertRecordsSurviveRestart(cluster, records);
    }

    private void assertRecordsSurviveRestart(ClusterInstance cluster, List<CoordinatorRecord> expected) throws Exception {
        cluster.createTopic(MIRROR_STATE.topic(), 1, (short) 1,
            Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
        MirrorCoordinatorRecordSerde writer = new MirrorCoordinatorRecordSerde();
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (CoordinatorRecord record : expected) {
                producer.send(new ProducerRecord<>(
                    MIRROR_STATE.topic(),
                    MIRROR_STATE.partition(),
                    writer.serializeKey(record),
                    writer.serializeValue(record)
                )).get(30, TimeUnit.SECONDS);
            }
        }

        cluster.restartBroker(cluster.brokerIds().iterator().next(), Map.of());
        cluster.waitForReadyBrokers();

        MirrorCoordinatorRecordSerde reader = new MirrorCoordinatorRecordSerde();
        List<CoordinatorRecord> actual = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(
            Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        )) {
            consumer.assign(List.of(MIRROR_STATE));
            consumer.seekToBeginning(List.of(MIRROR_STATE));
            TestUtils.waitForCondition(() -> {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(100))) {
                    actual.add(reader.deserialize(
                        ByteBuffer.wrap(record.key()),
                        record.value() == null ? null : ByteBuffer.wrap(record.value())
                    ));
                }
                return actual.size() >= expected.size();
            }, "Failed to read mirror state records after broker restart");
        }
        assertEquals(expected, actual);
    }

    private static CoordinatorRecord stateRecord(MirrorPartitionState state, String errorMessage) {
        return CoordinatorRecord.record(
            new MirrorPartitionStateKey().setMirrorName("source-to-destination").setTopicId(TOPIC_ID).setPartition(0),
            new ApiMessageAndVersion(
                new MirrorPartitionStateValue()
                    .setState(state.value())
                    .setLeaderEpoch(11)
                    .setStateEpoch(2)
                    .setErrorMessage(errorMessage)
                    .setRetryAttempt((short) 1)
                    .setPreviousState(MirrorPartitionState.MIRRORING.value()),
                (short) 0
            )
        );
    }

    private static CoordinatorRecord epochRecord(int epoch, long offset) {
        return CoordinatorRecord.record(
            new LastMirrorEpochsKey().setMirrorName("source-to-destination").setTopicId(TOPIC_ID).setPartition(0),
            new ApiMessageAndVersion(
                new LastMirrorEpochsValue().setLastMirrorEpoch(epoch).setLastMirrorOffset(offset),
                (short) 0
            )
        );
    }
}
