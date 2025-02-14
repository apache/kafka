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
package org.apache.kafka.tools.consumer.group.share;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.coordinator.share.ShareGroupOffset;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.share.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.share.generated.ShareUpdateValue;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.PersisterStateBatch;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShareGroupStateMessageFormatterTest {
    private static final SharePartitionKey KEY_1 = SharePartitionKey.getInstance("gs1", Uuid.fromString("gtb2stGYRk-vWZ2zAozmoA"), 0);
    private static final ShareGroupOffset SHARE_GROUP_OFFSET_1 = new ShareGroupOffset.Builder()
        .setSnapshotEpoch(0)
        .setStateEpoch(1)
        .setLeaderEpoch(20)
        .setStartOffset(50)
        .setStateBatches(
            Arrays.asList(
                new PersisterStateBatch(
                    100,
                    200,
                    (byte) 1,
                    (short) 10
                ),
                new PersisterStateBatch(
                    201,
                    210,
                    (byte) 2,
                    (short) 10
                )
            )
        ).build();

    private static final SharePartitionKey KEY_2 = SharePartitionKey.getInstance("gs2", Uuid.fromString("r9Nq4xGAQf28jvu36t7gQQ"), 0);
    private static final ShareGroupOffset SHARE_GROUP_OFFSET_2 = new ShareGroupOffset.Builder()
        .setSnapshotEpoch(1)
        .setStateEpoch(3)
        .setLeaderEpoch(25)
        .setStartOffset(55)
        .setStateBatches(
            Arrays.asList(
                new PersisterStateBatch(
                    100,
                    150,
                    (byte) 1,
                    (short) 12
                ),
                new PersisterStateBatch(
                    151,
                    200,
                    (byte) 2,
                    (short) 15
                )
            )
        ).build();

    private static final ShareSnapshotKey SHARE_SNAPSHOT_KEY = new ShareSnapshotKey()
        .setGroupId(KEY_1.groupId())
        .setTopicId(KEY_1.topicId())
        .setPartition(KEY_1.partition());

    private static final ShareSnapshotValue SHARE_SNAPSHOT_VALUE = new ShareSnapshotValue()
        .setSnapshotEpoch(SHARE_GROUP_OFFSET_1.snapshotEpoch())
        .setStateEpoch(SHARE_GROUP_OFFSET_1.stateEpoch())
        .setLeaderEpoch(SHARE_GROUP_OFFSET_1.leaderEpoch())
        .setStartOffset(SHARE_GROUP_OFFSET_1.startOffset())
        .setStateBatches(
            SHARE_GROUP_OFFSET_1.stateBatches().stream()
                .map(batch -> new ShareSnapshotValue.StateBatch()
                    .setFirstOffset(batch.firstOffset())
                    .setLastOffset(batch.lastOffset())
                    .setDeliveryState(batch.deliveryState())
                    .setDeliveryCount(batch.deliveryCount()))
                .toList()
        );

    private static final ShareUpdateKey SHARE_UPDATE_KEY = new ShareUpdateKey()
        .setGroupId(KEY_2.groupId())
        .setTopicId(KEY_2.topicId())
        .setPartition(KEY_2.partition());

    private static final ShareUpdateValue SHARE_UPDATE_VALUE = new ShareUpdateValue()
        .setSnapshotEpoch(SHARE_GROUP_OFFSET_2.snapshotEpoch())
        .setLeaderEpoch(SHARE_GROUP_OFFSET_2.leaderEpoch())
        .setStartOffset(SHARE_GROUP_OFFSET_2.startOffset())
        .setStateBatches(
            SHARE_GROUP_OFFSET_2.stateBatches().stream()
                .map(batch -> new ShareUpdateValue.StateBatch()
                    .setFirstOffset(batch.firstOffset())
                    .setLastOffset(batch.lastOffset())
                    .setDeliveryState(batch.deliveryState())
                    .setDeliveryCount(batch.deliveryCount()))
                .toList()
        );

    private static Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_SNAPSHOT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_SNAPSHOT_VALUE).array(),
                "{\"key\":{\"version\":0,\"data\":{\"groupId\":\"gs1\",\"topicId\":\"gtb2stGYRk-vWZ2zAozmoA\",\"partition\":0}},\"value\":{\"version\":0,\"data\":{\"snapshotEpoch\":0,\"stateEpoch\":1,\"leaderEpoch\":20,\"startOffset\":50,\"stateBatches\":[{\"firstOffset\":100,\"lastOffset\":200,\"deliveryState\":1,\"deliveryCount\":10},{\"firstOffset\":201,\"lastOffset\":210,\"deliveryState\":2,\"deliveryCount\":10}]}}}"
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, SHARE_UPDATE_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_UPDATE_VALUE).array(),
                "{\"key\":{\"version\":1,\"data\":{\"groupId\":\"gs2\",\"topicId\":\"r9Nq4xGAQf28jvu36t7gQQ\",\"partition\":0}},\"value\":{\"version\":0,\"data\":{\"snapshotEpoch\":1,\"leaderEpoch\":25,\"startOffset\":55,\"stateBatches\":[{\"firstOffset\":100,\"lastOffset\":150,\"deliveryState\":1,\"deliveryCount\":12},{\"firstOffset\":151,\"lastOffset\":200,\"deliveryState\":2,\"deliveryCount\":15}]}}}"
            ),
            // wrong versions
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, SHARE_SNAPSHOT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_SNAPSHOT_VALUE).array(),
                "{\"key\":{\"version\":10,\"data\":\"unknown\"},\"value\":{\"version\":0,\"data\":\"unknown\"}}"
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 15, SHARE_UPDATE_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_UPDATE_VALUE).array(),
                "{\"key\":{\"version\":15,\"data\":\"unknown\"},\"value\":{\"version\":0,\"data\":\"unknown\"}}"
            )
        );
    }

    private static Stream<Arguments> exceptions() {
        return Stream.of(
            // wrong types
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_SNAPSHOT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_UPDATE_VALUE).array(),
                new RuntimeException("non-nullable field stateBatches was serialized as null")
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, SHARE_UPDATE_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_SNAPSHOT_VALUE).array(),
                new RuntimeException("non-nullable field stateBatches was serialized as null")
            )
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testShareGroupStateMessageFormatter(
        byte[] keyBuffer,
        byte[] valueBuffer,
        String expectedOutput
    ) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, 0,
            0L, TimestampType.CREATE_TIME, 0,
            0, keyBuffer, valueBuffer,
            new RecordHeaders(), Optional.empty());

        try (MessageFormatter formatter = new ShareGroupStateMessageFormatter()) {
            formatter.configure(emptyMap());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            formatter.writeTo(record, new PrintStream(out));
            assertEquals(expectedOutput, out.toString());
        }
    }

    @ParameterizedTest
    @MethodSource("exceptions")
    public void testShareGroupStateMessageFormatterException(
        byte[] keyBuffer,
        byte[] valueBuffer,
        RuntimeException expectedOutput
    ) {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
            Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0, 0,
            0L, TimestampType.CREATE_TIME, 0,
            0, keyBuffer, valueBuffer,
            new RecordHeaders(), Optional.empty());

        try (MessageFormatter formatter = new ShareGroupStateMessageFormatter()) {
            formatter.configure(emptyMap());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            RuntimeException re = assertThrows(RuntimeException.class, () -> formatter.writeTo(record, new PrintStream(out)));
            assertEquals(expectedOutput.getMessage(), re.getMessage());
        }
    }
}
