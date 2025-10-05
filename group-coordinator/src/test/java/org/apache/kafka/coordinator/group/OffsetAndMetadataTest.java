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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OffsetAndMetadataTest {
    @Test
    public void testAttributes() {
        Uuid topicId = Uuid.randomUuid();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
            100L,
            OptionalInt.of(10),
            "metadata",
            1234L,
            OptionalLong.of(5678L),
            topicId
        );

        assertEquals(100L, offsetAndMetadata.committedOffset);
        assertEquals(OptionalInt.of(10), offsetAndMetadata.leaderEpoch);
        assertEquals("metadata", offsetAndMetadata.metadata);
        assertEquals(1234L, offsetAndMetadata.commitTimestampMs);
        assertEquals(OptionalLong.of(5678L), offsetAndMetadata.expireTimestampMs);
        assertEquals(topicId, offsetAndMetadata.topicId);
    }

    private static Stream<Uuid> uuids() {
        return Stream.of(
            Uuid.ZERO_UUID,
            Uuid.randomUuid()
        );
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testFromRecord(Uuid uuid) {
        OffsetCommitValue record = new OffsetCommitValue()
            .setOffset(100L)
            .setLeaderEpoch(-1)
            .setMetadata("metadata")
            .setCommitTimestamp(1234L)
            .setExpireTimestamp(-1L)
            .setTopicId(uuid);

        assertEquals(new OffsetAndMetadata(
            10L,
            100L,
            OptionalInt.empty(),
            "metadata",
            1234L,
            OptionalLong.empty(),
            uuid
        ), OffsetAndMetadata.fromRecord(10L, record));

        record
            .setLeaderEpoch(12)
            .setExpireTimestamp(5678L);

        assertEquals(new OffsetAndMetadata(
            11L,
            100L,
            OptionalInt.of(12),
            "metadata",
            1234L,
            OptionalLong.of(5678L),
            uuid
        ), OffsetAndMetadata.fromRecord(11L, record));
    }

    @ParameterizedTest
    @MethodSource("uuids")
    public void testFromRequest(Uuid uuid) {
        MockTime time = new MockTime();

        OffsetCommitRequestData.OffsetCommitRequestPartition partition =
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100L)
                .setCommittedLeaderEpoch(-1)
                .setCommittedMetadata(null);

        assertEquals(
            new OffsetAndMetadata(
                100L,
                OptionalInt.empty(),
                "",
                time.milliseconds(),
                OptionalLong.empty(),
                uuid
            ), OffsetAndMetadata.fromRequest(
                uuid,
                partition,
                time.milliseconds(),
                OptionalLong.empty()
            )
        );

        partition
            .setCommittedLeaderEpoch(10)
            .setCommittedMetadata("hello");

        assertEquals(
            new OffsetAndMetadata(
                100L,
                OptionalInt.of(10),
                "hello",
                time.milliseconds(),
                OptionalLong.empty(),
                uuid
            ), OffsetAndMetadata.fromRequest(
                uuid,
                partition,
                time.milliseconds(),
                OptionalLong.empty()
            )
        );

        assertEquals(
            new OffsetAndMetadata(
                100L,
                OptionalInt.of(10),
                "hello",
                time.milliseconds(),
                OptionalLong.of(5678L),
                uuid
            ), OffsetAndMetadata.fromRequest(
                uuid,
                partition,
                time.milliseconds(),
                OptionalLong.of(5678L)
            )
        );
    }

    @Test
    public void testFromTransactionalRequest() {
        MockTime time = new MockTime();

        TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition partition =
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100L)
                .setCommittedLeaderEpoch(-1)
                .setCommittedMetadata(null);

        assertEquals(
            new OffsetAndMetadata(
                100L,
                OptionalInt.empty(),
                "",
                time.milliseconds(),
                OptionalLong.empty(),
                Uuid.ZERO_UUID
            ), OffsetAndMetadata.fromRequest(
                partition,
                time.milliseconds()
            )
        );

        partition
            .setCommittedLeaderEpoch(10)
            .setCommittedMetadata("hello");

        assertEquals(
            new OffsetAndMetadata(
                100L,
                OptionalInt.of(10),
                "hello",
                time.milliseconds(),
                OptionalLong.empty(),
                Uuid.ZERO_UUID
            ), OffsetAndMetadata.fromRequest(
                partition,
                time.milliseconds()
            )
        );
    }
}
