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
package org.apache.kafka.raft;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.server.common.KRaftVersion;

import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class KafkaRaftClientFetchTest {
    // Since the log is empty the valid offset is 0. Use an invalid offset to show that records are not getting decoded
    private static final long BASE_OFFSET = 1234;
    private static final int EPOCH = 4321;

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
    void testRandomRecords(
        @ForAll int seed
    ) throws Exception {
        testFetchResponseWithInvalidRecord(buildRandomRecords(new Random(seed)));
    }

    @Test
    void testNotEnoughBytes() throws Exception {
        testFetchResponseWithInvalidRecord(
            MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[Records.LOG_OVERHEAD - 1]))
        );
    }

    @Test
    void testRecordsSizeTooSmall() throws Exception {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(LegacyRecord.RECORD_OVERHEAD_V0 - 1);
        buffer.position(0);
        buffer.limit(buffer.capacity());

        testFetchResponseWithInvalidRecord(MemoryRecords.readableRecords(buffer));
    }

    @Test
    void testNotEnoughBytesToMagic() throws Exception {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        buffer.position(0);
        buffer.limit(Records.HEADER_SIZE_UP_TO_MAGIC - 1);

        testFetchResponseWithInvalidRecord(MemoryRecords.readableRecords(buffer));
    }

    @Test
    void testNegativedMagic() throws Exception {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        // Write the epoch
        buffer.putInt(EPOCH);
        // Write magic
        buffer.put((byte) -1);
        buffer.position(0);
        buffer.limit(buffer.capacity());

        testFetchResponseWithInvalidRecord(MemoryRecords.readableRecords(buffer));
    }

    @Test
    void testLargedMagic() throws Exception {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        // Write the epoch
        buffer.putInt(EPOCH);
        // Write magic
        buffer.put((byte) (RecordBatch.CURRENT_MAGIC_VALUE + 1));
        buffer.position(0);
        buffer.limit(buffer.capacity());

        testFetchResponseWithInvalidRecord(MemoryRecords.readableRecords(buffer));
    }

    @Test
    void testLessBytesThanRecordSize() throws Exception {
        var buffer = ByteBuffer.allocate(256);
        // Write the base offset
        buffer.putLong(BASE_OFFSET);
        // Write record size
        buffer.putInt(buffer.capacity() - Records.LOG_OVERHEAD);
        // Write the epoch
        buffer.putInt(EPOCH);
        // Write magic
        buffer.put(RecordBatch.CURRENT_MAGIC_VALUE);
        buffer.position(0);
        buffer.limit(buffer.capacity() - Records.LOG_OVERHEAD - 1);

        testFetchResponseWithInvalidRecord(MemoryRecords.readableRecords(buffer));
    }

    @Test
    private static void testFetchResponseWithInvalidRecord(MemoryRecords records) throws Exception {
        int localId = KafkaRaftClientTest.randomReplicaId();
        ReplicaKey local = KafkaRaftClientTest.replicaKey(localId, true);
        ReplicaKey electedLeader = KafkaRaftClientTest.replicaKey(localId + 1, true);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, electedLeader)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, electedLeader.id())
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_996_PROTOCOL)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        long oldLogEndOffset = context.log.endOffset().offset();

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, electedLeader.id(), records, 0L, Errors.NONE)
        );

        context.client.poll();

        assertEquals(oldLogEndOffset, context.log.endOffset().offset());
    }

    private static MemoryRecords buildRandomRecords(Random random) {
        int size = random.nextInt(255) + 1;
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);

        return MemoryRecords.readableRecords(ByteBuffer.wrap(bytes));
    }
}
