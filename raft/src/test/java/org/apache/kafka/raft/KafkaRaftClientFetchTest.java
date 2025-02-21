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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ArbitraryMemoryRecords;
import org.apache.kafka.common.record.InvalidMemoryRecordsProvider;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.server.common.KRaftVersion;

import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class KafkaRaftClientFetchTest {
    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
    void testRandomRecords(
        @ForAll(supplier = ArbitraryMemoryRecords.class) MemoryRecords memoryRecords
    ) throws Exception {
        testFetchResponseWithInvalidRecord(memoryRecords, Integer.MAX_VALUE);
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidMemoryRecordsProvider.class)
    void testInvalidMemoryRecords(MemoryRecords records, Optional<Class<Exception>> expectedException) throws Exception {
        // CorruptRecordException are handled by the KafkaRaftClient so ignore the expected exception
        testFetchResponseWithInvalidRecord(records, Integer.MAX_VALUE);
    }

    private static void testFetchResponseWithInvalidRecord(MemoryRecords records, int epoch) throws Exception {
        int localId = KafkaRaftClientTest.randomReplicaId();
        ReplicaKey local = KafkaRaftClientTest.replicaKey(localId, true);
        ReplicaKey electedLeader = KafkaRaftClientTest.replicaKey(localId + 1, true);

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

    @Test
    void testReplicationOfHigherPartitionLeaderEpoch() throws Exception {
        int epoch = 2;
        int localId = KafkaRaftClientTest.randomReplicaId();
        ReplicaKey local = KafkaRaftClientTest.replicaKey(localId, true);
        ReplicaKey electedLeader = KafkaRaftClientTest.replicaKey(localId + 1, true);

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
        int numberOfRecords = 10;
        MemoryRecords batchWithValidEpoch = MemoryRecords.withRecords(
            oldLogEndOffset,
            Compression.NONE,
            epoch,
            IntStream
                .range(0, numberOfRecords)
                .mapToObj(number -> new SimpleRecord(Integer.toString(number).getBytes()))
                .toArray(SimpleRecord[]::new)
        );

        MemoryRecords batchWithInvalidEpoch = MemoryRecords.withRecords(
            oldLogEndOffset + numberOfRecords,
            Compression.NONE,
            epoch + 1,
            IntStream
                .range(0, numberOfRecords)
                .mapToObj(number -> new SimpleRecord(Integer.toString(number).getBytes()))
                .toArray(SimpleRecord[]::new)
        );

        var buffer = ByteBuffer.allocate(batchWithValidEpoch.sizeInBytes() + batchWithInvalidEpoch.sizeInBytes());
        buffer.put(batchWithValidEpoch.buffer());
        buffer.put(batchWithInvalidEpoch.buffer());
        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, electedLeader.id(), records, 0L, Errors.NONE)
        );

        context.client.poll();

        // Check that only the first batch was appended because the second batch has a greater epoch
        assertEquals(oldLogEndOffset + numberOfRecords, context.log.endOffset().offset());
    }
}
