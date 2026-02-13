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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.ArbitraryMemoryRecords;
import org.apache.kafka.common.record.internal.InvalidMemoryRecordsProvider;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.server.common.KRaftVersion;

import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
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
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, OptionalLong.empty());

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
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, OptionalLong.empty());

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

    @Test
    void testHighWatermarkSentInFetchRequest() throws Exception {
        int epoch = 2;
        int localId = KafkaRaftClientTest.randomReplicaId();
        ReplicaKey local = KafkaRaftClientTest.replicaKey(localId, true);
        ReplicaKey electedLeader = KafkaRaftClientTest.replicaKey(localId + 1, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .appendToLog(epoch, List.of("a", "b", "c"))
            .appendToLog(epoch, List.of("d", "e", "f"))
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, electedLeader)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, electedLeader.id())
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1166_PROTOCOL)
            .build();

        var localLogEndOffset = context.log.endOffset().offset();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(
            fetchRequest,
            epoch,
            localLogEndOffset,
            epoch,
            OptionalLong.empty()
        );

        // Set the HWM to the LEO
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(
                epoch,
                electedLeader.id(),
                MemoryRecords.EMPTY,
                localLogEndOffset,
                Errors.NONE
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(
            fetchRequest,
            epoch,
            localLogEndOffset,
            epoch,
            OptionalLong.of(localLogEndOffset)
        );
    }

    @Test
    void testDefaultHwmDeferred() throws Exception {
        var epoch = 2;
        var local = KafkaRaftClientTest.replicaKey(
            KafkaRaftClientTest.randomReplicaId(),
            true
        );
        var voter = KafkaRaftClientTest.replicaKey(local.id() + 1, true);
        var remote = KafkaRaftClientTest.replicaKey(local.id() + 2, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .appendToLog(epoch, List.of("a", "b", "c"))
            .appendToLog(epoch, List.of("d", "e", "f"))
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, voter)), KRaftVersion.KRAFT_VERSION_1
            )
            .withUnknownLeader(epoch)
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1166_PROTOCOL)
            .build();

        context.unattachedToLeader();
        epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        var localLogEndOffset = context.log.endOffset().offset();
        var lastFetchedEpoch = context.log.lastFetchedEpoch();
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                remote,
                localLogEndOffset,
                lastFetchedEpoch,
                Integer.MAX_VALUE
            )
        );

        // Check that the fetch response was deferred
        for (var i = 0; i < 10; ++i) {
            context.client.poll();
            assertEquals(List.of(), context.drainSentResponses(ApiKeys.FETCH));
        }
    }

    @Test
    void testUnknownHwmDeferredWhenLeaderDoesNotKnowHwm() throws Exception {
        var epoch = 2;
        var local = KafkaRaftClientTest.replicaKey(
            KafkaRaftClientTest.randomReplicaId(),
            true
        );
        var voter = KafkaRaftClientTest.replicaKey(local.id() + 1, true);
        var remote = KafkaRaftClientTest.replicaKey(local.id() + 2, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .appendToLog(epoch, List.of("a", "b", "c"))
            .appendToLog(epoch, List.of("d", "e", "f"))
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, voter)), KRaftVersion.KRAFT_VERSION_1
            )
            .withUnknownLeader(epoch)
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1166_PROTOCOL)
            .build();

        context.unattachedToLeader();
        epoch = context.currentEpoch();

        var localLogEndOffset = context.log.endOffset().offset();
        var lastFetchedEpoch = context.log.lastFetchedEpoch();
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                remote,
                localLogEndOffset,
                lastFetchedEpoch,
                OptionalLong.empty(),
                Integer.MAX_VALUE
            )
        );

        // Check that the fetch response was deferred
        for (var i = 0; i < 10; ++i) {
            context.client.poll();
            assertEquals(List.of(), context.drainSentResponses(ApiKeys.FETCH));
        }
    }

    @Test
    void testOutdatedHwmCompletedWhenLeaderKnowsHwm() throws Exception {
        var epoch = 2;
        var local = KafkaRaftClientTest.replicaKey(
            KafkaRaftClientTest.randomReplicaId(),
            true
        );
        var voter = KafkaRaftClientTest.replicaKey(local.id() + 1, true);
        var remote = KafkaRaftClientTest.replicaKey(local.id() + 2, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .appendToLog(epoch, List.of("a", "b", "c"))
            .appendToLog(epoch, List.of("d", "e", "f"))
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, voter)), KRaftVersion.KRAFT_VERSION_1
            )
            .withUnknownLeader(epoch)
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1166_PROTOCOL)
            .build();

        context.unattachedToLeader();
        epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        var localLogEndOffset = context.log.endOffset().offset();
        var lastFetchedEpoch = context.log.lastFetchedEpoch();

        // FETCH response completed when remote replica doesn't know HWM
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                remote,
                localLogEndOffset,
                lastFetchedEpoch,
                OptionalLong.empty(),
                Integer.MAX_VALUE
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(localLogEndOffset, epoch);

        // FETCH response completed when remote replica has outdated HWM
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                remote,
                localLogEndOffset,
                lastFetchedEpoch,
                OptionalLong.of(localLogEndOffset - 1),
                Integer.MAX_VALUE
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(localLogEndOffset, epoch);
    }

    @Test
    void testUnchangedHighWatermarkDeferred() throws Exception {
        var epoch = 2;
        var local = KafkaRaftClientTest.replicaKey(
            KafkaRaftClientTest.randomReplicaId(),
            true
        );
        var voter = KafkaRaftClientTest.replicaKey(local.id() + 1, true);
        var remote = KafkaRaftClientTest.replicaKey(local.id() + 2, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .appendToLog(epoch, List.of("a", "b", "c"))
            .appendToLog(epoch, List.of("d", "e", "f"))
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, voter)), KRaftVersion.KRAFT_VERSION_1
            )
            .withUnknownLeader(epoch)
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1166_PROTOCOL)
            .build();

        context.unattachedToLeader();
        epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        var localLogEndOffset = context.log.endOffset().offset();
        var lastFetchedEpoch = context.log.lastFetchedEpoch();
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                remote,
                localLogEndOffset,
                lastFetchedEpoch,
                OptionalLong.of(localLogEndOffset),
                Integer.MAX_VALUE
            )
        );

        // Check that the fetch response was deferred
        for (var i = 0; i < 10; ++i) {
            context.client.poll();
            assertEquals(List.of(), context.drainSentResponses(ApiKeys.FETCH));
        }
    }

    @Test
    void testUpdatedHighWatermarkCompleted() throws Exception {
        var epoch = 2;
        var local = KafkaRaftClientTest.replicaKey(
            KafkaRaftClientTest.randomReplicaId(),
            true
        );
        var voter = KafkaRaftClientTest.replicaKey(local.id() + 1, true);
        var remote = KafkaRaftClientTest.replicaKey(local.id() + 2, true);

        RaftClientTestContext context = new RaftClientTestContext.Builder(
            local.id(),
            local.directoryId().get()
        )
            .appendToLog(epoch, List.of("a", "b", "c"))
            .appendToLog(epoch, List.of("d", "e", "f"))
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(local, voter)), KRaftVersion.KRAFT_VERSION_1
            )
            .withUnknownLeader(epoch)
            .withRaftProtocol(RaftClientTestContext.RaftProtocol.KIP_1166_PROTOCOL)
            .build();

        context.unattachedToLeader();
        epoch = context.currentEpoch();

        // Establish a HWM (3) but don't set it to the LEO
        context.deliverRequest(context.fetchRequest(epoch, voter, 3L, 2, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        var localLogEndOffset = context.log.endOffset().offset();
        var lastFetchedEpoch = context.log.lastFetchedEpoch();
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                remote,
                localLogEndOffset,
                lastFetchedEpoch,
                OptionalLong.of(localLogEndOffset),
                Integer.MAX_VALUE
            )
        );

        // Check that the fetch response was deferred
        for (var i = 0; i < 10; ++i) {
            context.client.poll();
            assertEquals(List.of(), context.drainSentResponses(ApiKeys.FETCH));
        }

        // Update the HWM and complete the deferred FETCH response
        context.deliverRequest(
            context.fetchRequest(epoch, voter, localLogEndOffset, lastFetchedEpoch, 0)
        );
        context.pollUntilResponse();

        // Check that two fetch requests were completed
        var fetchResponses = context.drainSentResponses(ApiKeys.FETCH);
        for (var fetchResponse : fetchResponses) {
            var partitionResponse = context.assertFetchResponseData(fetchResponse);
            assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
            assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
            assertEquals(localLogEndOffset, partitionResponse.highWatermark());
        }
    }
}
