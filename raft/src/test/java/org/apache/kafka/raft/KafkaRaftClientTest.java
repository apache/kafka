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

import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.raft.RaftClientTestContext.ELECTION_BACKOFF_MAX_MS;
import static org.apache.kafka.raft.RaftClientTestContext.ELECTION_TIMEOUT_MS;
import static org.apache.kafka.raft.RaftClientTestContext.FETCH_TIMEOUT_MS;
import static org.apache.kafka.raft.RaftClientTestContext.LOCAL_ID;
import static org.apache.kafka.raft.RaftClientTestContext.METADATA_PARTITION;
import static org.apache.kafka.raft.RaftClientTestContext.REQUEST_TIMEOUT_MS;
import static org.apache.kafka.raft.RaftClientTestContext.RETRY_BACKOFF_MS;
import static org.apache.kafka.raft.RaftClientTestContext.fetchRequest;
import static org.apache.kafka.raft.RaftClientTestContext.fetchResponse;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientTest {
    @Test
    public void testInitializeSingleMemberQuorum() throws IOException {
        RaftClientTestContext context = RaftClientTestContext.build(Collections.singleton(LOCAL_ID));
        assertEquals(
            ElectionState.withElectedLeader(1, LOCAL_ID, Collections.singleton(LOCAL_ID)),
            context.quorumStateStore.readElectionState()
        );
    }

    @Test
    public void testInitializeAsLeaderFromStateStoreSingleMemberQuorum() throws Exception {
        // Start off as leader. We should still bump the epoch after initialization

        int initialEpoch = 2;
        Set<Integer> voters = Collections.singleton(LOCAL_ID);
        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(
                        ElectionState.withElectedLeader(initialEpoch, LOCAL_ID, voters)
                    );
                });
            })
            .build(voters);

        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(initialEpoch + 1, context.log.lastFetchedEpoch());
        assertEquals(new LeaderAndEpoch(OptionalInt.of(LOCAL_ID), initialEpoch + 1),
            context.client.currentLeaderAndEpoch());
        assertEquals(ElectionState.withElectedLeader(initialEpoch + 1, LOCAL_ID, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testInitializeAsLeaderFromStateStore() throws Exception {
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, 1);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(RaftClientTestContext.ELECTION_TIMEOUT_MS);
            })
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, LOCAL_ID, voters));
                });
            })
            .build(voters);


        assertEquals(0L, context.log.endOffset().offset);
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), context.quorumStateStore.readElectionState());

        context.time.sleep(RaftClientTestContext.ELECTION_TIMEOUT_MS);
        context.pollUntilSend();
        context.assertSentVoteRequest(epoch + 1, 0, 0L);
    }

    @Test
    public void testInitializeAsCandidateFromStateStore() throws Exception {
        // Need 3 node to require a 2-node majority
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, 1, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(2, LOCAL_ID, voters));
                });
            })
            .build(voters);

        assertEquals(0L, context.log.endOffset().offset);

        // Send out vote requests.
        context.client.poll();

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(2, 0, 0);
        assertEquals(2, voteRequests.size());
    }

    @Test
    public void testInitializeAsCandidateAndBecomeLeader() throws Exception {
        final int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);
        RaftClientTestContext context = RaftClientTestContext.build(voters);

        assertEquals(ElectionState.withUnknownLeader(0, voters), context.quorumStateStore.readElectionState());
        context.time.sleep(2 * RaftClientTestContext.ELECTION_TIMEOUT_MS);

        context.pollUntilSend();
        assertEquals(ElectionState.withVotedCandidate(1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        int correlationId = context.assertSentVoteRequest(1, 0, 0L);
        context.deliverResponse(correlationId, otherNodeId, RaftClientTestContext.voteResponse(true, Optional.empty(), 1));

        // Become leader after receiving the vote
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
        long electionTimestamp = context.time.milliseconds();

        // Leader change record appended
        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(1L, context.log.lastFlushedOffset());

        // Send BeginQuorumEpoch to voters
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(1);

        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());

        Record record = batch.iterator().next();
        assertEquals(electionTimestamp, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(LOCAL_ID, Collections.singletonList(otherNodeId), record.key(), record.value());
    }

    @Test
    public void testHandleBeginQuorumRequest() throws Exception {
        int otherNodeId = 1;
        int votedCandidateEpoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(votedCandidateEpoch, otherNodeId, voters));
                });
            })
            .build(voters);


        context.deliverRequest(beginEpochRequest(votedCandidateEpoch, otherNodeId));

        context.client.poll();

        assertEquals(
            ElectionState.withElectedLeader(votedCandidateEpoch, otherNodeId, voters),
            context.quorumStateStore.readElectionState()
        );

        context.assertSentBeginQuorumEpochResponse(Errors.NONE, votedCandidateEpoch, OptionalInt.of(otherNodeId));
    }

    @Test
    public void testHandleBeginQuorumResponse() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, LOCAL_ID, voters));
                });
            })
            .build(voters);

        context.deliverRequest(beginEpochRequest(leaderEpoch + 1, otherNodeId));

        context.client.poll();

        assertEquals(
            ElectionState.withElectedLeader(leaderEpoch + 1, otherNodeId, voters),
            context.quorumStateStore.readElectionState()
        );
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        int jitterMs = 85;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateRandom(random -> {
                Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());
            })
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters));
                });
            })
            .build(voters);

        context.deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), otherNodeId, Collections.singletonList(LOCAL_ID)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.empty());

        // We should still be candidate until expiration of election timeout
        context.time.sleep(ELECTION_TIMEOUT_MS + jitterMs - 1);
        context.client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        // Enter the backoff period
        context.time.sleep(1);
        context.client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        // After backoff, we will become a candidate again
        context.time.sleep(ELECTION_BACKOFF_MAX_MS);
        context.client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyLeader() throws Exception {
        int voter2 = LOCAL_ID + 1;
        int voter3 = LOCAL_ID + 2;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, voter2, voter3);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // One of the voters may have sent EndEpoch as a candidate because it
        // had not yet been notified that the local node was the leader.
        context.deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), voter2, Arrays.asList(LOCAL_ID, voter3)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));

        // We should still be leader as long as fetch timeout has not expired
        context.time.sleep(FETCH_TIMEOUT_MS - 1);
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumStartsNewElectionAfterBackoffIfReceivedFromVotedCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        context.deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), otherNodeId, Collections.singletonList(LOCAL_ID)));
        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.empty());

        context.time.sleep(ELECTION_BACKOFF_MAX_MS);
        context.client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumStartsNewElectionImmediatelyIfFollowerUnattached() throws Exception {
        int voter2 = LOCAL_ID + 1;
        int voter3 = LOCAL_ID + 2;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
                });
            })
            .build(voters);
        
        context.deliverRequest(endEpochRequest(epoch, OptionalInt.of(voter2), voter2, Arrays.asList(LOCAL_ID, voter3)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(voter2));

        // Should become a candidate immediately
        context.client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testLocalReadFromLeader() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        context.deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 0));
        context.client.poll();
        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());
        context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.client.append(MemoryRecords.withRecords(CompressionType.NONE, records), AckMode.LEADER, Integer.MAX_VALUE);
        context.client.poll();
        assertEquals(3L, context.log.endOffset().offset);
        assertEquals(3L, context.log.lastFlushedOffset());
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        context.validateLocalRead(new OffsetAndEpoch(1L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        context.validateLocalRead(new OffsetAndEpoch(1L, epoch), Isolation.UNCOMMITTED, records);
        context.validateLocalRead(new OffsetAndEpoch(3L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        context.validateLocalRead(new OffsetAndEpoch(3L, epoch), Isolation.UNCOMMITTED, new SimpleRecord[0]);

        context.deliverRequest(fetchRequest(epoch, otherNodeId, 3L, epoch, 0));
        context.client.poll();
        assertEquals(OptionalLong.of(3L), context.client.highWatermark());
        context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));

        context.validateLocalRead(new OffsetAndEpoch(1L, epoch), Isolation.COMMITTED, records);
    }

    @Test
    public void testDelayedLocalReadFromLeader() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        context.deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 0));
        context.client.poll();
        assertEquals(1L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());
        context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));

        CompletableFuture<Records> logEndReadFuture = context.client.read(new OffsetAndEpoch(1L, epoch),
            Isolation.UNCOMMITTED, 500);
        assertFalse(logEndReadFuture.isDone());

        CompletableFuture<Records> highWatermarkReadFuture = context.client.read(new OffsetAndEpoch(1L, epoch),
            Isolation.COMMITTED, 500);
        assertFalse(logEndReadFuture.isDone());

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.client.append(MemoryRecords.withRecords(CompressionType.NONE, records), AckMode.LEADER, Integer.MAX_VALUE);
        context.client.poll();
        assertEquals(3L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        assertTrue(logEndReadFuture.isDone());
        RaftClientTestContext.assertMatchingRecords(records, logEndReadFuture.get());
        assertFalse(highWatermarkReadFuture.isDone());

        context.deliverRequest(fetchRequest(epoch, otherNodeId, 3L, epoch, 0));
        context.client.poll();
        assertEquals(OptionalLong.of(3L), context.client.highWatermark());
        context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));

        assertTrue(highWatermarkReadFuture.isDone());
        RaftClientTestContext.assertMatchingRecords(records, highWatermarkReadFuture.get());
    }

    @Test
    public void testLocalReadFromFollower() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        SimpleRecord[] records1 = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(0, 0), records1, 2L);
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());

        context.validateLocalRead(new OffsetAndEpoch(0, 0), Isolation.COMMITTED, records1);
        context.validateLocalRead(new OffsetAndEpoch(0, 0), Isolation.UNCOMMITTED, records1);
        context.validateLocalRead(new OffsetAndEpoch(2L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        context.validateLocalRead(new OffsetAndEpoch(2L, epoch), Isolation.UNCOMMITTED, new SimpleRecord[0]);

        SimpleRecord[] records2 = new SimpleRecord[] {
            new SimpleRecord("c".getBytes()),
            new SimpleRecord("d".getBytes()),
            new SimpleRecord("e".getBytes())
        };
        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(2L, epoch), records2, 2L);
        assertEquals(5L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());

        context.validateLocalRead(new OffsetAndEpoch(2L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        context.validateLocalRead(new OffsetAndEpoch(2L, epoch), Isolation.UNCOMMITTED, records2);
        context.validateLocalRead(new OffsetAndEpoch(5L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        context.validateLocalRead(new OffsetAndEpoch(5L, epoch), Isolation.UNCOMMITTED, new SimpleRecord[0]);

        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(5L, epoch), new SimpleRecord[0], 5L);
        assertEquals(5L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(5L), context.client.highWatermark());

        context.validateLocalRead(new OffsetAndEpoch(2L, epoch), Isolation.COMMITTED, records2);
        context.validateLocalRead(new OffsetAndEpoch(5L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
    }

    @Test
    public void testDelayedLocalReadFromFollowerToHighWatermark() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(0, 0), records, 0L);
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        CompletableFuture<Records> future = context.client.read(new OffsetAndEpoch(0, 0),
            Isolation.COMMITTED, 500);
        assertFalse(future.isDone());

        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(2L, epoch), new SimpleRecord[0], 2L);
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());
        assertTrue(future.isDone());
        RaftClientTestContext.assertMatchingRecords(records, future.get());
    }

    @Test
    public void testDelayedLocalReadFromFollowerToHighWatermarkTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        SimpleRecord[] records1 = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(0, 0), records1, 0L);
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        CompletableFuture<Records> future = context.client.read(new OffsetAndEpoch(0, 0),
            Isolation.COMMITTED, 500);
        assertFalse(future.isDone());

        context.time.sleep(500);
        context.client.poll();
        assertTrue(future.isDone());
        assertFutureThrows(future, org.apache.kafka.common.errors.TimeoutException.class);
    }

    @Test
    public void testLocalReadLogTruncationError() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.fetchFromLeader(otherNodeId, epoch, new OffsetAndEpoch(0, 0), records, 2L);
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());

        CompletableFuture<Records> future = context.client.read(new OffsetAndEpoch(1, 1),
            Isolation.COMMITTED, 0);
        assertTrue(future.isDone());
        assertFutureThrows(future, LogTruncationException.class);
    }

    @Test
    public void testDelayedLocalReadLogTruncationErrorAfterUncleanElection() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        // Initialize as leader and append some data that will eventually get truncated
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);
        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        context.client.append(MemoryRecords.withRecords(CompressionType.NONE, records),
            AckMode.LEADER, Integer.MAX_VALUE);
        context.client.poll();
        assertEquals(3L, context.log.endOffset().offset);

        // The other node becomes leader
        int newEpoch = 3;
        context.deliverRequest(beginEpochRequest(newEpoch, otherNodeId));
        context.client.poll();
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, newEpoch, OptionalInt.of(otherNodeId));

        CompletableFuture<Records> future = context.client.read(new OffsetAndEpoch(3L, epoch),
            Isolation.UNCOMMITTED, 500);
        assertFalse(future.isDone());

        // We send a fetch at the current offset and the leader tells us to truncate
        context.pollUntilSend();
        int fetchCorrelationId = context.assertSentFetchRequest(newEpoch, 3L, epoch);
        FetchResponseData fetchResponse = outOfRangeFetchRecordsResponse(
            newEpoch, otherNodeId, 1L, epoch, 0L);
        context.deliverResponse(fetchCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(1L, context.log.endOffset().offset);
        assertTrue(future.isDone());
        assertFutureThrows(future, LogTruncationException.class);
    }


    @Test
    public void testHandleEndQuorumRequest() throws Exception {
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, oldLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, oldLeaderId, voters));
                });
            })
            .build(voters);

        context.deliverRequest(endEpochRequest(leaderEpoch, OptionalInt.of(oldLeaderId), oldLeaderId, Collections.singletonList(LOCAL_ID)));

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        context.client.poll();
        assertEquals(
            ElectionState.withVotedCandidate(leaderEpoch + 1, LOCAL_ID, voters),
            context.quorumStateStore.readElectionState()
        );
    }

    @Test
    public void testHandleEndQuorumRequestWithLowerPriorityToBecomeLeader() throws Exception {
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        int preferredNextLeader = 3;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, oldLeaderId, preferredNextLeader);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, oldLeaderId, voters));
                });
            })
            .build(voters);

        context.deliverRequest(endEpochRequest(leaderEpoch,
            OptionalInt.of(oldLeaderId), oldLeaderId, Arrays.asList(preferredNextLeader, LOCAL_ID)));

        context.pollUntilSend();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        // The election won't trigger by one round retry backoff
        context.time.sleep(1);

        context.pollUntilSend();

        context.assertSentFetchRequest(leaderEpoch, 0, 0);

        context.time.sleep(RETRY_BACKOFF_MS);

        context.pollUntilSend();

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(leaderEpoch + 1, 0, 0);
        assertEquals(2, voteRequests.size());

        // Should have already done self-voting
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch + 1, LOCAL_ID, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testVoteRequestTimeout() throws Exception {
        int epoch = 1;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.build(voters);

        assertEquals(ElectionState.withUnknownLeader(0, voters), context.quorumStateStore.readElectionState());

        context.time.sleep(2 * ELECTION_TIMEOUT_MS);
        context.pollUntilSend();
        assertEquals(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        int correlationId = context.assertSentVoteRequest(epoch, 0, 0L);

        context.time.sleep(REQUEST_TIMEOUT_MS);
        context.client.poll();
        int retryCorrelationId = context.assertSentVoteRequest(epoch, 0, 0L);

        // Even though we have resent the request, we should still accept the response to
        // the first request if it arrives late.
        context.deliverResponse(correlationId, otherNodeId, RaftClientTestContext.voteResponse(true, Optional.empty(), 1));
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        // If the second request arrives later, it should have no effect
        context.deliverResponse(retryCorrelationId, otherNodeId, RaftClientTestContext.voteResponse(true, Optional.empty(), 1));
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleValidVoteRequestAsFollower() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
                });
            })
            .build(voters);

        context.deliverRequest(voteRequest(epoch, otherNodeId, epoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        assertEquals(ElectionState.withVotedCandidate(epoch, otherNodeId, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithElectedLeader() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        int electedLeaderId = 3;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId, electedLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, electedLeaderId, voters));
                });
            })
            .build(voters);

        context.deliverRequest(voteRequest(epoch, otherNodeId, epoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), false);

        assertEquals(ElectionState.withElectedLeader(epoch, electedLeaderId, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithVotedCandidate() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        int votedCandidateId = 3;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId, votedCandidateId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, votedCandidateId, voters));
                });
            })
            .build(voters);

        context.deliverRequest(voteRequest(epoch, otherNodeId, epoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withVotedCandidate(epoch, votedCandidateId, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleInvalidVoteRequestWithOlderEpoch() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
                });
            })
            .build(voters);

        context.deliverRequest(voteRequest(epoch - 1, otherNodeId, epoch - 2, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleInvalidVoteRequestAsObserver() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        int otherNodeId2 = 2;
        Set<Integer> voters = Utils.mkSet(otherNodeId, otherNodeId2);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
                });
            })
            .build(voters);

        context.deliverRequest(voteRequest(epoch + 1, otherNodeId, epoch, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testLeaderIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, leaderEpoch);

        context.deliverRequest(voteRequest(leaderEpoch, otherNodeId, leaderEpoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(LOCAL_ID), false);
        assertEquals(ElectionState.withElectedLeader(leaderEpoch, LOCAL_ID, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testStateMachineApplyCommittedRecords() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 0L, epoch, 500));
        context.pollUntilSend();
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // Append some records with leader commit mode
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = context.client.append(records,
            AckMode.LEADER, Integer.MAX_VALUE);

        context.client.poll();
        assertTrue(future.isDone());
        assertEquals(new OffsetAndEpoch(3, epoch), future.get());

        // Let follower send a fetch, it should advance the high watermark
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 500));
        context.pollUntilSend();
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        // Let the follower to send another fetch from offset 4
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 4L, epoch, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(4L), context.client.highWatermark());

        // Append more records with quorum commit mode
        appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        future = context.client.append(records, AckMode.QUORUM, Integer.MAX_VALUE);

        // Appending locally should not complete the future
        context.client.poll();
        assertFalse(future.isDone());

        // Let follower send a fetch, it should not yet advance the high watermark
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 4L, epoch, 500));
        context.pollUntilSend();
        assertFalse(future.isDone());
        assertEquals(OptionalLong.of(4L), context.client.highWatermark());

        // Let the follower to send another fetch at 7, which should not advance the high watermark and complete the future
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 7L, epoch, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(7L), context.client.highWatermark());

        assertTrue(future.isDone());
        assertEquals(new OffsetAndEpoch(6, epoch), future.get());
    }

    @Test
    public void testStateMachineExpireAppendedRecords() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 0L, epoch, 500));
        context.pollUntilSend();
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // Append some records with quorum commit mode
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };

        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = context.client.append(records, AckMode.QUORUM, REQUEST_TIMEOUT_MS);

        context.client.poll();
        assertFalse(future.isDone());

        context.time.sleep(REQUEST_TIMEOUT_MS - 1);
        assertFalse(future.isDone());

        context.time.sleep(1);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void testCandidateIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(leaderEpoch, LOCAL_ID, voters));
                });
            })
            .build(voters);

        context.pollUntilSend();

        context.deliverRequest(voteRequest(leaderEpoch, otherNodeId, leaderEpoch - 1, 1));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch, LOCAL_ID, voters),
            context.quorumStateStore.readElectionState());
    }

    @Test
    public void testRetryElection() throws Exception {
        int otherNodeId = 1;
        int epoch = 1;
        int exponentialFactor = 85;  // set it large enough so that we will bound on jitter
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateRandom(random -> {
                Mockito.doReturn(exponentialFactor).when(random).nextInt(Mockito.anyInt());
            })
            .build(voters);


        assertEquals(ElectionState.withUnknownLeader(0, voters), context.quorumStateStore.readElectionState());

        context.time.sleep(2 * ELECTION_TIMEOUT_MS);
        context.pollUntilSend();
        assertEquals(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int correlationId = context.assertSentVoteRequest(epoch, 0, 0L);
        context.deliverResponse(correlationId, otherNodeId, RaftClientTestContext.voteResponse(false, Optional.empty(), 1));

        context.client.poll();

        // All nodes have rejected our candidacy, but we should still remember that we had voted
        ElectionState latest = context.quorumStateStore.readElectionState();
        assertEquals(epoch, latest.epoch);
        assertTrue(latest.hasVoted());
        assertEquals(LOCAL_ID, latest.votedId());

        // Even though our candidacy was rejected, we will backoff for jitter period
        // before we bump the epoch and start a new election.
        context.time.sleep(ELECTION_BACKOFF_MAX_MS - 1);
        context.client.poll();
        assertEquals(epoch, context.quorumStateStore.readElectionState().epoch);

        // After jitter expires, we become a candidate again
        context.time.sleep(1);
        context.client.poll();
        context.pollUntilSend();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
        context.assertSentVoteRequest(epoch + 1, 0, 0L);
    }

    @Test
    public void testInitializeAsFollowerEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.pollUntilSend();

        context.assertSentFetchRequest(epoch, 0L, 0);
    }

    @Test
    public void testInitializeAsFollowerNonEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .updateLog(log -> {
                log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);
            })
            .build(voters);


        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.pollUntilSend();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);
    }

    @Test
    public void testVoterBecomeCandidateAfterFetchTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .updateLog(log -> {
                log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.pollUntilSend();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);

        context.time.sleep(FETCH_TIMEOUT_MS);

        context.pollUntilSend();

        context.assertSentVoteRequest(epoch + 1, lastEpoch, 1L);
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testInitializeObserverNoPreviousState() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.build(voters);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));

        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverQuorumDiscoveryFailure() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);

        RaftClientTestContext context = RaftClientTestContext.build(voters);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(-1, -1, MemoryRecords.EMPTY, -1, Errors.UNKNOWN_SERVER_ERROR));
        context.client.poll();

        context.time.sleep(RETRY_BACKOFF_MS);
        context.pollUntilSend();

        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        context.client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverSendDiscoveryFetchAfterFetchTimeout() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.build(voters);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        context.client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), context.quorumStateStore.readElectionState());
        context.time.sleep(FETCH_TIMEOUT_MS);

        context.pollUntilSend();
        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testInvalidFetchRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        context.deliverRequest(fetchRequest(
            epoch, otherNodeId, -5L, 0, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(LOCAL_ID));

        context.deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, -1, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(LOCAL_ID));

        context.deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, epoch + 1, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(LOCAL_ID));

        context.deliverRequest(fetchRequest(
            epoch + 1, otherNodeId, 0L, 0, 0));
        context.client.poll();
        context.assertSentFetchResponse(Errors.UNKNOWN_LEADER_EPOCH, epoch, OptionalInt.of(LOCAL_ID));

        context.deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, 0, -1));
        context.client.poll();
        context.assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(LOCAL_ID));
    }

    @Test
    public void testVoterOnlyRequestValidation() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        int nonVoterId = 2;
        context.deliverRequest(voteRequest(epoch, nonVoterId, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(LOCAL_ID), false);

        context.deliverRequest(beginEpochRequest(epoch, nonVoterId));
        context.client.poll();
        context.assertSentBeginQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(LOCAL_ID));

        context.deliverRequest(endEpochRequest(epoch, OptionalInt.of(LOCAL_ID), nonVoterId, Collections.singletonList(otherNodeId)));
        context.client.poll();

        // The sent request has no LOCAL_ID as a preferable voter.
        context.assertSentEndQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(LOCAL_ID));
    }

    @Test
    public void testInvalidVoteRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.deliverRequest(voteRequest(epoch + 1, otherNodeId, 0, -5L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.deliverRequest(voteRequest(epoch + 1, otherNodeId, -1, 0L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.deliverRequest(voteRequest(epoch + 1, otherNodeId, epoch + 1, 0L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testPurgatoryFetchTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        int maxWaitTimeMs = 500;
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, maxWaitTimeMs));
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());

        // After expiration of the max wait time, the fetch returns an empty record set
        context.time.sleep(maxWaitTimeMs);
        context.client.poll();
        MemoryRecords fetchedRecords = context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @Test
    public void testPurgatoryFetchSatisfiedByWrite() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        context.deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 500));
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());

        // Append some records that can fulfill the Fetch request
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = context.client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        context.client.poll();
        assertTrue(future.isDone());

        MemoryRecords fetchedRecords = context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));
        List<Record> recordList = Utils.toList(fetchedRecords.records());
        assertEquals(appendRecords.length, recordList.size());
        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i], new SimpleRecord(recordList.get(i)));
        }
    }

    @Test
    public void testPurgatoryFetchCompletedByFollowerTransition() throws Exception {
        int voter1 = LOCAL_ID;
        int voter2 = LOCAL_ID + 1;
        int voter3 = LOCAL_ID + 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        context.deliverRequest(fetchRequest(epoch, voter2, 1L, epoch, 500));
        context.client.poll();
        assertTrue(context.channel.drainSendQueue().stream()
            .noneMatch(msg -> msg.data() instanceof FetchResponseData));

        // Now we get a BeginEpoch from the other voter and become a follower
        context.deliverRequest(beginEpochRequest(epoch + 1, voter3));
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), context.quorumStateStore.readElectionState());

        // We expect the BeginQuorumEpoch response and a failed Fetch response
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.of(voter3));

        // The fetch should be satisfied immediately and return an error
        MemoryRecords fetchedRecords = context.assertSentFetchResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, epoch + 1, OptionalInt.of(voter3));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }


    @Test
    public void testFetchResponseIgnoredAfterBecomingCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        // The other node starts out as the leader
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        // Wait until we have a Fetch inflight to the leader
        context.pollUntilSend();
        int fetchCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);

        // Now await the fetch timeout and become a candidate
        context.time.sleep(FETCH_TIMEOUT_MS);
        context.client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        context.deliverResponse(fetchCorrelationId, otherNodeId,
            fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE));

        context.client.poll();
        assertEquals(0, context.log.endOffset().offset);
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingFollowerOfDifferentLeader() throws Exception {
        int voter1 = LOCAL_ID;
        int voter2 = LOCAL_ID + 1;
        int voter3 = LOCAL_ID + 2;
        int epoch = 5;
        // Start out with `voter2` as the leader
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, voter2, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, voter2, voters), context.quorumStateStore.readElectionState());

        // Wait until we have a Fetch inflight to the leader
        context.pollUntilSend();
        int fetchCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);

        // Now receive a BeginEpoch from `voter3`
        context.deliverRequest(beginEpochRequest(epoch + 1, voter3));
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), context.quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchResponseData response = fetchResponse(epoch, voter2, records, 0L, Errors.NONE);
        context.deliverResponse(fetchCorrelationId, voter2, response);

        context.client.poll();
        assertEquals(0, context.log.endOffset().offset);
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testVoteResponseIgnoredAfterBecomingFollower() throws Exception {
        int voter1 = LOCAL_ID;
        int voter2 = LOCAL_ID + 1;
        int voter3 = LOCAL_ID + 2;
        int epoch = 5;
        // This node initializes as a candidate
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, voter1, voters));
                });
            })
            .build(voters);


        assertEquals(ElectionState.withVotedCandidate(epoch, voter1, voters), context.quorumStateStore.readElectionState());

        // Wait until the vote requests are inflight
        context.pollUntilSend();
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // While the vote requests are still inflight, we receive a BeginEpoch for the same epoch
        context.deliverRequest(beginEpochRequest(epoch, voter3));
        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter3, voters), context.quorumStateStore.readElectionState());

        // The vote requests now return and should be ignored
        VoteResponseData voteResponse1 = RaftClientTestContext.voteResponse(false, Optional.empty(), epoch);
        context.deliverResponse(voteRequests.get(0).correlationId, voter2, voteResponse1);

        VoteResponseData voteResponse2 = RaftClientTestContext.voteResponse(false, Optional.of(voter3), epoch);
        context.deliverResponse(voteRequests.get(1).correlationId, voter3, voteResponse2);

        context.client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter3, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverLeaderRediscoveryAfterBrokerNotAvailableError() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.build(voters);

        context.discoverLeaderAsObserver(voters, leaderId, epoch);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destinationId());
        RaftClientTestContext.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.deliverResponse(fetchRequest1.correlationId, fetchRequest1.destinationId(),
            fetchResponse(epoch, -1, MemoryRecords.EMPTY, -1, Errors.BROKER_NOT_AVAILABLE));
        context.pollUntilSend();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destinationId());
        assertTrue(voters.contains(fetchRequest2.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        Errors error = fetchRequest2.destinationId() == leaderId ?
            Errors.NONE : Errors.NOT_LEADER_OR_FOLLOWER;
        context.deliverResponse(fetchRequest2.correlationId, fetchRequest2.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, error));
        context.client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverLeaderRediscoveryAfterRequestTimeout() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.build(voters);

        context.discoverLeaderAsObserver(voters, leaderId, epoch);

        context.pollUntilSend();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destinationId());
        RaftClientTestContext.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.time.sleep(REQUEST_TIMEOUT_MS);
        context.pollUntilSend();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destinationId());
        assertTrue(voters.contains(fetchRequest2.destinationId()));
        RaftClientTestContext.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        context.deliverResponse(fetchRequest2.correlationId, fetchRequest2.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        context.client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), context.quorumStateStore.readElectionState());
    }

    @Test
    public void testLeaderGracefulShutdown() throws Exception {
        int otherNodeId = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isShuttingDown());
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other voter
        context.client.poll();
        assertTrue(context.client.isShuttingDown());
        assertTrue(context.client.isRunning());
        context.assertSentEndQuorumEpochRequest(1, OptionalInt.of(LOCAL_ID), otherNodeId);

        // We should still be able to handle vote requests during graceful shutdown
        // in order to help the new leader get elected
        context.deliverRequest(voteRequest(epoch + 1, otherNodeId, epoch, 1L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);

        // Graceful shutdown completes when a new leader is elected
        context.deliverRequest(beginEpochRequest(2, otherNodeId));

        TestUtils.waitForCondition(() -> {
            context.client.poll();
            return !context.client.isRunning();
        }, 5000, "Client failed to shutdown before expiration of timeout");
        assertFalse(context.client.isShuttingDown());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @Test
    public void testEndQuorumEpochSentBasedOnFetchOffset() throws Exception {
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, closeFollower, laggingFollower);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        context.buildFollowerSet(epoch, closeFollower, laggingFollower);

        // Now shutdown
        context.client.shutdown(ELECTION_TIMEOUT_MS * 2);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isRunning());

        // Send EndQuorumEpoch request to the close follower
        context.client.poll();
        assertTrue(context.client.isRunning());

        List<RaftRequest.Outbound> endQuorumRequests =
            context.collectEndQuorumRequests(1, OptionalInt.of(LOCAL_ID), Utils.mkSet(closeFollower, laggingFollower));

        assertEquals(2, endQuorumRequests.size());
    }

    @Test
    public void testDescribeQuorum() throws Exception {
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, closeFollower, laggingFollower);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        context.buildFollowerSet(epoch, closeFollower, laggingFollower);

        // Create observer
        int observerId = 3;
        context.deliverRequest(fetchRequest(epoch, observerId, 0L, 0, 0));

        context.client.poll();

        long highWatermark = 1L;
        context.assertSentFetchResponse(highWatermark, epoch);

        context.deliverRequest(DescribeQuorumRequest.singletonRequest(METADATA_PARTITION));

        context.client.poll();

        context.assertSentDescribeQuorumResponse(LOCAL_ID, epoch, highWatermark,
            Arrays.asList(
                new ReplicaState()
                    .setReplicaId(LOCAL_ID)
                    // As we are appending the records directly to the log,
                    // the leader end offset hasn't been updated yet.
                    .setLogEndOffset(3L),
                new ReplicaState()
                    .setReplicaId(laggingFollower)
                    .setLogEndOffset(0L),
                new ReplicaState()
                    .setReplicaId(closeFollower)
                    .setLogEndOffset(1L)),
            Collections.singletonList(
                new ReplicaState()
                    .setReplicaId(observerId)
                    .setLogEndOffset(0L)));
    }

    @Test
    public void testLeaderGracefulShutdownTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other vote
        context.client.poll();
        assertTrue(context.client.isRunning());

        context.assertSentEndQuorumEpochRequest(epoch, OptionalInt.of(LOCAL_ID), otherNodeId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        context.time.sleep(shutdownTimeoutMs);

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isCompletedExceptionally());
        assertFutureThrows(shutdownFuture, TimeoutException.class);
    }

    @Test
    public void testFollowerGracefulShutdown() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.client.poll();

        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @Test
    public void testGracefulShutdownSingleMemberQuorum() throws IOException {
        RaftClientTestContext context = RaftClientTestContext.build(Collections.singleton(LOCAL_ID));

        assertEquals(ElectionState.withElectedLeader(
            1, LOCAL_ID, Collections.singleton(LOCAL_ID)), context.quorumStateStore.readElectionState());
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());
        int shutdownTimeoutMs = 5000;
        context.client.shutdown(shutdownTimeoutMs);
        assertTrue(context.client.isRunning());
        context.client.poll();
        assertFalse(context.client.isRunning());
    }

    @Test
    public void testFollowerReplication() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.pollUntilSend();

        int fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchResponseData response = fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, response);

        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(2L, context.log.lastFlushedOffset());
    }

    @Test
    public void testEmptyRecordSetInFetchResponse() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        // Receive an empty fetch response
        context.pollUntilSend();
        int fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);
        FetchResponseData fetchResponse = fetchResponse(epoch, otherNodeId,
            MemoryRecords.EMPTY, 0L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(0L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // Receive some records in the next poll, but do not advance high watermark
        context.pollUntilSend();
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            epoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 0L, 0);
        fetchResponse = fetchResponse(epoch, otherNodeId,
            records, 0L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // The next fetch response is empty, but should still advance the high watermark
        context.pollUntilSend();
        fetchQuorumCorrelationId = context.assertSentFetchRequest(epoch, 2L, epoch);
        fetchResponse = fetchResponse(epoch, otherNodeId,
            MemoryRecords.EMPTY, 2L, Errors.NONE);
        context.deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());
    }

    @Test
    public void testAppendEmptyRecordSetNotAllowed() throws Exception {
        int epoch = 5;
        Set<Integer> voters = Collections.singleton(LOCAL_ID);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, LOCAL_ID, voters));
                });
            })
            .build(voters);

        assertThrows(IllegalArgumentException.class, () ->
            context.client.append(MemoryRecords.EMPTY, AckMode.LEADER, Integer.MAX_VALUE));
    }

    @Test
    public void testAppendToNonLeaderFails() throws IOException {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);

        CompletableFuture<OffsetAndEpoch> future = context.client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        context.client.poll();

        assertFutureThrows(future, NotLeaderOrFollowerException.class);
    }

    @Test
    public void testFetchShouldBeTreatedAsLeaderEndorsement() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(ELECTION_TIMEOUT_MS);
            })
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch - 1, voters));
                });
            })
            .build(voters);

        context.time.sleep(ELECTION_TIMEOUT_MS);
        context.expectLeaderElection(voters, epoch);

        context.pollUntilSend();

        // We send BeginEpoch, but it gets lost and the destination finds the leader through the Fetch API
        context.assertSentBeginQuorumEpochRequest(epoch);

        context.deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, 0, 500));

        context.client.poll();

        // The BeginEpoch request eventually times out. We should not send another one.
        context.assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(LOCAL_ID));
        context.time.sleep(REQUEST_TIMEOUT_MS);

        context.client.poll();

        List<RaftMessage> sentMessages = context.channel.drainSendQueue();
        assertEquals(0, sentMessages.size());
    }

    @Test
    public void testLeaderAppendSingleMemberQuorum() throws IOException {
        Set<Integer> voters = Collections.singleton(LOCAL_ID);

        RaftClientTestContext context = RaftClientTestContext.build(voters);
        long now = context.time.milliseconds();

        assertEquals(ElectionState.withElectedLeader(1, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        // We still write the leader change message
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(1L, CompressionType.NONE, 1, appendRecords);

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        context.client.append(records, AckMode.LEADER, Integer.MAX_VALUE);

        // Then poll the appended data with leader change record
        context.client.poll();
        assertEquals(OptionalLong.of(4L), context.client.highWatermark());

        // Now try reading it
        int otherNodeId = 1;
        context.deliverRequest(fetchRequest(1, otherNodeId, 0L, 0, 500));

        context.client.poll();

        MemoryRecords fetchedRecords = context.assertSentFetchResponse(Errors.NONE, 1, OptionalInt.of(LOCAL_ID));
        List<MutableRecordBatch> batches = Utils.toList(fetchedRecords.batchIterator());
        assertEquals(2, batches.size());

        MutableRecordBatch leaderChangeBatch = batches.get(0);
        assertTrue(leaderChangeBatch.isControlBatch());
        List<Record> readRecords = Utils.toList(leaderChangeBatch.iterator());
        assertEquals(1, readRecords.size());

        Record record = readRecords.get(0);
        assertEquals(now, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(LOCAL_ID, Collections.emptyList(),
            record.key(), record.value());

        MutableRecordBatch batch = batches.get(1);
        assertEquals(1, batch.partitionLeaderEpoch());
        readRecords = Utils.toList(batch.iterator());
        assertEquals(3, readRecords.size());

        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i].value(), readRecords.get(i).value());
        }
    }

    @Test
    public void testFollowerLogReconciliation() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .updateLog(log -> {
                log.appendAsLeader(Arrays.asList(
                            new SimpleRecord("foo".getBytes()),
                            new SimpleRecord("bar".getBytes())), lastEpoch);
                log.appendAsLeader(Arrays.asList(
                            new SimpleRecord("baz".getBytes())), lastEpoch);
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());
        assertEquals(3L, context.log.endOffset().offset);

        context.pollUntilSend();

        int correlationId = context.assertSentFetchRequest(epoch, 3L, lastEpoch);

        FetchResponseData response = outOfRangeFetchRecordsResponse(epoch, otherNodeId, 2L,
            lastEpoch, 1L);
        context.deliverResponse(correlationId, otherNodeId, response);

        // Poll again to complete truncation
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset);

        // Now we should be fetching
        context.client.poll();
        context.assertSentFetchRequest(epoch, 2L, lastEpoch);
    }

    @Test
    public void testMetrics() throws Exception {
        int epoch = 1;

        RaftClientTestContext context = RaftClientTestContext.build(Collections.singleton(LOCAL_ID));

        assertNotNull(getMetric(context.metrics, "current-state"));
        assertNotNull(getMetric(context.metrics, "current-leader"));
        assertNotNull(getMetric(context.metrics, "current-vote"));
        assertNotNull(getMetric(context.metrics, "current-epoch"));
        assertNotNull(getMetric(context.metrics, "high-watermark"));
        assertNotNull(getMetric(context.metrics, "log-end-offset"));
        assertNotNull(getMetric(context.metrics, "log-end-epoch"));
        assertNotNull(getMetric(context.metrics, "number-unknown-voter-connections"));
        assertNotNull(getMetric(context.metrics, "poll-idle-ratio-avg"));
        assertNotNull(getMetric(context.metrics, "commit-latency-avg"));
        assertNotNull(getMetric(context.metrics, "commit-latency-max"));
        assertNotNull(getMetric(context.metrics, "election-latency-avg"));
        assertNotNull(getMetric(context.metrics, "election-latency-max"));
        assertNotNull(getMetric(context.metrics, "fetch-records-rate"));
        assertNotNull(getMetric(context.metrics, "append-records-rate"));

        assertEquals("leader", getMetric(context.metrics, "current-state").metricValue());
        assertEquals((double) LOCAL_ID, getMetric(context.metrics, "current-leader").metricValue());
        assertEquals((double) LOCAL_ID, getMetric(context.metrics, "current-vote").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "current-epoch").metricValue());
        assertEquals((double) 1L, getMetric(context.metrics, "high-watermark").metricValue());
        assertEquals((double) 1L, getMetric(context.metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "log-end-epoch").metricValue());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        context.client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        context.client.poll();

        assertEquals((double) 4L, getMetric(context.metrics, "high-watermark").metricValue());
        assertEquals((double) 4L, getMetric(context.metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "log-end-epoch").metricValue());

        CompletableFuture<Void> shutdownFuture = context.client.shutdown(100);
        context.client.poll();
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());

        // should only have total-metrics-count left
        assertEquals(1, context.metrics.metrics().size());
    }

    @Test
    public void testClusterAuthorizationFailedInFetch() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), context.quorumStateStore.readElectionState());

        context.pollUntilSend();

        int correlationId = context.assertSentFetchRequest(epoch, 0, 0);
        FetchResponseData response = new FetchResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInBeginQuorumEpoch() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(ELECTION_TIMEOUT_MS);
            })
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch - 1, voters));
                });
            })
            .build(voters);


        context.time.sleep(ELECTION_TIMEOUT_MS);
        context.expectLeaderElection(voters, epoch);

        context.pollUntilSend();
        int correlationId = context.assertSentBeginQuorumEpochRequest(epoch);
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInVote() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters));
                });
            })
            .build(voters);

        assertEquals(ElectionState.withVotedCandidate(epoch, LOCAL_ID, voters), context.quorumStateStore.readElectionState());

        context.pollUntilSend();
        int correlationId = context.assertSentVoteRequest(epoch, 0, 0L);
        VoteResponseData response = new VoteResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInEndQuorumEpoch() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(LOCAL_ID, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(voters, epoch);

        context.client.shutdown(5000);
        context.pollUntilSend();

        int correlationId = context.assertSentEndQuorumEpochRequest(epoch, OptionalInt.of(LOCAL_ID), otherNodeId);
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    private static KafkaMetric getMetric(final Metrics metrics, final String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
    }

    private static FetchResponseData outOfRangeFetchRecordsResponse(
        int epoch,
        int leaderId,
        long divergingEpochEndOffset,
        int divergingEpoch,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(METADATA_PARTITION, Errors.NONE, partitionData -> {
            partitionData
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);

            partitionData.divergingEpoch()
                .setEpoch(divergingEpoch)
                .setEndOffset(divergingEpochEndOffset);
        });
    }

    private static VoteRequestData voteRequest(int epoch, int candidateId, int lastEpoch, long lastEpochOffset) {
        return VoteRequest.singletonRequest(
            METADATA_PARTITION,
            epoch,
            candidateId,
            lastEpoch,
            lastEpochOffset
        );
    }

    private static BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return BeginQuorumEpochRequest.singletonRequest(
            METADATA_PARTITION,
            epoch,
            leaderId
        );
    }

    private static EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        OptionalInt leaderId,
        int replicaId,
        List<Integer> preferredSuccessors) {
        return EndQuorumEpochRequest.singletonRequest(
            METADATA_PARTITION,
            replicaId,
            epoch,
            leaderId.orElse(-1),
            preferredSuccessors
        );
    }
}
