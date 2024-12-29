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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.DataOutputStreamWritable;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchBuilder;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.raft.LeaderState.CHECK_QUORUM_TIMEOUT_FACTOR;
import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RaftClientTestContext {
    public final RecordSerde<String> serde = Builder.SERDE;
    final TopicPartition metadataPartition = Builder.METADATA_PARTITION;
    final Uuid metadataTopicId = Uuid.METADATA_TOPIC_ID;
    final int electionBackoffMaxMs = Builder.ELECTION_BACKOFF_MAX_MS;
    final int fetchMaxWaitMs = Builder.FETCH_MAX_WAIT_MS;
    final int fetchTimeoutMs = Builder.FETCH_TIMEOUT_MS;
    final int checkQuorumTimeoutMs = (int) (fetchTimeoutMs * CHECK_QUORUM_TIMEOUT_FACTOR);
    final int beginQuorumEpochTimeoutMs = fetchTimeoutMs / 2;
    final int retryBackoffMs = Builder.RETRY_BACKOFF_MS;

    private int electionTimeoutMs;
    private int requestTimeoutMs;
    private int appendLingerMs;

    private final QuorumStateStore quorumStateStore;
    final String clusterId;
    private final OptionalInt localId;
    public final Uuid localDirectoryId;
    public final KRaftVersion kraftVersion;
    public final KafkaRaftClient<String> client;
    final Metrics metrics;
    public final MockLog log;
    final MockNetworkChannel channel;
    final MockMessageQueue messageQueue;
    final MockTime time;
    final MockListener listener;
    final VoterSet startingVoters;
    final Set<Integer> bootstrapIds;
    // Used to determine which RPC request and response to construct
    final boolean kip853Rpc;
    // Used to determine if the local kraft client was configured to always flush
    final boolean alwaysFlush;

    private final List<RaftResponse.Outbound> sentResponses = new ArrayList<>();

    public static final class Builder {
        static final int DEFAULT_ELECTION_TIMEOUT_MS = 10000;

        private static final RecordSerde<String> SERDE = new StringSerde();
        private static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);
        private static final int ELECTION_BACKOFF_MAX_MS = 100;
        private static final int FETCH_MAX_WAIT_MS = 0;
        // fetch timeout is usually larger than election timeout
        private static final int FETCH_TIMEOUT_MS = 50000;
        private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
        private static final int RETRY_BACKOFF_MS = 50;
        private static final int DEFAULT_APPEND_LINGER_MS = 0;

        private final MockMessageQueue messageQueue = new MockMessageQueue();
        private final MockTime time = new MockTime();
        private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        private final MockableRandom random = new MockableRandom(1L);
        private final LogContext logContext = new LogContext();
        private final MockLog log = new MockLog(METADATA_PARTITION, Uuid.METADATA_TOPIC_ID, logContext);
        private final String clusterId = Uuid.randomUuid().toString();
        private final OptionalInt localId;
        private KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_0;
        private final Uuid localDirectoryId;

        private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;
        private int electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;
        private int appendLingerMs = DEFAULT_APPEND_LINGER_MS;
        private MemoryPool memoryPool = MemoryPool.NONE;
        private Optional<List<InetSocketAddress>> bootstrapServers = Optional.empty();
        private boolean kip853Rpc = false;
        private boolean alwaysFlush = false;
        private VoterSet startingVoters = VoterSet.empty();
        private Endpoints localListeners = Endpoints.empty();
        private boolean isStartingVotersStatic = false;

        public Builder(int localId, Set<Integer> staticVoters) {
            this(OptionalInt.of(localId), staticVoters);
        }

        public Builder(OptionalInt localId, Set<Integer> staticVoters) {
            this(localId, Uuid.randomUuid());

            withStaticVoters(staticVoters);
        }

        public Builder(int localId, Uuid localDirectoryId) {
            this(OptionalInt.of(localId), localDirectoryId);
        }

        public Builder(OptionalInt localId, Uuid localDirectoryId) {
            this.localId = localId;
            this.localDirectoryId = localDirectoryId;
        }

        Builder withElectedLeader(int epoch, int leaderId) {
            quorumStateStore.writeElectionState(
                ElectionState.withElectedLeader(epoch, leaderId, startingVoters.voterIds()),
                kraftVersion
            );
            return this;
        }

        Builder withUnknownLeader(int epoch) {
            quorumStateStore.writeElectionState(
                ElectionState.withUnknownLeader(epoch, startingVoters.voterIds()),
                kraftVersion
            );
            return this;
        }

        Builder withVotedCandidate(int epoch, ReplicaKey votedKey) {
            quorumStateStore.writeElectionState(
                ElectionState.withVotedCandidate(epoch, votedKey, startingVoters.voterIds()),
                kraftVersion
            );
            return this;
        }

        Builder updateRandom(Consumer<MockableRandom> consumer) {
            consumer.accept(random);
            return this;
        }

        Builder withMemoryPool(MemoryPool pool) {
            this.memoryPool = pool;
            return this;
        }

        Builder withAppendLingerMs(int appendLingerMs) {
            this.appendLingerMs = appendLingerMs;
            return this;
        }

        public Builder appendToLog(int epoch, List<String> records) {
            MemoryRecords batch = buildBatch(
                time.milliseconds(),
                log.endOffset().offset(),
                epoch,
                records
            );
            log.appendAsLeader(batch, epoch);
            // Need to flush the log to update the last flushed offset. This is always correct
            // because append operation was done in the Builder which represent the state of the
            // log before the replica starts.
            log.flush(false);

            // Reset the value of this method since "flush" before the replica start should not
            // count when checking for flushes by the KRaft client.
            log.flushedSinceLastChecked();
            return this;
        }

        Builder withEmptySnapshot(OffsetAndEpoch snapshotId) {
            try (RecordsSnapshotWriter<?> snapshot = new RecordsSnapshotWriter.Builder()
                    .setTime(time)
                    .setKraftVersion(KRaftVersion.KRAFT_VERSION_0)
                    .setRawSnapshotWriter(log.createNewSnapshotUnchecked(snapshotId).get())
                    .build(SERDE)
            ) {
                snapshot.freeze();
            }

            return this;
        }

        Builder deleteBeforeSnapshot(OffsetAndEpoch snapshotId) {
            if (snapshotId.offset() > log.highWatermark().offset()) {
                log.updateHighWatermark(new LogOffsetMetadata(snapshotId.offset()));
            }
            log.deleteBeforeSnapshot(snapshotId);

            return this;
        }

        Builder withElectionTimeoutMs(int electionTimeoutMs) {
            this.electionTimeoutMs = electionTimeoutMs;
            return this;
        }

        Builder withRequestTimeoutMs(int requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        Builder withBootstrapServers(Optional<List<InetSocketAddress>> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        Builder withKip853Rpc(boolean kip853Rpc) {
            this.kip853Rpc = kip853Rpc;
            return this;
        }

        Builder withAlwaysFlush(boolean alwaysFlush) {
            this.alwaysFlush = alwaysFlush;
            return this;
        }

        Builder withStaticVoters(Set<Integer> staticVoters) {
            Map<Integer, InetSocketAddress> staticVoterAddressMap = staticVoters
                .stream()
                .collect(
                    Collectors.toMap(Function.identity(), RaftClientTestContext::mockAddress)
                );

            return withStaticVoters(
                VoterSet.fromInetSocketAddresses(
                    MockNetworkChannel.LISTENER_NAME,
                    staticVoterAddressMap
                )
            );
        }

        Builder withStaticVoters(VoterSet staticVoters) {
            startingVoters = staticVoters;
            isStartingVotersStatic = true;
            kraftVersion = KRaftVersion.KRAFT_VERSION_0;

            return this;
        }

        Builder withBootstrapSnapshot(Optional<VoterSet> voters) {
            startingVoters = voters.orElse(VoterSet.empty());
            isStartingVotersStatic = false;

            if (voters.isPresent()) {
                kraftVersion = KRaftVersion.KRAFT_VERSION_1;

                RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
                    .setRawSnapshotWriter(
                        log.createNewSnapshotUnchecked(Snapshots.BOOTSTRAP_SNAPSHOT_ID).get()
                    )
                    .setKraftVersion(kraftVersion)
                    .setVoterSet(voters);

                try (RecordsSnapshotWriter<String> writer = builder.build(SERDE)) {
                    writer.freeze();
                }
            } else {
                // Create an empty bootstrap snapshot if there is no voter set
                kraftVersion = KRaftVersion.KRAFT_VERSION_0;
                withEmptySnapshot(Snapshots.BOOTSTRAP_SNAPSHOT_ID);
            }

            return this;
        }

        Builder withLocalListeners(Endpoints localListeners) {
            this.localListeners = localListeners;
            return this;
        }

        public RaftClientTestContext build() throws IOException {
            Metrics metrics = new Metrics(time);
            MockNetworkChannel channel = new MockNetworkChannel();
            MockListener listener = new MockListener(localId);
            Map<Integer, InetSocketAddress> staticVoterAddressMap = Collections.emptyMap();
            if (isStartingVotersStatic) {
                staticVoterAddressMap = startingVoters
                    .voterNodes(startingVoters.voterIds().stream(), channel.listenerName())
                    .stream()
                    .collect(
                        Collectors.toMap(
                            Node::id,
                            node -> InetSocketAddress.createUnresolved(node.host(), node.port())
                        )
                    );
            }

            /*
             * Compute the local listeners if the test didn't override it.
             * Only potential voters/leader need to provide the local listeners.
             * If the local id is not set (must be observer), the local listener can be empty.
             */
            Endpoints localListeners = this.localListeners.isEmpty() ?
                localId.isPresent() ?
                    startingVoters.listeners(localId.getAsInt()) :
                    Endpoints.empty() :
                this.localListeners;

            Map<String, Integer> configMap = new HashMap<>();
            configMap.put(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
            configMap.put(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);
            configMap.put(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, electionTimeoutMs);
            configMap.put(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, ELECTION_BACKOFF_MAX_MS);
            configMap.put(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, FETCH_TIMEOUT_MS);
            configMap.put(QuorumConfig.QUORUM_LINGER_MS_CONFIG, appendLingerMs);
            QuorumConfig quorumConfig = new QuorumConfig(new AbstractConfig(QuorumConfig.CONFIG_DEF, configMap));

            List<InetSocketAddress> computedBootstrapServers = bootstrapServers.orElseGet(() -> {
                if (isStartingVotersStatic) {
                    return Collections.emptyList();
                } else {
                    return startingVoters
                        .voterNodes(startingVoters.voterIds().stream(), channel.listenerName())
                        .stream()
                        .map(node -> InetSocketAddress.createUnresolved(node.host(), node.port()))
                        .collect(Collectors.toList());
                }
            });

            KafkaRaftClient<String> client = new KafkaRaftClient<>(
                localId,
                localDirectoryId,
                SERDE,
                channel,
                messageQueue,
                log,
                memoryPool,
                time,
                new MockExpirationService(time),
                FETCH_MAX_WAIT_MS,
                alwaysFlush,
                clusterId,
                computedBootstrapServers,
                localListeners,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                logContext,
                random,
                quorumConfig
            );

            client.register(listener);
            client.initialize(
                staticVoterAddressMap,
                quorumStateStore,
                metrics
            );

            RaftClientTestContext context = new RaftClientTestContext(
                clusterId,
                localId,
                localDirectoryId,
                kraftVersion,
                client,
                log,
                channel,
                messageQueue,
                time,
                quorumStateStore,
                startingVoters,
                IntStream
                    .iterate(-2, id -> id - 1)
                    .limit(bootstrapServers.map(List::size).orElse(0))
                    .boxed()
                    .collect(Collectors.toSet()),
                kip853Rpc,
                alwaysFlush,
                metrics,
                listener
            );

            context.electionTimeoutMs = electionTimeoutMs;
            context.requestTimeoutMs = requestTimeoutMs;
            context.appendLingerMs = appendLingerMs;

            return context;
        }
    }

    @SuppressWarnings("ParameterNumber")
    private RaftClientTestContext(
        String clusterId,
        OptionalInt localId,
        Uuid localDirectoryId,
        KRaftVersion kraftVersion,
        KafkaRaftClient<String> client,
        MockLog log,
        MockNetworkChannel channel,
        MockMessageQueue messageQueue,
        MockTime time,
        QuorumStateStore quorumStateStore,
        VoterSet startingVoters,
        Set<Integer> bootstrapIds,
        boolean kip853Rpc,
        boolean alwaysFlush,
        Metrics metrics,
        MockListener listener
    ) {
        this.clusterId = clusterId;
        this.localId = localId;
        this.localDirectoryId = localDirectoryId;
        this.kraftVersion = kraftVersion;
        this.client = client;
        this.log = log;
        this.channel = channel;
        this.messageQueue = messageQueue;
        this.time = time;
        this.quorumStateStore = quorumStateStore;
        this.startingVoters = startingVoters;
        this.bootstrapIds = bootstrapIds;
        this.kip853Rpc = kip853Rpc;
        this.alwaysFlush = alwaysFlush;
        this.metrics = metrics;
        this.listener = listener;
    }

    int electionTimeoutMs() {
        return electionTimeoutMs;
    }

    int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    int appendLingerMs() {
        return appendLingerMs;
    }

    MemoryRecords buildBatch(
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        return buildBatch(time.milliseconds(), baseOffset, epoch, records);
    }

    static MemoryRecords buildBatch(
        long timestamp,
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        BatchBuilder<String> builder = new BatchBuilder<>(
            buffer,
            Builder.SERDE,
            Compression.NONE,
            baseOffset,
            timestamp,
            epoch,
            512
        );

        for (String record : records) {
            builder.appendRecord(record, null);
        }

        return builder.build();
    }

    static RaftClientTestContext initializeAsLeader(int localId, Set<Integer> voters, int epoch) throws Exception {
        if (epoch <= 0) {
            throw new IllegalArgumentException("Cannot become leader in epoch " + epoch);
        }

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch - 1)
            .build();

        context.assertUnknownLeader(epoch - 1);
        context.becomeLeader();
        return context;
    }

    public void becomeLeader() throws Exception {
        int currentEpoch = currentEpoch();
        time.sleep(electionTimeoutMs * 2L);
        expectAndGrantVotes(currentEpoch + 1);
        expectBeginEpoch(currentEpoch + 1);
    }

    public OptionalInt currentLeader() {
        return currentLeaderAndEpoch().leaderId();
    }

    public int currentEpoch() {
        return currentLeaderAndEpoch().epoch();
    }

    LeaderAndEpoch currentLeaderAndEpoch() {
        ElectionState election = quorumStateStore.readElectionState().get();
        return new LeaderAndEpoch(election.optionalLeaderId(), election.epoch());
    }

    void expectAndGrantVotes(int epoch) throws Exception {
        pollUntilRequest();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch,
            log.lastFetchedEpoch(), log.endOffset().offset());

        for (RaftRequest.Outbound request : voteRequests) {
            VoteResponseData voteResponse = voteResponse(true, OptionalInt.empty(), epoch);
            deliverResponse(request.correlationId(), request.destination(), voteResponse);
        }

        client.poll();
        assertElectedLeader(epoch, localIdOrThrow());
    }

    private int localIdOrThrow() {
        return localId.orElseThrow(() -> new AssertionError("Required local id is not defined"));
    }

    public ReplicaKey localReplicaKey() {
        return kip853Rpc ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(localIdOrThrow(), ReplicaKey.NO_DIRECTORY_ID);
    }

    private void expectBeginEpoch(int epoch) throws Exception {
        pollUntilRequest();
        for (RaftRequest.Outbound request : collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, localIdOrThrow());
            deliverResponse(request.correlationId(), request.destination(), beginEpochResponse);
            client.poll();
        }
    }

    public void pollUntil(TestCondition condition) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return condition.conditionMet();
        }, 5000, "Condition failed to be satisfied before timeout");
    }

    void pollUntilResponse() throws InterruptedException {
        pollUntil(() -> !sentResponses.isEmpty());
    }

    void pollUntilRequest() throws InterruptedException {
        pollUntil(channel::hasSentRequests);
    }

    void assertVotedCandidate(int epoch, int candidateId) {
        assertEquals(
            ElectionState.withVotedCandidate(
                epoch,
                ReplicaKey.of(candidateId, ReplicaKey.NO_DIRECTORY_ID),
                startingVoters.voterIds()
            ),
            quorumStateStore.readElectionState().get()
        );
    }

    public void assertElectedLeader(int epoch, int leaderId) {
        Set<Integer> voters = kraftVersion.isReconfigSupported() ?
                Collections.emptySet() : startingVoters.voterIds();
        assertEquals(
            ElectionState.withElectedLeader(epoch, leaderId, voters),
            quorumStateStore.readElectionState().get()
        );
    }

    void assertUnknownLeader(int epoch) {
        assertEquals(
            ElectionState.withUnknownLeader(epoch, startingVoters.voterIds()),
            quorumStateStore.readElectionState().get()
        );
    }

    void assertResignedLeader(int epoch, int leaderId) {
        assertTrue(client.quorum().isResigned());
        assertEquals(
            ElectionState.withElectedLeader(epoch, leaderId, startingVoters.voterIds()),
            quorumStateStore.readElectionState().get()
        );
    }

    DescribeQuorumResponseData collectDescribeQuorumResponse() {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.DESCRIBE_QUORUM);
        assertEquals(1, sentMessages.size());
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertInstanceOf(
            DescribeQuorumResponseData.class,
            raftMessage.data(),
            "Unexpected request type " + raftMessage.data());
        return (DescribeQuorumResponseData) raftMessage.data();
    }

    void assertSentDescribeQuorumResponse(
        int leaderId,
        int leaderEpoch,
        long highWatermark,
        List<ReplicaState> voterStates,
        List<ReplicaState> observerStates
    ) {
        assertSentDescribeQuorumResponse(Errors.NONE, leaderId, leaderEpoch, highWatermark, voterStates, observerStates);
    }

    void assertSentDescribeQuorumResponse(
        Errors error,
        int leaderId,
        int leaderEpoch,
        long highWatermark,
        List<ReplicaState> voterStates,
        List<ReplicaState> observerStates
    ) {
        DescribeQuorumResponseData response = collectDescribeQuorumResponse();

        DescribeQuorumResponseData.PartitionData partitionData = new DescribeQuorumResponseData.PartitionData()
            .setErrorCode(error.code())
            .setLeaderId(leaderId)
            .setLeaderEpoch(leaderEpoch)
            .setHighWatermark(highWatermark)
            .setCurrentVoters(voterStates)
            .setObservers(observerStates);

        if (!error.equals(Errors.NONE)) {
            partitionData.setErrorMessage(error.message());
        }

        DescribeQuorumResponseData.NodeCollection nodes = new DescribeQuorumResponseData.NodeCollection(0);
        if (describeQuorumRpcVersion() >= 2) {
            nodes = new DescribeQuorumResponseData.NodeCollection(voterStates.size());
            for (ReplicaState voterState : voterStates) {
                nodes.add(new DescribeQuorumResponseData.Node()
                    .setNodeId(voterState.replicaId())
                    .setListeners(startingVoters.listeners(voterState.replicaId()).toDescribeQuorumResponseListeners()));
            }
        }

        DescribeQuorumResponseData expectedResponse = DescribeQuorumResponse.singletonResponse(
            metadataPartition,
            partitionData,
            nodes
        );

        List<ReplicaState> sortedVoters = response
            .topics()
            .get(0)
            .partitions()
            .get(0)
            .currentVoters()
            .stream()
            .sorted(Comparator.comparingInt(ReplicaState::replicaId))
            .collect(Collectors.toList());
        response.topics().get(0).partitions().get(0).setCurrentVoters(sortedVoters);
        response.nodes().sort(Comparator.comparingInt(DescribeQuorumResponseData.Node::nodeId));

        assertEquals(expectedResponse, response);
    }

    RaftRequest.Outbound assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset, int numVoteReceivers) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(numVoteReceivers, voteRequests.size());
        return voteRequests.iterator().next();
    }

    void assertSentVoteResponse(Errors error) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(VoteResponseData.class, raftMessage.data());
        VoteResponseData response = (VoteResponseData) raftMessage.data();

        assertEquals(error, Errors.forCode(response.errorCode()));
    }

    void assertSentVoteResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId,
        boolean voteGranted
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(VoteResponseData.class, raftMessage.data());
        VoteResponseData response = (VoteResponseData) raftMessage.data();
        assertTrue(hasValidTopicPartition(response, metadataPartition));

        VoteResponseData.PartitionData partitionResponse = response.topics().get(0).partitions().get(0);

        String leaderIdDebugLog = "Leader Id: " + leaderId +
            " Partition response leader Id: " + partitionResponse.leaderId();
        assertEquals(voteGranted, partitionResponse.voteGranted());
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId(), leaderIdDebugLog);
        assertEquals(epoch, partitionResponse.leaderEpoch());

        if (kip853Rpc && leaderId.isPresent()) {
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId.getAsInt());
            Endpoints responseEndpoints = Endpoints.fromVoteResponse(
                channel.listenerName(),
                leaderId.getAsInt(),
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
    }

    List<RaftRequest.Outbound> collectVoteRequests(
        int epoch,
        int lastEpoch,
        long lastEpochOffset
    ) {
        List<RaftRequest.Outbound> voteRequests = new ArrayList<>();
        for (RaftRequest.Outbound raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof VoteRequestData) {
                VoteRequestData request = (VoteRequestData) raftMessage.data();
                VoteRequestData.PartitionData partitionRequest = unwrap(request);

                assertEquals(epoch, partitionRequest.replicaEpoch());
                assertEquals(localIdOrThrow(), partitionRequest.replicaId());
                assertEquals(lastEpoch, partitionRequest.lastOffsetEpoch());
                assertEquals(lastEpochOffset, partitionRequest.lastOffset());
                voteRequests.add(raftMessage);
            }
        }
        return voteRequests;
    }

    private ApiMessage roundTripApiMessage(ApiMessage message, short version) {
        ObjectSerializationCache cache =  new ObjectSerializationCache();
        ByteArrayOutputStream  buffer = new ByteArrayOutputStream(message.size(cache, version));

        // Encode the message to a byte array with the given version
        DataOutputStreamWritable writer = new DataOutputStreamWritable(new DataOutputStream(buffer));
        message.write(writer, cache, version);

        // Decode the message from the byte array
        ByteBufferAccessor reader = new ByteBufferAccessor(ByteBuffer.wrap(buffer.toByteArray()));
        message.read(reader, version);

        return message;
    }

    void deliverRequest(ApiMessage request) {
        short version = raftRequestVersion(request);
        deliverRequest(request, version);
    }

    void deliverRequest(ApiMessage request, short version) {
        ApiMessage versionedRequest = roundTripApiMessage(request, version);
        RaftRequest.Inbound inboundRequest = new RaftRequest.Inbound(
            channel.listenerName(),
            channel.newCorrelationId(),
            version,
            versionedRequest,
            time.milliseconds()
        );
        inboundRequest.completion.whenComplete((response, exception) -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            } else {
                sentResponses.add(response);
            }
        });
        client.handle(inboundRequest);
    }

    void deliverResponse(int correlationId, Node source, ApiMessage response) {
        short version = raftResponseVersion(response);
        ApiMessage versionedResponse = roundTripApiMessage(response, version);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, versionedResponse, source));
    }

    List<RaftRequest.Outbound> assertSentBeginQuorumEpochRequest(int epoch, Set<Integer> destinationIds) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(destinationIds.size(), requests.size());
        assertEquals(destinationIds, requests.stream().map(r -> r.destination().id()).collect(Collectors.toSet()));

        return requests;
    }

    private List<RaftResponse.Outbound> drainSentResponses(
        ApiKeys apiKey
    ) {
        List<RaftResponse.Outbound> res = new ArrayList<>();
        Iterator<RaftResponse.Outbound> iterator = sentResponses.iterator();
        while (iterator.hasNext()) {
            RaftResponse.Outbound response = iterator.next();
            if (response.data().apiKey() == apiKey.id) {
                res.add(response);
                iterator.remove();
            }
        }
        return res;
    }

    void assertSentBeginQuorumEpochResponse(
        Errors responseError
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(BeginQuorumEpochResponseData.class, raftMessage.data());
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));

        if (!response.topics().isEmpty()) {
            BeginQuorumEpochResponseData.PartitionData partitionResponse = response
                .topics()
                .get(0)
                .partitions()
                .get(0);
            if (kip853Rpc && partitionResponse.leaderId() >= 0) {
                int leaderId = partitionResponse.leaderId();
                Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
                Endpoints responseEndpoints = Endpoints.fromBeginQuorumEpochResponse(
                    channel.listenerName(),
                    leaderId,
                    response.nodeEndpoints()
                );
                assertEquals(expectedLeaderEndpoints, responseEndpoints);
            }
        }
    }

    void assertSentBeginQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(BeginQuorumEpochResponseData.class, raftMessage.data());
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        BeginQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(
            partitionError,
            Errors.forCode(partitionResponse.errorCode()),
            "Leader Id: " + leaderId +
            " Partition response leader Id: " + partitionResponse.leaderId()
        );

        if (kip853Rpc && leaderId.isPresent()) {
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId.getAsInt());
            Endpoints responseEndpoints = Endpoints.fromBeginQuorumEpochResponse(
                channel.listenerName(),
                leaderId.getAsInt(),
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
    }

    RaftRequest.Outbound assertSentEndQuorumEpochRequest(int epoch, int destinationId) {
        List<RaftRequest.Outbound> endQuorumRequests = collectEndQuorumRequests(
            epoch,
            Collections.singleton(destinationId),
            Optional.empty()
        );
        assertEquals(1, endQuorumRequests.size());
        return endQuorumRequests.get(0);
    }

    void assertSentEndQuorumEpochResponse(
        Errors responseError
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(EndQuorumEpochResponseData.class, raftMessage.data());
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));

        if (!response.topics().isEmpty()) {
            EndQuorumEpochResponseData.PartitionData partitionResponse = response
                .topics()
                .get(0)
                .partitions()
                .get(0);
            if (kip853Rpc && partitionResponse.leaderId() >= 0) {
                int leaderId = partitionResponse.leaderId();
                Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
                Endpoints responseEndpoints = Endpoints.fromEndQuorumEpochResponse(
                    channel.listenerName(),
                    leaderId,
                    response.nodeEndpoints()
                );
                assertEquals(expectedLeaderEndpoints, responseEndpoints);
            }
        }
    }

    void assertSentEndQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertInstanceOf(EndQuorumEpochResponseData.class, raftMessage.data());
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        EndQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(partitionError, Errors.forCode(partitionResponse.errorCode()));

        if (kip853Rpc && leaderId.isPresent()) {
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId.getAsInt());
            Endpoints responseEndpoints = Endpoints.fromEndQuorumEpochResponse(
                channel.listenerName(),
                leaderId.getAsInt(),
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
    }

    RaftRequest.Outbound assertSentFetchRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.FETCH));
        assertEquals(1, sentRequests.size());
        return sentRequests.get(0);
    }

    RaftRequest.Outbound assertSentFetchRequest(
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        List<RaftRequest.Outbound> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());

        RaftRequest.Outbound raftRequest = sentMessages.get(0);
        assertFetchRequestData(raftRequest, epoch, fetchOffset, lastFetchedEpoch);
        return raftRequest;
    }

    FetchResponseData.PartitionData assertSentFetchPartitionResponse() {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1, sentMessages.size(), "Found unexpected sent messages " + sentMessages);
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data().apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        assertEquals(1, response.responses().size());
        assertEquals(metadataPartition.topic(), response.responses().get(0).topic());
        assertEquals(1, response.responses().get(0).partitions().size());

        FetchResponseData.PartitionData partitionResponse = response.responses().get(0).partitions().get(0);
        if (kip853Rpc && partitionResponse.currentLeader().leaderId() >= 0) {
            int leaderId = partitionResponse.currentLeader().leaderId();
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
            Endpoints responseEndpoints = Endpoints.fromFetchResponse(
                channel.listenerName(),
                leaderId,
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }

        return partitionResponse;
    }

    void assertSentFetchPartitionResponse(Errors topLevelError) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1, sentMessages.size(), "Found unexpected sent messages " + sentMessages);
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data().apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(topLevelError, Errors.forCode(response.errorCode()));
    }


    MemoryRecords assertSentFetchPartitionResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        FetchResponseData.PartitionData partitionResponse = assertSentFetchPartitionResponse();
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.currentLeader().leaderId());
        assertEquals(-1, partitionResponse.divergingEpoch().endOffset());
        assertEquals(-1, partitionResponse.divergingEpoch().epoch());
        assertEquals(-1, partitionResponse.snapshotId().endOffset());
        assertEquals(-1, partitionResponse.snapshotId().epoch());

        return (MemoryRecords) partitionResponse.records();
    }

    MemoryRecords assertSentFetchPartitionResponse(
        long highWatermark,
        int leaderEpoch
    ) {
        FetchResponseData.PartitionData partitionResponse = assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderEpoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(highWatermark, partitionResponse.highWatermark());
        assertEquals(-1, partitionResponse.divergingEpoch().endOffset());
        assertEquals(-1, partitionResponse.divergingEpoch().epoch());
        assertEquals(-1, partitionResponse.snapshotId().endOffset());
        assertEquals(-1, partitionResponse.snapshotId().epoch());
        return (MemoryRecords) partitionResponse.records();
    }

    RaftRequest.Outbound assertSentFetchSnapshotRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.FETCH_SNAPSHOT));
        assertEquals(1, sentRequests.size());

        return sentRequests.get(0);
    }

    void assertSentFetchSnapshotResponse(Errors responseError) {
        assertSentFetchSnapshotResponse(responseError, metadataPartition);
    }

    Optional<FetchSnapshotResponseData.PartitionSnapshot> assertSentFetchSnapshotResponse(
        TopicPartition topicPartition
    ) {
        return assertSentFetchSnapshotResponse(Errors.NONE, topicPartition);
    }

    Optional<FetchSnapshotResponseData.PartitionSnapshot> assertSentFetchSnapshotResponse(
        Errors responseError,
        TopicPartition topicPartition
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH_SNAPSHOT);
        assertEquals(1, sentMessages.size());

        RaftMessage message = sentMessages.get(0);
        assertInstanceOf(FetchSnapshotResponseData.class, message.data());

        FetchSnapshotResponseData response = (FetchSnapshotResponseData) message.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));

        Optional<FetchSnapshotResponseData.PartitionSnapshot> result =
            FetchSnapshotResponse.forTopicPartition(response, topicPartition);

        if (result.isPresent() && kip853Rpc && result.get().currentLeader().leaderId() >= 0) {
            int leaderId = result.get().currentLeader().leaderId();
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(leaderId);
            Endpoints responseEndpoints = Endpoints.fromFetchSnapshotResponse(
                channel.listenerName(),
                leaderId,
                response.nodeEndpoints()
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }

        return result;
    }

    RaftRequest.Outbound assertSentApiVersionsRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.API_VERSIONS));
        assertEquals(1, sentRequests.size());

        return sentRequests.get(0);
    }

    AddRaftVoterResponseData assertSentAddVoterResponse(Errors error) {
        List<RaftResponse.Outbound> sentResponses = drainSentResponses(ApiKeys.ADD_RAFT_VOTER);
        assertEquals(1, sentResponses.size());

        RaftResponse.Outbound response = sentResponses.get(0);
        assertInstanceOf(AddRaftVoterResponseData.class, response.data());

        AddRaftVoterResponseData addVoterResponse = (AddRaftVoterResponseData) response.data();
        assertEquals(error, Errors.forCode(addVoterResponse.errorCode()));

        return addVoterResponse;
    }

    RemoveRaftVoterResponseData assertSentRemoveVoterResponse(Errors error) {
        List<RaftResponse.Outbound> sentResponses = drainSentResponses(ApiKeys.REMOVE_RAFT_VOTER);
        assertEquals(1, sentResponses.size());

        RaftResponse.Outbound response = sentResponses.get(0);
        assertInstanceOf(RemoveRaftVoterResponseData.class, response.data());

        RemoveRaftVoterResponseData removeVoterResponse = (RemoveRaftVoterResponseData) response.data();
        assertEquals(error, Errors.forCode(removeVoterResponse.errorCode()));

        return removeVoterResponse;
    }

    RaftRequest.Outbound assertSentUpdateVoterRequest(
        ReplicaKey replicaKey,
        int epoch,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.UPDATE_RAFT_VOTER));
        assertEquals(1, sentRequests.size());

        RaftRequest.Outbound request = sentRequests.get(0);
        assertInstanceOf(UpdateRaftVoterRequestData.class, request.data());

        UpdateRaftVoterRequestData updateVoterRequest = (UpdateRaftVoterRequestData) request.data();
        assertEquals(clusterId, updateVoterRequest.clusterId());
        assertEquals(epoch, updateVoterRequest.currentLeaderEpoch());
        assertEquals(replicaKey.id(), updateVoterRequest.voterId());
        assertEquals(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID), updateVoterRequest.voterDirectoryId());
        assertEquals(endpoints, Endpoints.fromUpdateVoterRequest(updateVoterRequest.listeners()));
        assertEquals(supportedVersions.min(), updateVoterRequest.kRaftVersionFeature().minSupportedVersion());
        assertEquals(supportedVersions.max(), updateVoterRequest.kRaftVersionFeature().maxSupportedVersion());

        return request;
    }

    UpdateRaftVoterResponseData assertSentUpdateVoterResponse(
        Errors error,
        OptionalInt leaderId,
        int epoch
    ) {
        List<RaftResponse.Outbound> sentResponses = drainSentResponses(ApiKeys.UPDATE_RAFT_VOTER);
        assertEquals(1, sentResponses.size());

        RaftResponse.Outbound response = sentResponses.get(0);
        assertInstanceOf(UpdateRaftVoterResponseData.class, response.data());

        UpdateRaftVoterResponseData updateVoterResponse = (UpdateRaftVoterResponseData) response.data();
        assertEquals(error, Errors.forCode(updateVoterResponse.errorCode()));
        assertEquals(leaderId.orElse(-1), updateVoterResponse.currentLeader().leaderId());
        assertEquals(epoch, updateVoterResponse.currentLeader().leaderEpoch());

        if (updateVoterResponse.currentLeader().leaderId() >= 0) {
            int id = updateVoterResponse.currentLeader().leaderId();
            Endpoints expectedLeaderEndpoints = startingVoters.listeners(id);
            Endpoints responseEndpoints = Endpoints.fromInetSocketAddresses(
                Collections.singletonMap(
                    channel.listenerName(),
                    InetSocketAddress.createUnresolved(
                        updateVoterResponse.currentLeader().host(),
                        updateVoterResponse.currentLeader().port()
                    )
                )
            );
            assertEquals(expectedLeaderEndpoints, responseEndpoints);
        }
        return updateVoterResponse;
    }

    List<RaftRequest.Outbound> collectEndQuorumRequests(
        int epoch,
        Set<Integer> destinationIdSet,
        Optional<List<ReplicaKey>> preferredCandidates
    ) {
        List<RaftRequest.Outbound> endQuorumRequests = new ArrayList<>();
        Set<Integer> collectedDestinationIdSet = new HashSet<>();

        Optional<List<Integer>> preferredSuccessorsOpt = preferredCandidates
            .map(list -> list.stream().map(ReplicaKey::id).collect(Collectors.toList()));

        for (RaftRequest.Outbound raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof EndQuorumEpochRequestData) {
                EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) raftMessage.data();

                EndQuorumEpochRequestData.PartitionData partitionRequest =
                    request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.leaderEpoch());
                assertEquals(localIdOrThrow(), partitionRequest.leaderId());
                preferredSuccessorsOpt.ifPresent(preferredSuccessors ->
                    assertEquals(preferredSuccessors, partitionRequest.preferredSuccessors())
                );
                preferredCandidates.ifPresent(preferred ->
                    assertEquals(
                        preferred,
                        partitionRequest
                            .preferredCandidates()
                            .stream()
                            .map(replica -> ReplicaKey.of(replica.candidateId(), replica.candidateDirectoryId()))
                            .collect(Collectors.toList())
                    )
                );

                collectedDestinationIdSet.add(raftMessage.destination().id());
                endQuorumRequests.add(raftMessage);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
    }

    void discoverLeaderAsObserver(
        int leaderId,
        int epoch
    ) throws Exception {
        pollUntilRequest();
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        int destinationId = fetchRequest.destination().id();
        assertTrue(
            startingVoters.voterIds().contains(destinationId) || bootstrapIds.contains(destinationId),
            String.format("id %d is not in sets %s or %s", destinationId, startingVoters, bootstrapIds)
        );
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
        );
        client.poll();
        assertElectedLeader(epoch, leaderId);
    }

    List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        for (RaftRequest.Outbound raftRequest : channel.drainSentRequests(Optional.of(ApiKeys.BEGIN_QUORUM_EPOCH))) {
            assertInstanceOf(BeginQuorumEpochRequestData.class, raftRequest.data());
            assertNotEquals(localIdOrThrow(), raftRequest.destination().id());
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();

            BeginQuorumEpochRequestData.PartitionData partitionRequest =
                request.topics().get(0).partitions().get(0);

            assertEquals(epoch, partitionRequest.leaderEpoch());
            assertEquals(localIdOrThrow(), partitionRequest.leaderId());
            requests.add(raftRequest);
        }
        return requests;
    }

    public static InetSocketAddress mockAddress(int id) {
        return InetSocketAddress.createUnresolved("localhost", 9990 + id);
    }

    EndQuorumEpochResponseData endEpochResponse(
        int epoch,
        OptionalInt leaderId
    ) {
        return RaftUtil.singletonEndQuorumEpochResponse(
            channel.listenerName(),
            endQuorumEpochRpcVersion(),
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            leaderId.isPresent() ? startingVoters.listeners(leaderId.getAsInt()) : Endpoints.empty()
        );
    }

    EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        int leaderId,
        List<ReplicaKey> preferredCandidates
    ) {
        return endEpochRequest(
            clusterId,
            epoch,
            leaderId,
            preferredCandidates
        );
    }

    EndQuorumEpochRequestData endEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        List<ReplicaKey> preferredCandidates
    ) {
        return RaftUtil.singletonEndQuorumEpochRequest(
            metadataPartition,
            clusterId,
            epoch,
            leaderId,
            preferredCandidates
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return beginEpochRequest(clusterId, epoch, leaderId);
    }

    BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId, Endpoints endpoints) {
        ReplicaKey localReplicaKey = kip853Rpc ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID);

        return beginEpochRequest(clusterId, epoch, leaderId, endpoints, localReplicaKey);
    }

    BeginQuorumEpochRequestData beginEpochRequest(String clusterId, int epoch, int leaderId) {
        ReplicaKey localReplicaKey = kip853Rpc ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID);

        return beginEpochRequest(clusterId, epoch, leaderId, localReplicaKey);
    }

    BeginQuorumEpochRequestData beginEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        ReplicaKey voterKey
    ) {
        return beginEpochRequest(
            clusterId,
            epoch,
            leaderId,
            startingVoters.listeners(leaderId),
            voterKey
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        Endpoints endpoints,
        ReplicaKey voterKey
    ) {
        return RaftUtil.singletonBeginQuorumEpochRequest(
            metadataPartition,
            clusterId,
            epoch,
            leaderId,
            endpoints,
            voterKey
        );
    }

    BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return RaftUtil.singletonBeginQuorumEpochResponse(
            channel.listenerName(),
            beginQuorumEpochRpcVersion(),
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId,
            startingVoters.listeners(leaderId)
        );
    }

    VoteRequestData voteRequest(
        int epoch,
        ReplicaKey candidateKey,
        int lastEpoch,
        long lastEpochOffset
    ) {
        return voteRequest(
            clusterId,
            epoch,
            candidateKey,
            lastEpoch,
            lastEpochOffset,
            false
        );
    }

    VoteRequestData preVoteRequest(
        int epoch,
        ReplicaKey candidateKey,
        int lastEpoch,
        long lastEpochOffset
    ) {
        return voteRequest(
            clusterId,
            epoch,
            candidateKey,
            lastEpoch,
            lastEpochOffset,
            true
        );
    }

    VoteRequestData voteRequest(
        String clusterId,
        int epoch,
        ReplicaKey candidateKey,
        int lastEpoch,
        long lastEpochOffset,
        boolean preVote
    ) {
        ReplicaKey localReplicaKey = kip853Rpc ?
            ReplicaKey.of(localIdOrThrow(), localDirectoryId) :
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID);

        return voteRequest(
            clusterId,
            epoch,
            candidateKey,
            localReplicaKey,
            lastEpoch,
            lastEpochOffset,
            preVote
        );
    }

    VoteRequestData voteRequest(
        String clusterId,
        int epoch,
        ReplicaKey candidateKey,
        ReplicaKey voterKey,
        int lastEpoch,
        long lastEpochOffset,
        boolean preVote
    ) {
        return RaftUtil.singletonVoteRequest(
                metadataPartition,
                clusterId,
                epoch,
                candidateKey,
                voterKey,
                lastEpoch,
                lastEpochOffset,
                preVote
        );
    }

    VoteResponseData voteResponse(boolean voteGranted, OptionalInt leaderId, int epoch) {
        return RaftUtil.singletonVoteResponse(
            channel.listenerName(),
            voteRpcVersion(),
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            voteGranted,
            leaderId.isPresent() ? startingVoters.listeners(leaderId.getAsInt()) : Endpoints.empty()
        );
    }

    private VoteRequestData.PartitionData unwrap(VoteRequestData voteRequest) {
        assertTrue(RaftUtil.hasValidTopicPartition(voteRequest, metadataPartition));
        return voteRequest.topics().get(0).partitions().get(0);
    }

    static void assertMatchingRecords(
        String[] expected,
        Records actual
    ) {
        List<Record> recordList = Utils.toList(actual.records());
        assertEquals(expected.length, recordList.size());
        for (int i = 0; i < expected.length; i++) {
            Record record = recordList.get(i);
            assertEquals(expected[i], Utils.utf8(record.value()),
                "Record at offset " + record.offset() + " does not match expected");
        }
    }

    static void verifyLeaderChangeMessage(
        int leaderId,
        List<Integer> voters,
        List<Integer> grantingVoters,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(recordKey));

        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(recordValue);
        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(
            voters
                .stream()
                .map(voterId -> new Voter().setVoterId(voterId))
                .collect(Collectors.toSet()),
            new HashSet<>(leaderChangeMessage.voters())
        );
        assertEquals(
            grantingVoters
                .stream()
                .map(voterId -> new Voter().setVoterId(voterId))
                .collect(Collectors.toSet()),
            new HashSet<>(leaderChangeMessage.grantingVoters())
        );
    }

    void assertFetchRequestData(
        RaftRequest.Outbound message,
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        assertInstanceOf(
            FetchRequestData.class,
            message.data(),
            "unexpected request type " + message.data());
        FetchRequestData request = (FetchRequestData) message.data();
        assertEquals(KafkaRaftClient.MAX_FETCH_SIZE_BYTES, request.maxBytes());
        assertEquals(fetchMaxWaitMs, request.maxWaitMs());

        assertEquals(1, request.topics().size());
        assertEquals(metadataPartition.topic(), request.topics().get(0).topic());
        assertEquals(1, request.topics().get(0).partitions().size());

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        assertEquals(epoch, fetchPartition.currentLeaderEpoch());
        assertEquals(fetchOffset, fetchPartition.fetchOffset());
        assertEquals(lastFetchedEpoch, fetchPartition.lastFetchedEpoch());
        assertEquals(localId.orElse(-1), request.replicaState().replicaId());

        // Assert that voters have flushed up to the fetch offset
        if ((localId.isPresent() && startingVoters.voterIds().contains(localId.getAsInt())) ||
            alwaysFlush
        ) {
            assertEquals(
                log.firstUnflushedOffset(),
                fetchOffset,
                String.format(
                    "expected voters have the fetch offset (%s) be the same as the unflushed offset (%s)",
                    log.firstUnflushedOffset(),
                    fetchOffset
                )
            );
        } else {
            assertFalse(log.flushedSinceLastChecked(), "KRaft client should not explicitly flush when it is an observer");
        }
    }

    FetchRequestData fetchRequest(
        int epoch,
        ReplicaKey replicaKey,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        return fetchRequest(
            epoch,
            clusterId,
            replicaKey,
            fetchOffset,
            lastFetchedEpoch,
            maxWaitTimeMs
        );
    }

    FetchRequestData fetchRequest(
        int epoch,
        String clusterId,
        ReplicaKey replicaKey,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        FetchRequestData request = RaftUtil.singletonFetchRequest(
            metadataPartition,
            metadataTopicId,
            fetchPartition -> {
                fetchPartition
                    .setCurrentLeaderEpoch(epoch)
                    .setLastFetchedEpoch(lastFetchedEpoch)
                    .setFetchOffset(fetchOffset);
                if (kip853Rpc) {
                    fetchPartition
                        .setReplicaDirectoryId(replicaKey.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID));
                }
            }
        );
        return request
            .setMaxWaitMs(maxWaitTimeMs)
            .setClusterId(clusterId)
            .setReplicaState(
                new FetchRequestData.ReplicaState().setReplicaId(replicaKey.id())
            );
    }

    FetchResponseData fetchResponse(
        int epoch,
        int leaderId,
        Records records,
        long highWatermark,
        Errors error
    ) {
        return RaftUtil.singletonFetchResponse(
            channel.listenerName(),
            fetchRpcVersion(),
            metadataPartition,
            metadataTopicId,
            Errors.NONE,
            leaderId,
            startingVoters.listeners(leaderId),
            partitionData -> {
                partitionData
                    .setRecords(records)
                    .setErrorCode(error.code())
                    .setHighWatermark(highWatermark);

                partitionData.currentLeader()
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId);
            }
        );
    }

    FetchResponseData divergingFetchResponse(
        int epoch,
        int leaderId,
        long divergingEpochEndOffset,
        int divergingEpoch,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(
            channel.listenerName(),
            fetchRpcVersion(),
            metadataPartition,
            metadataTopicId,
            Errors.NONE,
            leaderId,
            startingVoters.listeners(leaderId),
            partitionData -> {
                partitionData.setHighWatermark(highWatermark);

                partitionData.currentLeader()
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId);

                partitionData.divergingEpoch()
                    .setEpoch(divergingEpoch)
                    .setEndOffset(divergingEpochEndOffset);
            }
        );
    }

    FetchResponseData snapshotFetchResponse(
        int epoch,
        int leaderId,
        OffsetAndEpoch snapshotId,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(
            channel.listenerName(),
            fetchRpcVersion(),
            metadataPartition,
            metadataTopicId,
            Errors.NONE,
            leaderId,
            startingVoters.listeners(leaderId),
            partitionData -> {
                partitionData.setHighWatermark(highWatermark);

                partitionData.currentLeader()
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId);

                partitionData.snapshotId()
                    .setEpoch(snapshotId.epoch())
                    .setEndOffset(snapshotId.offset());
            }
        );
    }

    FetchSnapshotResponseData fetchSnapshotResponse(
        int leaderId,
        UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        return RaftUtil.singletonFetchSnapshotResponse(
            channel.listenerName(),
            fetchSnapshotRpcVersion(),
            metadataPartition,
            leaderId,
            startingVoters.listeners(leaderId),
            operator
        );
    }

    DescribeQuorumRequestData describeQuorumRequest() {
        return RaftUtil.singletonDescribeQuorumRequest(metadataPartition);
    }

    AddRaftVoterRequestData addVoterRequest(
        int timeoutMs,
        ReplicaKey voter,
        Endpoints endpoints
    ) {
        return addVoterRequest(
            clusterId,
            timeoutMs,
            voter,
            endpoints
        );
    }

    AddRaftVoterRequestData addVoterRequest(
        String clusterId,
        int timeoutMs,
        ReplicaKey voter,
        Endpoints endpoints
    ) {
        return RaftUtil.addVoterRequest(
            clusterId,
            timeoutMs,
            voter,
            endpoints
        );
    }

    RemoveRaftVoterRequestData removeVoterRequest(ReplicaKey voter) {
        return removeVoterRequest(clusterId, voter);
    }

    RemoveRaftVoterRequestData removeVoterRequest(String cluster, ReplicaKey voter) {
        return RaftUtil.removeVoterRequest(cluster, voter);
    }

    UpdateRaftVoterRequestData updateVoterRequest(
        ReplicaKey voter,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        return updateVoterRequest(clusterId, voter, currentEpoch(), supportedVersions, endpoints);
    }

    UpdateRaftVoterRequestData updateVoterRequest(
        String clusterId,
        ReplicaKey voter,
        int epoch,
        SupportedVersionRange supportedVersions,
        Endpoints endpoints
    ) {
        return RaftUtil.updateVoterRequest(clusterId, voter, epoch, supportedVersions, endpoints);
    }

    UpdateRaftVoterResponseData updateVoterResponse(
        Errors error,
        LeaderAndEpoch leaderAndEpoch
    ) {
        return RaftUtil.updateVoterResponse(
            error,
            channel.listenerName(),
            leaderAndEpoch,
            leaderAndEpoch.leaderId().isPresent() ?
                startingVoters.listeners(leaderAndEpoch.leaderId().getAsInt()) :
                Endpoints.empty()
        );
    }

    private short fetchRpcVersion() {
        if (kip853Rpc) {
            return 17;
        } else {
            return 16;
        }
    }

    private short fetchSnapshotRpcVersion() {
        if (kip853Rpc) {
            return 1;
        } else {
            return 0;
        }
    }

    short voteRpcVersion() {
        if (kip853Rpc) {
            return ApiKeys.VOTE.latestVersion();
        } else {
            return 0;
        }
    }

    private short beginQuorumEpochRpcVersion() {
        if (kip853Rpc) {
            return 1;
        } else {
            return 0;
        }
    }

    private short endQuorumEpochRpcVersion() {
        if (kip853Rpc) {
            return 1;
        } else {
            return 0;
        }
    }

    private short describeQuorumRpcVersion() {
        if (kip853Rpc) {
            return 2;
        } else {
            return 1;
        }
    }

    private short addVoterRpcVersion() {
        if (kip853Rpc) {
            return 0;
        } else {
            throw new IllegalStateException("Reconfiguration must be enabled by calling withKip853Rpc(true)");
        }
    }

    private short removeVoterRpcVersion() {
        if (kip853Rpc) {
            return 0;
        } else {
            throw new IllegalStateException("Reconfiguration must be enabled by calling withKip853Rpc(true)");
        }
    }

    private short updateVoterRpcVersion() {
        if (kip853Rpc) {
            return 0;
        } else {
            throw new IllegalStateException("Reconfiguration must be enabled by calling withKip853Rpc(true)");
        }
    }

    private short raftRequestVersion(ApiMessage request) {
        if (request instanceof FetchRequestData) {
            return fetchRpcVersion();
        } else if (request instanceof FetchSnapshotRequestData) {
            return fetchSnapshotRpcVersion();
        } else if (request instanceof VoteRequestData) {
            return voteRpcVersion();
        } else if (request instanceof BeginQuorumEpochRequestData) {
            return beginQuorumEpochRpcVersion();
        } else if (request instanceof EndQuorumEpochRequestData) {
            return endQuorumEpochRpcVersion();
        } else if (request instanceof DescribeQuorumRequestData) {
            return describeQuorumRpcVersion();
        } else if (request instanceof AddRaftVoterRequestData) {
            return addVoterRpcVersion();
        } else if (request instanceof RemoveRaftVoterRequestData) {
            return removeVoterRpcVersion();
        } else if (request instanceof UpdateRaftVoterRequestData) {
            return updateVoterRpcVersion();
        } else {
            throw new IllegalArgumentException(String.format("Request %s is not a raft request", request));
        }
    }

    private short raftResponseVersion(ApiMessage response) {
        if (response instanceof FetchResponseData) {
            return fetchRpcVersion();
        } else if (response instanceof FetchSnapshotResponseData) {
            return fetchSnapshotRpcVersion();
        } else if (response instanceof VoteResponseData) {
            return voteRpcVersion();
        } else if (response instanceof BeginQuorumEpochResponseData) {
            return beginQuorumEpochRpcVersion();
        } else if (response instanceof EndQuorumEpochResponseData) {
            return endQuorumEpochRpcVersion();
        } else if (response instanceof DescribeQuorumResponseData) {
            return describeQuorumRpcVersion();
        } else if (response instanceof AddRaftVoterResponseData) {
            return addVoterRpcVersion();
        } else if (response instanceof RemoveRaftVoterResponseData) {
            return removeVoterRpcVersion();
        } else if (response instanceof UpdateRaftVoterResponseData) {
            return updateVoterRpcVersion();
        } else if (response instanceof ApiVersionsResponseData) {
            return 4;
        } else {
            throw new IllegalArgumentException(String.format("Request %s is not a raft response", response));
        }
    }

    public void advanceLocalLeaderHighWatermarkToLogEndOffset() throws InterruptedException {
        assertEquals(localId, currentLeader());
        long localLogEndOffset = log.endOffset().offset();

        Iterable<ReplicaKey> followers = () -> startingVoters
            .voterKeys()
            .stream()
            .filter(voterKey -> voterKey.id() != localId.getAsInt())
            .iterator();

        // Send a request from every voter
        for (ReplicaKey follower : followers) {
            deliverRequest(
                fetchRequest(currentEpoch(), follower, localLogEndOffset, currentEpoch(), 0)
            );

            pollUntilResponse();
            assertSentFetchPartitionResponse(Errors.NONE, currentEpoch(), localId);
        }

        pollUntil(() -> OptionalLong.of(localLogEndOffset).equals(client.highWatermark()));
    }

    static class MockListener implements RaftClient.Listener<String> {
        private final List<Batch<String>> commits = new ArrayList<>();
        private final List<BatchReader<String>> savedBatches = new ArrayList<>();
        private final Map<Integer, Long> claimedEpochStartOffsets = new HashMap<>();
        private LeaderAndEpoch currentLeaderAndEpoch = LeaderAndEpoch.UNKNOWN;
        private final OptionalInt localId;
        private Optional<SnapshotReader<String>> snapshot = Optional.empty();
        private boolean readCommit = true;

        MockListener(OptionalInt localId) {
            this.localId = localId;
        }

        int numCommittedBatches() {
            return commits.size();
        }

        Long claimedEpochStartOffset(int epoch) {
            return claimedEpochStartOffsets.get(epoch);
        }

        LeaderAndEpoch currentLeaderAndEpoch() {
            return currentLeaderAndEpoch;
        }

        List<Batch<String>> committedBatches() {
            return commits;
        }

        Batch<String> lastCommit() {
            if (commits.isEmpty()) {
                return null;
            } else {
                return commits.get(commits.size() - 1);
            }
        }

        OptionalLong lastCommitOffset() {
            if (commits.isEmpty()) {
                return OptionalLong.empty();
            } else {
                return OptionalLong.of(commits.get(commits.size() - 1).lastOffset());
            }
        }

        Optional<VoterSet> lastCommittedVoterSet() {
            return commits.stream()
                .flatMap(batch -> batch.controlRecords().stream())
                .flatMap(controlRecord -> {
                    if (controlRecord.type() == ControlRecordType.KRAFT_VOTERS) {
                        return Stream.of((VotersRecord) controlRecord.message());
                    } else {
                        return Stream.empty();
                    }
                })
                .reduce((accumulated, current) -> current)
                .map(VoterSet::fromVotersRecord);
        }

        OptionalInt currentClaimedEpoch() {
            if (localId.isPresent() && currentLeaderAndEpoch.isLeader(localId.getAsInt())) {
                return OptionalInt.of(currentLeaderAndEpoch.epoch());
            } else {
                return OptionalInt.empty();
            }
        }

        List<String> commitWithLastOffset(long lastOffset) {
            return commits.stream()
                .filter(batch -> batch.lastOffset() == lastOffset)
                .findFirst()
                .map(Batch::records)
                .orElse(null);
        }

        Optional<SnapshotReader<String>> drainHandledSnapshot() {
            Optional<SnapshotReader<String>> temp = snapshot;
            snapshot = Optional.empty();
            return temp;
        }

        void updateReadCommit(boolean readCommit) {
            this.readCommit = readCommit;

            if (readCommit) {
                for (BatchReader<String> batch : savedBatches) {
                    readBatch(batch);
                }

                savedBatches.clear();
            }
        }

        void readBatch(BatchReader<String> reader) {
            try {
                while (reader.hasNext()) {
                    long nextOffset = lastCommitOffset().isPresent() ?
                        lastCommitOffset().getAsLong() + 1 : 0L;
                    Batch<String> batch = reader.next();
                    // We expect monotonic offsets, but not necessarily sequential
                    // offsets since control records will be filtered.
                    assertTrue(batch.baseOffset() >= nextOffset,
                        "Received non-monotonic commit " + batch +
                            ". We expected an offset at least as large as " + nextOffset);
                    commits.add(batch);
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leaderAndEpoch) {
            // We record the current committed offset as the claimed epoch's start
            // offset. This is useful to verify that the `handleLeaderChange` callback
            // was not received early on the leader.
            assertTrue(
                leaderAndEpoch.epoch() >= currentLeaderAndEpoch.epoch(),
                String.format("new epoch (%d) not >= than old epoch (%d)", leaderAndEpoch.epoch(), currentLeaderAndEpoch.epoch())
            );
            assertNotEquals(currentLeaderAndEpoch, leaderAndEpoch);
            this.currentLeaderAndEpoch = leaderAndEpoch;

            currentClaimedEpoch().ifPresent(claimedEpoch -> {
                long claimedEpochStartOffset = lastCommitOffset().isPresent() ?
                    lastCommitOffset().getAsLong() : 0L;
                this.claimedEpochStartOffsets.put(leaderAndEpoch.epoch(), claimedEpochStartOffset);
            });
        }

        @Override
        public void handleCommit(BatchReader<String> reader) {
            if (readCommit) {
                readBatch(reader);
            } else {
                savedBatches.add(reader);
            }
        }

        @Override
        public void handleLoadSnapshot(SnapshotReader<String> reader) {
            snapshot.ifPresent(snapshot -> assertDoesNotThrow(snapshot::close));
            commits.clear();
            savedBatches.clear();
            snapshot = Optional.of(reader);
        }
    }
}
