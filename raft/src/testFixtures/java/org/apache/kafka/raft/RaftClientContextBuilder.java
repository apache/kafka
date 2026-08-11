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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.Snapshots;

import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.raft.SharedRaftClientContext.RaftProtocol.KIP_853_PROTOCOL;

public final class RaftClientContextBuilder {
    static final int DEFAULT_ELECTION_TIMEOUT_MS = 10000;

    static final RecordSerde<String> SERDE = new StringSerde();
    static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);
    static final int ELECTION_BACKOFF_MAX_MS = 100;
    static final int FETCH_MAX_WAIT_MS = 0;
    // fetch timeout is usually larger than election timeout
    static final int FETCH_TIMEOUT_MS = 50000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
    static final int RETRY_BACKOFF_MS = 50;
    private static final int DEFAULT_APPEND_LINGER_MS = 0;

    private final MockMessageQueue messageQueue = new MockMessageQueue();
    private final MockTime time = new MockTime();
    private final MockQuorumStateStore quorumStateStore = new MockQuorumStateStore();
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
    private SharedRaftClientContext.RaftProtocol raftProtocol = SharedRaftClientContext.RaftProtocol.KIP_595_PROTOCOL;
    private boolean canBecomeVoter = false;
    private VoterSet startingVoters = VoterSet.empty();
    private Endpoints localListeners = Endpoints.empty();
    private boolean isStartingVotersStatic = false;
    private boolean autoJoin = false;
    private int fetchSnapshotMaxBytes = QuorumConfig.DEFAULT_QUORUM_FETCH_SNAPSHOT_MAX_BYTES;
    private int fetchMaxBytes = QuorumConfig.DEFAULT_QUORUM_FETCH_MAX_BYTES;

    public RaftClientContextBuilder(int localId, Set<Integer> staticVoters) {
        this(OptionalInt.of(localId), staticVoters);
    }

    public RaftClientContextBuilder(OptionalInt localId, Set<Integer> staticVoters) {
        this(localId, Uuid.randomUuid());

        withStaticVoters(staticVoters);
    }

    public RaftClientContextBuilder(int localId, Uuid localDirectoryId) {
        this(OptionalInt.of(localId), localDirectoryId);
    }

    public RaftClientContextBuilder(OptionalInt localId, Uuid localDirectoryId) {
        this.localId = localId;
        this.localDirectoryId = localDirectoryId;
    }

    RaftClientContextBuilder withElectedLeader(int epoch, int leaderId) {
        quorumStateStore.writeElectionState(
            ElectionState.withElectedLeader(epoch, leaderId, Optional.empty(), startingVoters.voterIds()),
            kraftVersion
        );
        return this;
    }

    RaftClientContextBuilder withUnknownLeader(int epoch) {
        quorumStateStore.writeElectionState(
            ElectionState.withUnknownLeader(epoch, startingVoters.voterIds()),
            kraftVersion
        );
        return this;
    }

    RaftClientContextBuilder withVotedCandidate(int epoch, ReplicaKey votedKey) {
        quorumStateStore.writeElectionState(
            ElectionState.withVotedCandidate(epoch, votedKey, startingVoters.voterIds()),
            kraftVersion
        );
        return this;
    }

    RaftClientContextBuilder updateRandom(Consumer<MockableRandom> consumer) {
        consumer.accept(random);
        return this;
    }

    RaftClientContextBuilder withMemoryPool(MemoryPool pool) {
        this.memoryPool = pool;
        return this;
    }

    RaftClientContextBuilder withAppendLingerMs(int appendLingerMs) {
        this.appendLingerMs = appendLingerMs;
        return this;
    }

    public RaftClientContextBuilder appendToLog(int epoch, List<String> records) {
        MemoryRecords batch = RaftClientTestContext.buildBatch(
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

    RaftClientContextBuilder withEmptySnapshot(OffsetAndEpoch snapshotId) {
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

    RaftClientContextBuilder deleteBeforeSnapshot(OffsetAndEpoch snapshotId) {
        if (snapshotId.offset() > log.highWatermark().offset()) {
            log.updateHighWatermark(new LogOffsetMetadata(snapshotId.offset()));
        }
        log.deleteBeforeSnapshot(snapshotId);

        return this;
    }

    RaftClientContextBuilder withElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
        return this;
    }

    RaftClientContextBuilder withRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
        return this;
    }

    RaftClientContextBuilder withBootstrapServers(Optional<List<InetSocketAddress>> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    // deprecated, use withRpc instead
    RaftClientContextBuilder withKip853Rpc(boolean withKip853Rpc) {
        if (withKip853Rpc) {
            this.raftProtocol = KIP_853_PROTOCOL;
        }
        return this;
    }

    RaftClientContextBuilder withRaftProtocol(SharedRaftClientContext.RaftProtocol raftProtocol) {
        this.raftProtocol = raftProtocol;
        return this;
    }

    RaftClientContextBuilder withCanBecomeVoter(boolean canBecomeVoter) {
        this.canBecomeVoter = canBecomeVoter;
        return this;
    }

    RaftClientContextBuilder withStartingVoters(VoterSet voters, KRaftVersion kraftVersion) {
        if (kraftVersion.isReconfigSupported()) {
            return withBootstrapSnapshot(Optional.of(voters));
        } else {
            return withStaticVoters(voters.voterIds());
        }
    }

    RaftClientContextBuilder withStaticVoters(Set<Integer> staticVoters) {
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

    RaftClientContextBuilder withStaticVoters(VoterSet staticVoters) {
        startingVoters = staticVoters;
        isStartingVotersStatic = true;
        kraftVersion = KRaftVersion.KRAFT_VERSION_0;

        return this;
    }

    RaftClientContextBuilder withBootstrapSnapshot(Optional<VoterSet> voters) {
        return withBootstrapSnapshotRecords(voters, List.of());
    }

    RaftClientContextBuilder withBootstrapSnapshotRecords(Optional<VoterSet> voters, List<String> records) {
        startingVoters = voters.orElse(VoterSet.empty());
        isStartingVotersStatic = false;

        if (voters.isPresent()) {
            kraftVersion = KRaftVersion.LATEST_PRODUCTION;

            RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
                .setRawSnapshotWriter(
                    log.createNewSnapshotUnchecked(Snapshots.BOOTSTRAP_SNAPSHOT_ID).get()
                )
                .setKraftVersion(kraftVersion)
                .setVoterSet(voters);

            try (RecordsSnapshotWriter<String> writer = builder.build(SERDE)) {
                if (!records.isEmpty()) {
                    writer.append(records);
                }
                writer.freeze();
            }
        } else {
            // Create an empty bootstrap snapshot if there is no voter set
            kraftVersion = KRaftVersion.KRAFT_VERSION_0;
            withEmptySnapshot(Snapshots.BOOTSTRAP_SNAPSHOT_ID);
        }

        return this;
    }

    RaftClientContextBuilder withLocalListeners(Endpoints localListeners) {
        this.localListeners = localListeners;
        return this;
    }

    RaftClientContextBuilder withAutoJoin(boolean autoJoin) {
        this.autoJoin = autoJoin;
        return this;
    }

    RaftClientContextBuilder withFetchSnapshotMaxBytes(int fetchSnapshotMaxSizeBytes) {
        this.fetchSnapshotMaxBytes = fetchSnapshotMaxSizeBytes;
        return this;
    }

    RaftClientContextBuilder withFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
        return this;
    }

    public RaftClientTestContext build() throws IOException {
        Metrics metrics = new Metrics(time);
        MockNetworkChannel channel = new MockNetworkChannel();
        ExternalKRaftMetrics externalKRaftMetrics = Mockito.mock(ExternalKRaftMetrics.class);
        RaftClientTestContext.MockListener listener = new RaftClientTestContext.MockListener(localId);
        KafkaRaftClient<String> client = buildClient(listener, channel, metrics, externalKRaftMetrics);

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
            bootstrapIds(),
            raftProtocol,
            canBecomeVoter,
            metrics,
            externalKRaftMetrics,
            listener,
            fetchMaxBytes
        );

        applyOverrides(context);
        context.requestTimeoutMs = requestTimeoutMs;
        context.appendLingerMs = appendLingerMs;
        return context;
    }

    private KafkaRaftClient<String> buildClient(
        RaftClient.Listener<String> registeredListener,
        MockNetworkChannel channel,
        Metrics metrics,
        ExternalKRaftMetrics externalKRaftMetrics
    ) {
        Map<Integer, InetSocketAddress> staticVoterAddressMap = Map.of();
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

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configMap.put(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);
        configMap.put(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, electionTimeoutMs);
        configMap.put(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, ELECTION_BACKOFF_MAX_MS);
        configMap.put(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, FETCH_TIMEOUT_MS);
        configMap.put(QuorumConfig.QUORUM_LINGER_MS_CONFIG, appendLingerMs);
        configMap.put(QuorumConfig.QUORUM_AUTO_JOIN_ENABLE_CONFIG, autoJoin);
        configMap.put(QuorumConfig.QUORUM_FETCH_SNAPSHOT_MAX_BYTES_CONFIG, fetchSnapshotMaxBytes);
        configMap.put(QuorumConfig.QUORUM_FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        QuorumConfig quorumConfig = new QuorumConfig(new AbstractConfig(QuorumConfig.CONFIG_DEF, configMap));

        List<InetSocketAddress> computedBootstrapServers = bootstrapServers.orElseGet(() -> {
            if (isStartingVotersStatic) {
                return List.of();
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
            canBecomeVoter,
            clusterId,
            computedBootstrapServers,
            localListeners,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            logContext,
            random,
            quorumConfig
        );

        client.register(registeredListener);
        client.initialize(
            staticVoterAddressMap,
            quorumStateStore,
            metrics,
            externalKRaftMetrics
        );

        return client;
    }

    private Set<Integer> bootstrapIds() {
        return IntStream
            .iterate(-2, id -> id - 1)
            .limit(bootstrapServers.map(List::size).orElse(0))
            .boxed()
            .collect(Collectors.toSet());
    }

    private void applyOverrides(SharedRaftClientContext context) {
        context.electionTimeoutMs = electionTimeoutMs;
    }

}
