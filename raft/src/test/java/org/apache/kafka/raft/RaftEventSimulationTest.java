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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.MockLog.LogBatch;
import org.apache.kafka.raft.MockLog.LogEntry;
import org.apache.kafka.raft.internals.BatchMemoryPool;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The simulation testing framework provides a way to verify quorum behavior under
 * different conditions. It is similar to system testing in that the test involves
 * independently executing nodes, but there are several important differences:
 *
 * 1. Simulation behavior is deterministic provided an initial random seed. This
 *    makes it easy to reproduce and debug test failures.
 * 2. The simulation uses an in-memory message router instead of a real network.
 *    Not only is this much cheaper and faster, it provides an easy way to create
 *    flaky network conditions or even network partitions without losing the
 *    simulation determinism.
 * 3. Similarly, persistent state is stored in memory. We can nevertheless simulate
 *    different kinds of failures, such as the loss of unflushed data after a hard
 *    node restart using {@link MockLog}.
 *
 * The framework uses a single event scheduler in order to provide deterministic
 * executions. Each test is setup as a specific scenario with a variable number of
 * voters and observers. Much like system tests, there is typically a warmup
 * period, followed by some cluster event (such as a node failure), and then some
 * logic to validate behavior after recovery.
 *
 * If any of the tests fail, the output will indicate the arguments that failed.
 * The easiest way to reproduce the failure for debugging is to create a separate
 * `@Test` case which invokes the `@Property` method with those arguments directly.
 * This ensures that logging output will only include output from a single
 * simulation execution.
 */
@Tag("integration")
public class RaftEventSimulationTest {
    private static final TopicPartition METADATA_PARTITION = new TopicPartition("@metadata", 0);
    private static final int ELECTION_TIMEOUT_MS = 1000;
    private static final int ELECTION_JITTER_MS = 100;
    private static final int FETCH_TIMEOUT_MS = 3000;
    private static final int RETRY_BACKOFF_MS = 50;
    private static final int REQUEST_TIMEOUT_MS = 3000;
    private static final int FETCH_MAX_WAIT_MS = 100;
    private static final int LINGER_MS = 0;

    @Property(tries = 100)
    void canElectInitialLeader(
        @ForAll int seed,
        @ForAll @IntRange(min = 1, max = 5) int numVoters,
        @ForAll @IntRange(min = 0, max = 5) int numObservers
    ) {
        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        MessageRouter router = new MessageRouter(cluster);
        EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 1);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(cluster::hasConsistentLeader);
        scheduler.runUntil(() -> cluster.allReachedHighWatermark(10));
    }

    @Property(tries = 100)
    void canElectNewLeaderAfterOldLeaderFailure(
        @ForAll int seed,
        @ForAll @IntRange(min = 3, max = 5) int numVoters,
        @ForAll @IntRange(min = 0, max = 5) int numObservers,
        @ForAll boolean isGracefulShutdown
    ) {
        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        MessageRouter router = new MessageRouter(cluster);
        EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

        // Seed the cluster with some data
        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 1);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(cluster::hasConsistentLeader);
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

        // Shutdown the leader and write some more data. We can verify the new leader has been elected
        // by verifying that the high watermark can still advance.
        int leaderId = cluster.latestLeader().orElseThrow(() ->
            new AssertionError("Failed to find current leader")
        );

        if (isGracefulShutdown) {
            cluster.shutdown(leaderId);
        } else {
            cluster.kill(leaderId);
        }

        scheduler.runUntil(() -> cluster.allReachedHighWatermark(20));
        long highWatermark = cluster.maxHighWatermarkReached();

        // Restart the node and verify it catches up
        cluster.start(leaderId);
        scheduler.runUntil(() -> cluster.allReachedHighWatermark(highWatermark + 10));
    }

    @Property(tries = 100)
    void canRecoverAfterAllNodesKilled(
        @ForAll int seed,
        @ForAll @IntRange(min = 1, max = 5) int numVoters,
        @ForAll @IntRange(min = 0, max = 5) int numObservers
    ) {
        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        MessageRouter router = new MessageRouter(cluster);
        EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

        // Seed the cluster with some data
        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 1);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(cluster::hasConsistentLeader);
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));
        long highWatermark = cluster.maxHighWatermarkReached();

        // We kill all of the nodes. Then we bring back a majority and verify that
        // they are able to elect a leader and continue making progress
        cluster.killAll();

        Iterator<Integer> nodeIdsIterator = cluster.nodes().iterator();
        for (int i = 0; i < cluster.majoritySize(); i++) {
            Integer nodeId = nodeIdsIterator.next();
            cluster.start(nodeId);
        }

        scheduler.runUntil(() -> cluster.allReachedHighWatermark(highWatermark + 10));
    }

    @Property(tries = 100)
    void canElectNewLeaderAfterOldLeaderPartitionedAway(
        @ForAll int seed,
        @ForAll @IntRange(min = 3, max = 5) int numVoters,
        @ForAll @IntRange(min = 0, max = 5) int numObservers
    ) {
        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        MessageRouter router = new MessageRouter(cluster);
        EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

        // Seed the cluster with some data
        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 2);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(cluster::hasConsistentLeader);
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

        // The leader gets partitioned off. We can verify the new leader has been elected
        // by writing some data and ensuring that it gets replicated
        int leaderId = cluster.latestLeader().orElseThrow(() ->
            new AssertionError("Failed to find current leader")
        );
        router.filter(leaderId, new DropAllTraffic());

        Set<Integer> nonPartitionedNodes = new HashSet<>(cluster.nodes());
        nonPartitionedNodes.remove(leaderId);

        scheduler.runUntil(() -> cluster.allReachedHighWatermark(20, nonPartitionedNodes));
    }

    @Property(tries = 100)
    void canMakeProgressIfMajorityIsReachable(
        @ForAll int seed,
        @ForAll @IntRange(min = 0, max = 3) int numObservers
    ) {
        int numVoters = 5;
        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        MessageRouter router = new MessageRouter(cluster);
        EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

        // Seed the cluster with some data
        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 2);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(cluster::hasConsistentLeader);
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

        // Partition the nodes into two sets. Nodes are reachable within each set,
        // but the two sets cannot communicate with each other. We should be able
        // to make progress even if an election is needed in the larger set.
        router.filter(0, new DropOutboundRequestsFrom(Utils.mkSet(2, 3, 4)));
        router.filter(1, new DropOutboundRequestsFrom(Utils.mkSet(2, 3, 4)));
        router.filter(2, new DropOutboundRequestsFrom(Utils.mkSet(0, 1)));
        router.filter(3, new DropOutboundRequestsFrom(Utils.mkSet(0, 1)));
        router.filter(4, new DropOutboundRequestsFrom(Utils.mkSet(0, 1)));

        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(20));

        long minorityHighWatermark = cluster.maxHighWatermarkReached(Utils.mkSet(0, 1));
        long majorityHighWatermark = cluster.maxHighWatermarkReached(Utils.mkSet(2, 3, 4));

        assertTrue(majorityHighWatermark > minorityHighWatermark);

        // Now restore the partition and verify everyone catches up
        router.filter(0, new PermitAllTraffic());
        router.filter(1, new PermitAllTraffic());
        router.filter(2, new PermitAllTraffic());
        router.filter(3, new PermitAllTraffic());
        router.filter(4, new PermitAllTraffic());

        scheduler.runUntil(() -> cluster.allReachedHighWatermark(30));
    }

    @Property(tries = 100)
    void canMakeProgressAfterBackToBackLeaderFailures(
        @ForAll int seed,
        @ForAll @IntRange(min = 3, max = 5) int numVoters,
        @ForAll @IntRange(min = 0, max = 5) int numObservers
    ) {
        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        MessageRouter router = new MessageRouter(cluster);
        EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

        // Seed the cluster with some data
        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 5);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(cluster::hasConsistentLeader);
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

        int leaderId = cluster.latestLeader().getAsInt();
        router.filter(leaderId, new DropAllTraffic());
        scheduler.runUntil(() -> cluster.latestLeader().isPresent() && cluster.latestLeader().getAsInt() != leaderId);

        // As soon as we have a new leader, restore traffic to the old leader and partition the new leader
        int newLeaderId = cluster.latestLeader().getAsInt();
        router.filter(leaderId, new PermitAllTraffic());
        router.filter(newLeaderId, new DropAllTraffic());

        // Verify now that we can make progress
        long targetHighWatermark = cluster.maxHighWatermarkReached() + 10;
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(targetHighWatermark));
    }

    @Property(tries = 100)
    void canRecoverFromSingleNodeCommittedDataLoss(
        @ForAll int seed,
        @ForAll @IntRange(min = 3, max = 5) int numVoters,
        @ForAll @IntRange(min = 0, max = 2) int numObservers
    ) {
        // We run this test without the `MonotonicEpoch` and `MajorityReachedHighWatermark`
        // invariants since the loss of committed data on one node can violate them.

        Random random = new Random(seed);
        Cluster cluster = new Cluster(numVoters, numObservers, random);
        EventScheduler scheduler = new EventScheduler(cluster.random, cluster.time);
        scheduler.addInvariant(new MonotonicHighWatermark(cluster));
        scheduler.addInvariant(new SingleLeader(cluster));
        scheduler.addValidation(new ConsistentCommittedData(cluster));

        MessageRouter router = new MessageRouter(cluster);

        cluster.startAll();
        schedulePolling(scheduler, cluster, 3, 5);
        scheduler.schedule(router::deliverAll, 0, 2, 5);
        scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

        RaftNode node = cluster.randomRunning().orElseThrow(() ->
            new AssertionError("Failed to find running node")
        );

        // Kill a random node and drop all of its persistent state. The Raft
        // protocol guarantees should still ensure we lose no committed data
        // as long as a new leader is elected before the failed node is restarted.
        cluster.killAndDeletePersistentState(node.nodeId);
        scheduler.runUntil(() -> !cluster.hasLeader(node.nodeId) && cluster.hasConsistentLeader());

        // Now restart the failed node and ensure that it recovers.
        long highWatermarkBeforeRestart = cluster.maxHighWatermarkReached();
        cluster.start(node.nodeId);
        scheduler.runUntil(() -> cluster.allReachedHighWatermark(highWatermarkBeforeRestart + 10));
    }

    private EventScheduler schedulerWithDefaultInvariants(Cluster cluster) {
        EventScheduler scheduler = new EventScheduler(cluster.random, cluster.time);
        scheduler.addInvariant(new MonotonicHighWatermark(cluster));
        scheduler.addInvariant(new MonotonicEpoch(cluster));
        scheduler.addInvariant(new MajorityReachedHighWatermark(cluster));
        scheduler.addInvariant(new SingleLeader(cluster));
        scheduler.addValidation(new ConsistentCommittedData(cluster));
        return scheduler;
    }

    private void schedulePolling(EventScheduler scheduler,
                                 Cluster cluster,
                                 int pollIntervalMs,
                                 int pollJitterMs) {
        int delayMs = 0;
        for (int nodeId : cluster.nodes()) {
            scheduler.schedule(() -> cluster.pollIfRunning(nodeId), delayMs, pollIntervalMs, pollJitterMs);
            delayMs++;
        }
    }

    private static abstract class Event implements Comparable<Event> {
        final int eventId;
        final long deadlineMs;
        final Runnable action;

        protected Event(Runnable action, int eventId, long deadlineMs) {
            this.action = action;
            this.eventId = eventId;
            this.deadlineMs = deadlineMs;
        }

        void execute(EventScheduler scheduler) {
            action.run();
        }

        public int compareTo(Event other) {
            int compare = Long.compare(deadlineMs, other.deadlineMs);
            if (compare != 0)
                return compare;
            return Integer.compare(eventId, other.eventId);
        }
    }

    private static class PeriodicEvent extends Event {
        final Random random;
        final int periodMs;
        final int jitterMs;

        protected PeriodicEvent(Runnable action,
                                int eventId,
                                Random random,
                                long deadlineMs,
                                int periodMs,
                                int jitterMs) {
            super(action, eventId, deadlineMs);
            this.random = random;
            this.periodMs = periodMs;
            this.jitterMs = jitterMs;
        }

        @Override
        void execute(EventScheduler scheduler) {
            super.execute(scheduler);
            int nextExecDelayMs = periodMs + (jitterMs == 0 ? 0 : random.nextInt(jitterMs));
            scheduler.schedule(action, nextExecDelayMs, periodMs, jitterMs);
        }
    }

    private static class SequentialAppendAction implements Runnable {
        final Cluster cluster;

        private SequentialAppendAction(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void run() {
            cluster.withCurrentLeader(node -> {
                if (!node.client.isShuttingDown() && node.counter.isWritable())
                    node.counter.increment();
            });
        }
    }

    private interface Invariant {
        void verify();
    }

    private interface Validation {
        void validate();
    }

    private static class EventScheduler {
        private static final int MAX_ITERATIONS = 500000;

        final AtomicInteger eventIdGenerator = new AtomicInteger(0);
        final PriorityQueue<Event> queue = new PriorityQueue<>();
        final Random random;
        final Time time;
        final List<Invariant> invariants = new ArrayList<>();
        final List<Validation> validations = new ArrayList<>();

        private EventScheduler(Random random, Time time) {
            this.random = random;
            this.time = time;
        }

        // Add an invariant, which is checked after every event
        private void addInvariant(Invariant invariant) {
            invariants.add(invariant);
        }

        // Add a validation, which is checked at the end of the simulation
        private void addValidation(Validation validation) {
            validations.add(validation);
        }

        void schedule(Runnable action, int delayMs, int periodMs, int jitterMs) {
            long initialDeadlineMs = time.milliseconds() + delayMs;
            int eventId = eventIdGenerator.incrementAndGet();
            PeriodicEvent event = new PeriodicEvent(action, eventId, random, initialDeadlineMs, periodMs, jitterMs);
            queue.offer(event);
        }

        void runUntil(Supplier<Boolean> exitCondition) {
            for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
                if (exitCondition.get()) {
                    break;
                }

                if (queue.isEmpty()) {
                    throw new IllegalStateException("Event queue exhausted before condition was satisfied");
                }

                Event event = queue.poll();
                long delayMs = Math.max(event.deadlineMs - time.milliseconds(), 0);
                time.sleep(delayMs);
                event.execute(this);
                invariants.forEach(Invariant::verify);
            }

            assertTrue(exitCondition.get(), "Simulation condition was not satisfied after "
                + MAX_ITERATIONS + " iterations");

            validations.forEach(Validation::validate);
        }
    }

    private static class PersistentState {
        final MockQuorumStateStore store = new MockQuorumStateStore();
        final MockLog log = new MockLog(METADATA_PARTITION, Uuid.METADATA_TOPIC_ID);
    }

    private static class Cluster {
        final Random random;
        final AtomicInteger correlationIdCounter = new AtomicInteger();
        final MockTime time = new MockTime();
        final Uuid clusterId = Uuid.randomUuid();
        final Set<Integer> voters = new HashSet<>();
        final Map<Integer, PersistentState> nodes = new HashMap<>();
        final Map<Integer, RaftNode> running = new HashMap<>();

        private Cluster(int numVoters, int numObservers, Random random) {
            this.random = random;

            int nodeId = 0;
            for (; nodeId < numVoters; nodeId++) {
                voters.add(nodeId);
                nodes.put(nodeId, new PersistentState());
            }

            for (; nodeId < numVoters + numObservers; nodeId++) {
                nodes.put(nodeId, new PersistentState());
            }
        }

        Set<Integer> nodes() {
            return nodes.keySet();
        }

        int majoritySize() {
            return voters.size() / 2 + 1;
        }

        OptionalLong leaderHighWatermark() {
            Optional<RaftNode> leaderWithMaxEpoch = running.values().stream().filter(node -> node.client.quorum().isLeader())
                    .max((node1, node2) -> Integer.compare(node2.client.quorum().epoch(), node1.client.quorum().epoch()));
            if (leaderWithMaxEpoch.isPresent()) {
                return leaderWithMaxEpoch.get().client.highWatermark();
            } else {
                return OptionalLong.empty();
            }
        }

        boolean anyReachedHighWatermark(long offset) {
            return running.values().stream()
                    .anyMatch(node -> node.highWatermark() > offset);
        }

        long maxHighWatermarkReached() {
            return running.values().stream()
                .map(RaftNode::highWatermark)
                .max(Long::compareTo)
                .orElse(0L);
        }

        long maxHighWatermarkReached(Set<Integer> nodeIds) {
            return running.values().stream()
                .filter(node -> nodeIds.contains(node.nodeId))
                .map(RaftNode::highWatermark)
                .max(Long::compareTo)
                .orElse(0L);
        }

        boolean allReachedHighWatermark(long offset, Set<Integer> nodeIds) {
            return nodeIds.stream()
                .allMatch(nodeId -> running.get(nodeId).highWatermark() > offset);
        }

        boolean allReachedHighWatermark(long offset) {
            return running.values().stream()
                .allMatch(node -> node.highWatermark() > offset);
        }

        boolean hasLeader(int nodeId) {
            OptionalInt latestLeader = latestLeader();
            return latestLeader.isPresent() && latestLeader.getAsInt() == nodeId;
        }

        OptionalInt latestLeader() {
            OptionalInt latestLeader = OptionalInt.empty();
            int latestEpoch = 0;

            for (RaftNode node : running.values()) {
                if (node.client.quorum().epoch() > latestEpoch) {
                    latestLeader = node.client.quorum().leaderId();
                    latestEpoch = node.client.quorum().epoch();
                } else if (node.client.quorum().epoch() == latestEpoch && node.client.quorum().leaderId().isPresent()) {
                    latestLeader = node.client.quorum().leaderId();
                }
            }
            return latestLeader;
        }

        boolean hasConsistentLeader() {
            Iterator<RaftNode> iter = running.values().iterator();
            if (!iter.hasNext())
                return false;

            RaftNode first = iter.next();
            ElectionState election = first.store.readElectionState();
            if (!election.hasLeader())
                return false;

            while (iter.hasNext()) {
                RaftNode next = iter.next();
                if (!election.equals(next.store.readElectionState()))
                    return false;
            }

            return true;
        }

        void killAll() {
            running.clear();
        }

        void kill(int nodeId) {
            running.remove(nodeId);
        }

        void shutdown(int nodeId) {
            RaftNode node = running.get(nodeId);
            if (node == null) {
                throw new IllegalStateException("Attempt to shutdown a node which is not currently running");
            }
            node.client.shutdown(500).whenComplete((res, exception) -> kill(nodeId));
        }

        void pollIfRunning(int nodeId) {
            ifRunning(nodeId, RaftNode::poll);
        }

        Optional<RaftNode> nodeIfRunning(int nodeId) {
            return Optional.ofNullable(running.get(nodeId));
        }

        Collection<RaftNode> running() {
            return running.values();
        }

        void ifRunning(int nodeId, Consumer<RaftNode> action) {
            nodeIfRunning(nodeId).ifPresent(action);
        }

        Optional<RaftNode> randomRunning() {
            List<RaftNode> nodes = new ArrayList<>(running.values());
            if (nodes.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(nodes.get(random.nextInt(nodes.size())));
            }
        }

        void withCurrentLeader(Consumer<RaftNode> action) {
            for (RaftNode node : running.values()) {
                if (node.client.quorum().isLeader()) {
                    action.accept(node);
                }
            }
        }

        void forAllRunning(Consumer<RaftNode> action) {
            running.values().forEach(action);
        }

        void startAll() {
            if (!running.isEmpty())
                throw new IllegalStateException("Some nodes are already started");
            for (int voterId : nodes.keySet()) {
                start(voterId);
            }
        }

        void killAndDeletePersistentState(int nodeId) {
            kill(nodeId);
            nodes.put(nodeId, new PersistentState());
        }

        private static RaftConfig.AddressSpec nodeAddress(int id) {
            return new RaftConfig.InetAddressSpec(new InetSocketAddress("localhost", 9990 + id));
        }

        void start(int nodeId) {
            LogContext logContext = new LogContext("[Node " + nodeId + "] ");
            PersistentState persistentState = nodes.get(nodeId);
            MockNetworkChannel channel = new MockNetworkChannel(correlationIdCounter, voters);
            MockMessageQueue messageQueue = new MockMessageQueue();
            Map<Integer, RaftConfig.AddressSpec> voterAddressMap = voters.stream()
                .collect(Collectors.toMap(id -> id, Cluster::nodeAddress));
            RaftConfig raftConfig = new RaftConfig(voterAddressMap, REQUEST_TIMEOUT_MS, RETRY_BACKOFF_MS, ELECTION_TIMEOUT_MS,
                    ELECTION_JITTER_MS, FETCH_TIMEOUT_MS, LINGER_MS);
            Metrics metrics = new Metrics(time);

            persistentState.log.reopen();

            IntSerde serde = new IntSerde();
            MemoryPool memoryPool = new BatchMemoryPool(2, KafkaRaftClient.MAX_BATCH_SIZE_BYTES);

            KafkaRaftClient<Integer> client = new KafkaRaftClient<>(
                serde,
                channel,
                messageQueue,
                persistentState.log,
                persistentState.store,
                memoryPool,
                time,
                metrics,
                new MockExpirationService(time),
                FETCH_MAX_WAIT_MS,
                clusterId.toString(),
                OptionalInt.of(nodeId),
                logContext,
                random,
                raftConfig
            );
            RaftNode node = new RaftNode(
                nodeId,
                client,
                persistentState.log,
                channel,
                messageQueue,
                persistentState.store,
                logContext,
                time,
                random
            );
            node.initialize();
            running.put(nodeId, node);
        }
    }

    private static class RaftNode {
        final int nodeId;
        final KafkaRaftClient<Integer> client;
        final MockLog log;
        final MockNetworkChannel channel;
        final MockMessageQueue messageQueue;
        final MockQuorumStateStore store;
        final LogContext logContext;
        final ReplicatedCounter counter;
        final Time time;
        final Random random;

        private RaftNode(
            int nodeId,
            KafkaRaftClient<Integer> client,
            MockLog log,
            MockNetworkChannel channel,
            MockMessageQueue messageQueue,
            MockQuorumStateStore store,
            LogContext logContext,
            Time time,
            Random random
        ) {
            this.nodeId = nodeId;
            this.client = client;
            this.log = log;
            this.channel = channel;
            this.messageQueue = messageQueue;
            this.store = store;
            this.logContext = logContext;
            this.time = time;
            this.random = random;
            this.counter = new ReplicatedCounter(nodeId, client, logContext);
        }

        void initialize() {
            try {
                client.register(this.counter);
                client.initialize();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void poll() {
            try {
                do {
                    client.poll();
                } while (client.isRunning() && !messageQueue.isEmpty());
            } catch (Exception e) {
                throw new RuntimeException("Uncaught exception during poll of node " + nodeId, e);
            }
        }

        long highWatermark() {
            return client.quorum().highWatermark()
                .map(hw -> hw.offset)
                .orElse(0L);
        }

        @Override
        public String toString() {
            return "Node(id=" + nodeId + ", hw=" + highWatermark() + ")";
        }
    }

    private static class InflightRequest {
        final int correlationId;
        final int sourceId;
        final int destinationId;

        private InflightRequest(int correlationId, int sourceId, int destinationId) {
            this.correlationId = correlationId;
            this.sourceId = sourceId;
            this.destinationId = destinationId;
        }
    }

    private interface NetworkFilter {
        boolean acceptInbound(RaftMessage message);
        boolean acceptOutbound(RaftMessage message);
    }

    private static class PermitAllTraffic implements NetworkFilter {

        @Override
        public boolean acceptInbound(RaftMessage message) {
            return true;
        }

        @Override
        public boolean acceptOutbound(RaftMessage message) {
            return true;
        }
    }

    private static class DropAllTraffic implements NetworkFilter {

        @Override
        public boolean acceptInbound(RaftMessage message) {
            return false;
        }

        @Override
        public boolean acceptOutbound(RaftMessage message) {
            return false;
        }
    }

    private static class DropOutboundRequestsFrom implements NetworkFilter {

        private final Set<Integer> unreachable;

        private DropOutboundRequestsFrom(Set<Integer> unreachable) {
            this.unreachable = unreachable;
        }

        @Override
        public boolean acceptInbound(RaftMessage message) {
            return true;
        }

        @Override
        public boolean acceptOutbound(RaftMessage message) {
            if (message instanceof RaftRequest.Outbound) {
                RaftRequest.Outbound request = (RaftRequest.Outbound) message;
                return !unreachable.contains(request.destinationId());
            }
            return true;
        }
    }

    private static class MonotonicEpoch implements Invariant {
        final Cluster cluster;
        final Map<Integer, Integer> nodeEpochs = new HashMap<>();

        private MonotonicEpoch(Cluster cluster) {
            this.cluster = cluster;
            for (Map.Entry<Integer, PersistentState> nodeStateEntry : cluster.nodes.entrySet()) {
                Integer nodeId = nodeStateEntry.getKey();
                nodeEpochs.put(nodeId, 0);
            }
        }

        @Override
        public void verify() {
            for (Map.Entry<Integer, PersistentState> nodeStateEntry : cluster.nodes.entrySet()) {
                Integer nodeId = nodeStateEntry.getKey();
                PersistentState state = nodeStateEntry.getValue();
                Integer oldEpoch = nodeEpochs.get(nodeId);

                ElectionState electionState = state.store.readElectionState();
                if (electionState == null) {
                    continue;
                }

                Integer newEpoch = electionState.epoch;
                if (oldEpoch > newEpoch) {
                    fail("Non-monotonic update of epoch detected on node " + nodeId + ": " +
                            oldEpoch + " -> " + newEpoch);
                }
                cluster.ifRunning(nodeId, nodeState -> {
                    assertEquals(newEpoch.intValue(), nodeState.client.quorum().epoch());
                });
                nodeEpochs.put(nodeId, newEpoch);
            }
        }
    }

    private static class MajorityReachedHighWatermark implements Invariant {
        final Cluster cluster;

        private MajorityReachedHighWatermark(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void verify() {
            cluster.leaderHighWatermark().ifPresent(highWatermark -> {
                long numReachedHighWatermark = cluster.nodes.entrySet().stream()
                    .filter(entry -> cluster.voters.contains(entry.getKey()))
                    .filter(entry -> entry.getValue().log.endOffset().offset >= highWatermark)
                    .count();
                assertTrue(
                    numReachedHighWatermark >= cluster.majoritySize(),
                    "Insufficient nodes have reached current high watermark");
            });
        }
    }

    private static class SingleLeader implements Invariant {
        final Cluster cluster;
        int epoch = 0;
        OptionalInt leaderId = OptionalInt.empty();

        private SingleLeader(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void verify() {
            for (Map.Entry<Integer, PersistentState> nodeEntry : cluster.nodes.entrySet()) {
                PersistentState state = nodeEntry.getValue();
                ElectionState electionState = state.store.readElectionState();

                if (electionState != null && electionState.epoch >= epoch && electionState.hasLeader()) {
                    if (epoch == electionState.epoch && leaderId.isPresent()) {
                        assertEquals(leaderId.getAsInt(), electionState.leaderId());
                    } else {
                        epoch = electionState.epoch;
                        leaderId = OptionalInt.of(electionState.leaderId());
                    }
                }
            }
        }
    }

    private static class MonotonicHighWatermark implements Invariant {
        final Cluster cluster;
        long highWatermark = 0;

        private MonotonicHighWatermark(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void verify() {
            OptionalLong leaderHighWatermark = cluster.leaderHighWatermark();
            leaderHighWatermark.ifPresent(newHighWatermark -> {
                long oldHighWatermark = highWatermark;
                this.highWatermark = newHighWatermark;
                if (newHighWatermark < oldHighWatermark) {
                    fail("Non-monotonic update of high watermark detected: " +
                            oldHighWatermark + " -> " + newHighWatermark);
                }
            });
        }
    }

    /**
     * Validating the committed data is expensive, so we do this as a {@link Validation}. We depend
     * on the following external invariants:
     *
     * - High watermark increases monotonically
     * - Truncation below the high watermark is not permitted
     * - A majority of nodes reach the high watermark
     *
     * Under these assumptions, once the simulation finishes, we validate that all nodes have
     * consistent data below the respective high watermark that has been recorded.
     */
    private static class ConsistentCommittedData implements Validation {
        final Cluster cluster;
        final Map<Long, Integer> committedSequenceNumbers = new HashMap<>();

        private ConsistentCommittedData(Cluster cluster) {
            this.cluster = cluster;
        }

        private int parseSequenceNumber(ByteBuffer value) {
            return (int) Type.INT32.read(value);
        }

        private void assertCommittedData(int nodeId, KafkaRaftClient<Integer> manager, MockLog log) {
            OptionalLong highWatermark = manager.highWatermark();
            if (!highWatermark.isPresent()) {
                // We cannot do validation if the current high watermark is unknown
                return;
            }

            for (LogBatch batch : log.readBatches(0L, highWatermark)) {
                if (batch.isControlBatch) {
                    continue;
                }

                for (LogEntry entry : batch.entries) {
                    long offset = entry.offset;
                    assertTrue(offset < highWatermark.getAsLong());

                    int sequence = parseSequenceNumber(entry.record.value().duplicate());
                    committedSequenceNumbers.putIfAbsent(offset, sequence);

                    int committedSequence = committedSequenceNumbers.get(offset);
                    assertEquals(
                        committedSequence, sequence,
                        "Committed sequence at offset " + offset + " changed on node " + nodeId);
                }
            }
        }

        @Override
        public void validate() {
            cluster.forAllRunning(node -> assertCommittedData(node.nodeId, node.client, node.log));
        }
    }

    private static class MessageRouter {
        final Map<Integer, InflightRequest> inflight = new HashMap<>();
        final Map<Integer, NetworkFilter> filters = new HashMap<>();
        final Cluster cluster;

        private MessageRouter(Cluster cluster) {
            this.cluster = cluster;
            for (int nodeId : cluster.nodes.keySet())
                filters.put(nodeId, new PermitAllTraffic());
        }

        void deliver(int senderId, RaftRequest.Outbound outbound) {
            if (!filters.get(senderId).acceptOutbound(outbound))
                return;

            int correlationId = outbound.correlationId();
            int destinationId = outbound.destinationId();
            RaftRequest.Inbound inbound = new RaftRequest.Inbound(correlationId, outbound.data(),
                cluster.time.milliseconds());

            if (!filters.get(destinationId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(destinationId).ifPresent(node -> {
                inflight.put(correlationId, new InflightRequest(correlationId, senderId, destinationId));

                inbound.completion.whenComplete((response, exception) -> {
                    if (response != null && filters.get(destinationId).acceptOutbound(response)) {
                        deliver(destinationId, response);
                    }
                });

                node.client.handle(inbound);
            });
        }

        void deliver(int senderId, RaftResponse.Outbound outbound) {
            int correlationId = outbound.correlationId();
            RaftResponse.Inbound inbound = new RaftResponse.Inbound(correlationId, outbound.data(), senderId);
            InflightRequest inflightRequest = inflight.remove(correlationId);

            if (!filters.get(inflightRequest.sourceId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(inflightRequest.sourceId).ifPresent(node -> {
                node.channel.mockReceive(inbound);
            });
        }

        void filter(int nodeId, NetworkFilter filter) {
            filters.put(nodeId, filter);
        }

        void deliverTo(RaftNode node) {
            node.channel.drainSendQueue().forEach(msg -> deliver(node.nodeId, msg));
        }

        void deliverAll() {
            for (RaftNode node : cluster.running()) {
                deliverTo(node);
            }
        }
    }

    private static class IntSerde implements RecordSerde<Integer> {
        @Override
        public int recordSize(Integer data, ObjectSerializationCache serializationCache) {
            return Type.INT32.sizeOf(data);
        }

        @Override
        public void write(Integer data, ObjectSerializationCache serializationCache, Writable out) {
            out.writeInt(data);
        }

        @Override
        public Integer read(Readable input, int size) {
            return input.readInt();
        }
    }

}
