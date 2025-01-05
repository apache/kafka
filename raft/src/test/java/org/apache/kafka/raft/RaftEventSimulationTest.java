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
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.MockLog.LogBatch;
import org.apache.kafka.raft.MockLog.LogEntry;
import org.apache.kafka.raft.internals.BatchMemoryPool;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;

import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Tag;
import net.jqwik.api.constraints.IntRange;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    private static final TopicPartition METADATA_PARTITION = new TopicPartition("__cluster_metadata", 0);
    private static final int ELECTION_TIMEOUT_MS = 1000;
    private static final int ELECTION_JITTER_MS = 100;
    private static final int FETCH_TIMEOUT_MS = 3000;
    private static final int RETRY_BACKOFF_MS = 50;
    private static final int REQUEST_TIMEOUT_MS = 3000;
    private static final int FETCH_MAX_WAIT_MS = 100;
    private static final int LINGER_MS = 0;

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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

        Iterator<Integer> nodeIdsIterator = cluster.nodeIds().iterator();
        for (int i = 0; i < cluster.majoritySize(); i++) {
            Integer nodeId = nodeIdsIterator.next();
            cluster.start(nodeId);
        }

        scheduler.runUntil(() -> cluster.allReachedHighWatermark(highWatermark + 10));
    }

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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

        Set<Integer> nonPartitionedNodes = new HashSet<>(cluster.nodeIds());
        nonPartitionedNodes.remove(leaderId);

        scheduler.runUntil(() -> cluster.allReachedHighWatermark(20, nonPartitionedNodes));
    }

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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
        router.filter(
            0,
            new DropOutboundRequestsTo(cluster.endpointsFromIds(Set.of(2, 3, 4)))
        );
        router.filter(
            1,
            new DropOutboundRequestsTo(cluster.endpointsFromIds(Set.of(2, 3, 4)))
        );
        router.filter(2, new DropOutboundRequestsTo(cluster.endpointsFromIds(Set.of(0, 1))));
        router.filter(3, new DropOutboundRequestsTo(cluster.endpointsFromIds(Set.of(0, 1))));
        router.filter(4, new DropOutboundRequestsTo(cluster.endpointsFromIds(Set.of(0, 1))));

        long partitionLogEndOffset = cluster.maxLogEndOffset();
        scheduler.runUntil(() -> cluster.anyReachedHighWatermark(2 * partitionLogEndOffset));

        long minorityHighWatermark = cluster.maxHighWatermarkReached(Set.of(0, 1));
        long majorityHighWatermark = cluster.maxHighWatermarkReached(Set.of(2, 3, 4));

        assertTrue(
            majorityHighWatermark > minorityHighWatermark,
            String.format(
                "majorityHighWatermark = %s, minorityHighWatermark = %s",
                majorityHighWatermark,
                minorityHighWatermark
            )
        );

        // Now restore the partition and verify everyone catches up
        router.filter(0, new PermitAllTraffic());
        router.filter(1, new PermitAllTraffic());
        router.filter(2, new PermitAllTraffic());
        router.filter(3, new PermitAllTraffic());
        router.filter(4, new PermitAllTraffic());

        long restoredLogEndOffset = cluster.maxLogEndOffset();
        scheduler.runUntil(() -> cluster.allReachedHighWatermark(2 * restoredLogEndOffset));
    }

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
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
        scheduler.addInvariant(new SnapshotAtLogStart(cluster));
        scheduler.addInvariant(new LeaderNeverLoadSnapshot(cluster));
        scheduler.addValidation(new ConsistentCommittedData(cluster));
        return scheduler;
    }

    private void schedulePolling(EventScheduler scheduler,
                                 Cluster cluster,
                                 int pollIntervalMs,
                                 int pollJitterMs) {
        int delayMs = 0;
        for (int nodeId : cluster.nodeIds()) {
            scheduler.schedule(() -> cluster.pollIfRunning(nodeId), delayMs, pollIntervalMs, pollJitterMs);
            delayMs++;
        }
    }

    private abstract static class Event implements Comparable<Event> {
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
        final Uuid nodeDirectoryId = Uuid.randomUuid();
        final MockLog log;

        PersistentState(int nodeId) {
            log = new MockLog(
                METADATA_PARTITION,
                Uuid.METADATA_TOPIC_ID,
                new LogContext(String.format("[Node %s] ", nodeId))
            );
        }
    }

    private static class Cluster {
        final Random random;
        final AtomicInteger correlationIdCounter = new AtomicInteger();
        final MockTime time = new MockTime();
        final String clusterId = Uuid.randomUuid().toString();
        final Map<Integer, Node> voters = new HashMap<>();
        final Map<Integer, PersistentState> nodes = new HashMap<>();
        final Map<Integer, RaftNode> running = new HashMap<>();

        private Cluster(int numVoters, int numObservers, Random random) {
            this.random = random;

            for (int nodeId = 0; nodeId < numVoters; nodeId++) {
                voters.put(nodeId, nodeFromId(nodeId));
                nodes.put(nodeId, new PersistentState(nodeId));
            }

            for (int nodeIdDelta = 0; nodeIdDelta < numObservers; nodeIdDelta++) {
                int nodeId = numVoters + nodeIdDelta;
                nodes.put(nodeId, new PersistentState(nodeId));
            }
        }

        Set<InetSocketAddress> endpointsFromIds(Set<Integer> nodeIds) {
            return voters
                .values()
                .stream()
                .filter(node -> nodeIds.contains(node.id()))
                .map(Cluster::nodeAddress)
                .collect(Collectors.toSet());
        }

        Set<Integer> nodeIds() {
            return nodes.keySet();
        }

        int majoritySize() {
            return voters.size() / 2 + 1;
        }

        long maxLogEndOffset() {
            return running
                .values()
                .stream()
                .mapToLong(RaftNode::logEndOffset)
                .max()
                .orElse(0L);
        }

        OptionalLong leaderHighWatermark() {
            Optional<RaftNode> leaderWithMaxEpoch = running
                .values()
                .stream()
                .filter(node -> node.client.quorum().isLeader())
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
                .mapToLong(RaftNode::highWatermark)
                .max()
                .orElse(0L);
        }

        long maxHighWatermarkReached(Set<Integer> nodeIds) {
            return running.values().stream()
                .filter(node -> nodeIds.contains(node.nodeId))
                .mapToLong(RaftNode::highWatermark)
                .max()
                .orElse(0L);
        }

        boolean allReachedHighWatermark(long offset, Set<Integer> nodeIds) {
            return nodeIds.stream()
                .allMatch(nodeId -> running.get(nodeId).highWatermark() >= offset);
        }

        boolean allReachedHighWatermark(long offset) {
            return running.values().stream()
                .allMatch(node -> node.highWatermark() >= offset);
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
            ElectionState election = first.store.readElectionState().get();
            if (!election.hasLeader())
                return false;

            while (iter.hasNext()) {
                RaftNode next = iter.next();
                if (!election.equals(next.store.readElectionState().get()))
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
            nodes.put(nodeId, new PersistentState(nodeId));
        }

        private static final int PORT = 1234;

        private static InetSocketAddress nodeAddress(Node node) {
            return InetSocketAddress.createUnresolved(node.host(), node.port());
        }

        private static Node nodeFromId(int nodeId) {
            return new Node(nodeId, hostFromId(nodeId), PORT);
        }

        private static String hostFromId(int nodeId) {
            return String.format("host-node-%d", nodeId);
        }

        private static Endpoints endpointsFromId(int nodeId, ListenerName listenerName) {
            return Endpoints.fromInetSocketAddresses(
                Collections.singletonMap(
                    listenerName,
                    InetSocketAddress.createUnresolved(hostFromId(nodeId), PORT)
                )
            );
        }

        void start(int nodeId) {
            LogContext logContext = new LogContext("[Node " + nodeId + "] ");
            PersistentState persistentState = nodes.get(nodeId);
            MockNetworkChannel channel = new MockNetworkChannel(correlationIdCounter);
            MockMessageQueue messageQueue = new MockMessageQueue();
            Map<Integer, InetSocketAddress> voterAddressMap = voters
                .values()
                .stream()
                .collect(Collectors.toMap(Node::id, Cluster::nodeAddress));

            Map<String, Integer> configMap = new HashMap<>();
            configMap.put(QuorumConfig.QUORUM_REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
            configMap.put(QuorumConfig.QUORUM_RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);
            configMap.put(QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG, ELECTION_TIMEOUT_MS);
            configMap.put(QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG, ELECTION_JITTER_MS);
            configMap.put(QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG, FETCH_TIMEOUT_MS);
            configMap.put(QuorumConfig.QUORUM_LINGER_MS_CONFIG, LINGER_MS);
            QuorumConfig quorumConfig = new QuorumConfig(new AbstractConfig(QuorumConfig.CONFIG_DEF, configMap));
            Metrics metrics = new Metrics(time);

            persistentState.log.reopen();

            IntSerde serde = new IntSerde();
            MemoryPool memoryPool = new BatchMemoryPool(2, KafkaRaftClient.MAX_BATCH_SIZE_BYTES);

            KafkaRaftClient<Integer> client = new KafkaRaftClient<>(
                OptionalInt.of(nodeId),
                persistentState.nodeDirectoryId,
                serde,
                channel,
                messageQueue,
                persistentState.log,
                memoryPool,
                time,
                new MockExpirationService(time),
                FETCH_MAX_WAIT_MS,
                true,
                clusterId,
                Collections.emptyList(),
                endpointsFromId(nodeId, channel.listenerName()),
                Feature.KRAFT_VERSION.supportedVersionRange(),
                logContext,
                random,
                quorumConfig
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
                random,
                serde
            );
            node.initialize(voterAddressMap, metrics);

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
        final ReplicatedCounter counter;
        final RecordSerde<Integer> intSerde;

        private RaftNode(
            int nodeId,
            KafkaRaftClient<Integer> client,
            MockLog log,
            MockNetworkChannel channel,
            MockMessageQueue messageQueue,
            MockQuorumStateStore store,
            LogContext logContext,
            Time time,
            Random random,
            RecordSerde<Integer> intSerde
        ) {
            this.nodeId = nodeId;
            this.client = client;
            this.log = log;
            this.channel = channel;
            this.messageQueue = messageQueue;
            this.store = store;
            this.counter = new ReplicatedCounter(nodeId, client, logContext);
            this.intSerde = intSerde;
        }

        void initialize(Map<Integer, InetSocketAddress> voterAddresses, Metrics metrics) {
            client.register(counter);
            client.initialize(
                voterAddresses,
                store,
                metrics
            );
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
                .map(LogOffsetMetadata::offset)
                .orElse(0L);
        }

        long logEndOffset() {
            return log.endOffset().offset();
        }

        @Override
        public String toString() {
            return String.format(
                "Node(id=%s, hw=%s, logEndOffset=%s)",
                nodeId,
                highWatermark(),
                logEndOffset()
            );
        }
    }

    private static class InflightRequest {
        final int sourceId;
        final Node destination;

        private InflightRequest(int sourceId, Node destination) {
            this.sourceId = sourceId;
            this.destination = destination;
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

    private static class DropOutboundRequestsTo implements NetworkFilter {
        private final Set<InetSocketAddress> unreachable;

        /**
         * This network filter drops any outbound message sent to the {@code unreachable} nodes.
         *
         * @param unreachable the set of destination address which are not reachable
         */
        private DropOutboundRequestsTo(Set<InetSocketAddress> unreachable) {
            this.unreachable = unreachable;
        }

        @Override
        public boolean acceptInbound(RaftMessage message) {
            return true;
        }

        /**
         * Returns if the message should be sent to the destination.
         *
         * Returns false when outbound request messages contains a destination {@code Node} that
         * matches the set of unreachable {@code InetSocketAddress}. Note that the {@code Node.id()}
         * and {@code Node.rack()} are not compared.
         *
         * @param message the raft message
         * @return true if the message should be delivered, otherwise false
         */
        @Override
        public boolean acceptOutbound(RaftMessage message) {
            if (message instanceof RaftRequest.Outbound) {
                RaftRequest.Outbound request = (RaftRequest.Outbound) message;
                InetSocketAddress destination = InetSocketAddress.createUnresolved(
                    request.destination().host(),
                    request.destination().port()
                );
                return !unreachable.contains(destination);
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

                Optional<ElectionState> electionState = state.store.readElectionState();
                if (electionState.isEmpty()) {
                    continue;
                }

                int newEpoch = electionState.get().epoch();
                if (oldEpoch > newEpoch) {
                    fail("Non-monotonic update of epoch detected on node " + nodeId + ": " +
                            oldEpoch + " -> " + newEpoch);
                }
                cluster.ifRunning(nodeId, nodeState ->
                    assertEquals(newEpoch, nodeState.client.quorum().epoch())
                );
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
                    .filter(entry -> cluster.voters.containsKey(entry.getKey()))
                    .filter(entry -> entry.getValue().log.endOffset().offset() >= highWatermark)
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
                Optional<ElectionState> electionState = state.store.readElectionState();

                electionState.ifPresent(election -> {
                    if (election.epoch() >= epoch && election.hasLeader()) {
                        if (epoch == election.epoch() && leaderId.isPresent()) {
                            assertEquals(leaderId.getAsInt(), election.leaderId());
                        } else {
                            epoch = election.epoch();
                            leaderId = OptionalInt.of(election.leaderId());
                        }
                    }
                });
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

    private static class SnapshotAtLogStart implements Invariant {
        final Cluster cluster;

        private SnapshotAtLogStart(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void verify() {
            for (Map.Entry<Integer, PersistentState> nodeEntry : cluster.nodes.entrySet()) {
                int nodeId = nodeEntry.getKey();
                ReplicatedLog log = nodeEntry.getValue().log;
                log.earliestSnapshotId().ifPresent(earliestSnapshotId  -> {
                    long logStartOffset = log.startOffset();
                    ValidOffsetAndEpoch validateOffsetAndEpoch = log.validateOffsetAndEpoch(
                        earliestSnapshotId.offset(),
                        earliestSnapshotId.epoch()
                    );

                    assertTrue(
                        logStartOffset <= earliestSnapshotId.offset(),
                        () -> String.format(
                            "invalid log start offset (%s) and snapshotId offset (%s): nodeId = %s",
                            logStartOffset,
                            earliestSnapshotId.offset(),
                            nodeId
                        )
                    );
                    assertEquals(
                        ValidOffsetAndEpoch.valid(earliestSnapshotId),
                        validateOffsetAndEpoch,
                        () -> String.format("invalid leader epoch cache: nodeId = %s", nodeId)
                    );

                    if (logStartOffset > 0) {
                        assertEquals(
                            logStartOffset,
                            earliestSnapshotId.offset(),
                            () -> String.format("missing snapshot at log start offset: nodeId = %s", nodeId)
                        );
                    }
                });
            }
        }
    }

    private static class LeaderNeverLoadSnapshot implements Invariant {
        final Cluster cluster;

        private LeaderNeverLoadSnapshot(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void verify() {
            for (RaftNode raftNode : cluster.running()) {
                if (raftNode.counter.isWritable()) {
                    assertEquals(0, raftNode.counter.handleLoadSnapshotCalls());
                }
            }
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

        private void assertCommittedData(RaftNode node) {
            final int nodeId = node.nodeId;
            final KafkaRaftClient<Integer> manager = node.client;
            final MockLog log = node.log;

            OptionalLong highWatermark = manager.highWatermark();
            if (highWatermark.isEmpty()) {
                // We cannot do validation if the current high watermark is unknown
                return;
            }

            AtomicLong startOffset = new AtomicLong(0);
            log.earliestSnapshotId().ifPresent(snapshotId -> {
                assertTrue(snapshotId.offset() <= highWatermark.getAsLong());
                startOffset.set(snapshotId.offset());

                try (SnapshotReader<Integer> snapshot = RecordsSnapshotReader.of(
                        log.readSnapshot(snapshotId).get(),
                        node.intSerde,
                        BufferSupplier.create(),
                        Integer.MAX_VALUE,
                        true
                    )
                ) {
                    // Since the state machine is only on e value we only expect one data record in the snapshot
                    // Expect only one batch with only one record
                    OptionalInt sequence = OptionalInt.empty();
                    while (snapshot.hasNext()) {
                        Batch<Integer> batch = snapshot.next();
                        if (!batch.records().isEmpty()) {
                            assertEquals(1, batch.records().size());
                            assertFalse(sequence.isPresent());
                            sequence = OptionalInt.of(batch.records().get(0));
                        }
                    }

                    // The snapshotId offset is an "end offset"
                    long offset = snapshotId.offset() - 1;
                    committedSequenceNumbers.putIfAbsent(offset, sequence.getAsInt());

                    assertEquals(
                        committedSequenceNumbers.get(offset),
                        sequence.getAsInt(),
                        String.format("Committed sequence at offset %s changed on node %s", offset, nodeId)
                    );
                }
            });

            for (LogBatch batch : log.readBatches(startOffset.get(), highWatermark)) {
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
                        committedSequence,
                        sequence,
                        String.format("Committed sequence at offset %d changed on node %d", offset, nodeId)
                    );
                }
            }
        }

        @Override
        public void validate() {
            cluster.forAllRunning(this::assertCommittedData);
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
            Node destination = outbound.destination();
            RaftRequest.Inbound inbound = cluster
                .nodeIfRunning(senderId)
                .map(node ->
                    new RaftRequest.Inbound(
                        node.channel.listenerName(),
                        correlationId,
                        ApiMessageType
                            .fromApiKey(outbound.data().apiKey())
                            .highestSupportedVersion(true),
                        outbound.data(),
                        cluster.time.milliseconds()
                    )
                )
                .get();

            if (!filters.get(destination.id()).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(destination.id()).ifPresent(node -> {
                inflight.put(correlationId, new InflightRequest(senderId, destination));

                inbound.completion.whenComplete((response, exception) -> {
                    if (response != null && filters.get(destination.id()).acceptOutbound(response)) {
                        deliver(response);
                    }
                });

                node.client.handle(inbound);
            });
        }

        void deliver(RaftResponse.Outbound outbound) {
            int correlationId = outbound.correlationId();
            InflightRequest inflightRequest = inflight.remove(correlationId);

            RaftResponse.Inbound inbound = new RaftResponse.Inbound(
                correlationId,
                outbound.data(),
                // The source of the response is the destination of the request
                inflightRequest.destination
            );

            if (!filters.get(inflightRequest.sourceId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(inflightRequest.sourceId).ifPresent(node ->
                node.channel.mockReceive(inbound)
            );
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
