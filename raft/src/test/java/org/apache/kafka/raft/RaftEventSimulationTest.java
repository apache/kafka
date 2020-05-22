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

import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.MockLog.LogBatch;
import org.apache.kafka.raft.MockLog.LogEntry;
import org.junit.Test;

import java.io.IOException;
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
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class RaftEventSimulationTest {
    private static final int ELECTION_TIMEOUT_MS = 1000;
    private static final int ELECTION_JITTER_MS = 100;
    private static final int FETCH_TIMEOUT_MS = 5000;
    private static final int RETRY_BACKOFF_MS = 50;
    private static final int REQUEST_TIMEOUT_MS = 500;

    @Test
    public void testInitialLeaderElectionQuorumSizeOne() {
        testInitialLeaderElection(new QuorumConfig(1));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeTwo() {
        testInitialLeaderElection(new QuorumConfig(2));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeThree() {
        testInitialLeaderElection(new QuorumConfig(3));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeFour() {
        testInitialLeaderElection(new QuorumConfig(4));
    }

    @Test
    public void testInitialLeaderElectionQuorumSizeFive() {
        testInitialLeaderElection(new QuorumConfig(5));
    }

    private void testInitialLeaderElection(QuorumConfig config) {
        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            cluster.startAll();
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 1);
            scheduler.runUntil(() -> {
                try {
                    return cluster.hasConsistentLeader();
                } catch (IOException e) {
                    return false;
                }
            });
        }
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeOne() throws IOException {
        testReplicationNoLeaderChange(new QuorumConfig(1));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeTwo() throws IOException {
        testReplicationNoLeaderChange(new QuorumConfig(2));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeThree() throws IOException {
        testReplicationNoLeaderChange(new QuorumConfig(3, 0));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeFour() throws IOException {
        testReplicationNoLeaderChange(new QuorumConfig(4));
    }

    @Test
    public void testReplicationNoLeaderChangeQuorumSizeFive() throws IOException {
        testReplicationNoLeaderChange(new QuorumConfig(5));
    }

    private void testReplicationNoLeaderChange(QuorumConfig config) throws IOException {
        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 0 as the leader
            cluster.initializeElection(ElectionState.withElectedLeader(2, 0));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 0);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));
        }
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeThree() throws IOException {
        testElectionAfterLeaderFailure(new QuorumConfig(3, 0));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeThreeAndTwoObservers() throws IOException {
        testElectionAfterLeaderFailure(new QuorumConfig(3, 2));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeFour() throws IOException {
        testElectionAfterLeaderFailure(new QuorumConfig(4, 0));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeFourAndTwoObservers() throws IOException {
        testElectionAfterLeaderFailure(new QuorumConfig(4, 2));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeFive() throws IOException {
        testElectionAfterLeaderFailure(new QuorumConfig(5, 0));
    }

    @Test
    public void testElectionAfterLeaderFailureQuorumSizeFiveAndThreeObservers() throws IOException {
        testElectionAfterLeaderFailure(new QuorumConfig(5, 3));
    }

    private void testElectionAfterLeaderFailure(QuorumConfig config) throws IOException {
        // We need at least three voters to run this tests
        assumeTrue(config.numVoters > 2);

        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 1 as the leader
            cluster.initializeElection(ElectionState.withElectedLeader(2, 0));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            // Seed the cluster with some data
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 1);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

            // Kill the leader and write some more data. We can verify the new leader has been elected
            // by verifying that the high watermark can still advance.
            cluster.kill(1);
            scheduler.runUntil(() -> cluster.allReachedHighWatermark(20));
        }
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeThree() throws IOException {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(3));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeThreeAndTwoObservers() throws IOException {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(3, 2));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeFour() throws IOException {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(4));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeFourAndTwoObservers() throws IOException {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(4, 2));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeFive() throws IOException {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(5));
    }

    @Test
    public void testElectionAfterLeaderNetworkPartitionQuorumSizeFiveAndThreeObservers() throws IOException {
        testElectionAfterLeaderNetworkPartition(new QuorumConfig(5, 3));
    }

    private void testElectionAfterLeaderNetworkPartition(QuorumConfig config) throws IOException {
        // We need at least three voters to run this tests
        assumeTrue(config.numVoters > 2);

        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 1 as the leader
            cluster.initializeElection(ElectionState.withElectedLeader(2, 1));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            // Seed the cluster with some data
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 2);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

            // The leader gets partitioned off. We can verify the new leader has been elected
            // by writing some data and ensuring that it gets replicated
            router.filter(1, new DropAllTraffic());

            Set<Integer> nonPartitionedNodes = new HashSet<>(cluster.nodes());
            nonPartitionedNodes.remove(1);

            scheduler.runUntil(() -> cluster.allReachedHighWatermark(20, nonPartitionedNodes));
        }
    }

    @Test
    public void testElectionAfterMultiNodeNetworkPartitionQuorumSizeFive() throws IOException {
        testElectionAfterMultiNodeNetworkPartition(new QuorumConfig(5));
    }

    @Test
    public void testElectionAfterMultiNodeNetworkPartitionQuorumSizeFiveAndTwoObservers() throws IOException {
        testElectionAfterMultiNodeNetworkPartition(new QuorumConfig(5, 2));
    }

    private void testElectionAfterMultiNodeNetworkPartition(QuorumConfig config) throws IOException {
        // We need at least three voters to run this tests
        assumeTrue(config.numVoters > 2);

        for (int seed = 0; seed < 100; seed++) {
            Cluster cluster = new Cluster(config, seed);
            MessageRouter router = new MessageRouter(cluster);
            EventScheduler scheduler = schedulerWithDefaultInvariants(cluster);

            // Start with node 1 as the leader
            cluster.initializeElection(ElectionState.withElectedLeader(2, 1));
            cluster.startAll();
            assertTrue(cluster.hasConsistentLeader());

            // Seed the cluster with some data
            schedulePolling(scheduler, cluster, 3, 5);
            scheduler.schedule(router::deliverAll, 0, 2, 2);
            scheduler.schedule(new SequentialAppendAction(cluster), 0, 2, 3);
            scheduler.runUntil(() -> cluster.anyReachedHighWatermark(10));

            // Partition the nodes into two sets. Nodes are reachable within each set, but the
            // two sets cannot communicate with each other.
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
    }

    private EventScheduler schedulerWithDefaultInvariants(Cluster cluster) {
        EventScheduler scheduler = new EventScheduler(cluster.random, cluster.time);
        scheduler.addInvariant(new MonotonicHighWatermark(cluster));
        scheduler.addInvariant(new MonotonicEpoch(cluster));
        scheduler.addInvariant(new ConsistentCommittedData(cluster));
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

    @FunctionalInterface
    private interface Action {
        void execute();
    }

    private static abstract class Event implements Comparable<Event> {
        final int eventId;
        final long deadlineMs;
        final Action action;

        protected Event(Action action, int eventId, long deadlineMs) {
            this.action = action;
            this.eventId = eventId;
            this.deadlineMs = deadlineMs;
        }

        void execute(EventScheduler scheduler) {
            action.execute();
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

        protected PeriodicEvent(Action action,
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

    private static class SequentialAppendAction implements Action {
        final Cluster cluster;

        private SequentialAppendAction(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void execute() {
            cluster.withCurrentLeader(node -> node.counter.increment());
        }
    }

    @FunctionalInterface
    private interface Invariant {
        void verify();
    }

    private static class EventScheduler {
        final AtomicInteger eventIdGenerator = new AtomicInteger(0);
        final PriorityQueue<Event> queue = new PriorityQueue<>();
        final Random random;
        final Time time;
        final List<Invariant> invariants = new ArrayList<>();

        private EventScheduler(Random random, Time time) {
            this.random = random;
            this.time = time;
        }

        private void addInvariant(Invariant invariant) {
            invariants.add(invariant);
        }

        void schedule(Action action, int delayMs, int periodMs, int jitterMs) {
            long initialDeadlineMs = time.milliseconds() + delayMs;
            int eventId = eventIdGenerator.incrementAndGet();
            PeriodicEvent event = new PeriodicEvent(action, eventId, random, initialDeadlineMs, periodMs, jitterMs);
            queue.offer(event);
        }

        void runUntil(Supplier<Boolean> exitCondition) {
            while (!exitCondition.get()) {
                if (queue.isEmpty())
                    throw new IllegalStateException("Event queue exhausted before condition was satisfied");
                Event event = queue.poll();
                long delayMs = Math.max(event.deadlineMs - time.milliseconds(), 0);
                time.sleep(delayMs);
                event.execute(this);
                for (Invariant invariant : invariants)
                    invariant.verify();
            }
        }
    }

    private static class QuorumConfig {
        final int numVoters;
        final int numObservers;

        private QuorumConfig(int numVoters, int numObservers) {
            this.numVoters = numVoters;
            this.numObservers = numObservers;
        }

        private QuorumConfig(int numVoters) {
            this(numVoters, 0);
        }

    }

    private static class PersistentState {
        final MockQuorumStateStore store = new MockQuorumStateStore();
        final MockLog log = new MockLog();
    }

    private static class Cluster {
        final Random random;
        final AtomicInteger correlationIdCounter = new AtomicInteger();
        final Time time = new MockTime();
        final Set<Integer> voters = new HashSet<>();
        final Map<Integer, PersistentState> nodes = new HashMap<>();
        final Map<Integer, RaftNode> running = new HashMap<>();

        private Cluster(QuorumConfig config, int randomSeed) {
            this.random = new Random(randomSeed);

            int nodeId = 0;
            for (; nodeId < config.numVoters; nodeId++) {
                voters.add(nodeId);
                nodes.put(nodeId, new PersistentState());
            }

            for (; nodeId < config.numVoters + config.numObservers; nodeId++) {
                nodes.put(nodeId, new PersistentState());
            }
        }

        Set<Integer> nodes() {
            return nodes.keySet();
        }

        int randomNodeId() {
            return random.nextInt(nodes.size());
        }

        Set<Integer> observers() {
            Set<Integer> observers = new HashSet<>(nodes.keySet());
            observers.removeAll(voters);
            return observers;
        }

        Set<Integer> voters() {
            return voters;
        }

        OptionalLong leaderHighWatermark() {
            Optional<RaftNode> leaderWithMaxEpoch = running.values().stream().filter(node -> node.quorum.isLeader())
                    .max((node1, node2) -> Integer.compare(node2.quorum.epoch(), node1.quorum.epoch()));
            if (leaderWithMaxEpoch.isPresent()) {
                return leaderWithMaxEpoch.get().client.highWatermark();
            } else {
                return OptionalLong.empty();
            }
        }

        boolean anyReachedHighWatermark(long offset) {
            return running.values().stream()
                    .anyMatch(node -> node.quorum.highWatermark().orElse(0) > offset);
        }

        long maxHighWatermarkReached(Set<Integer> nodeIds) {
            return running.values().stream()
                .filter(node -> nodeIds.contains(node.nodeId))
                .map(node -> node.quorum.highWatermark().orElse(0))
                .max(Long::compareTo)
                .orElse(0L);
        }

        boolean allReachedHighWatermark(long offset, Set<Integer> nodeIds) {
            return nodeIds.stream()
                .allMatch(nodeId -> running.get(nodeId).quorum.highWatermark()
                    .orElse(0) > offset);
        }

        boolean allReachedHighWatermark(long offset) {
            return running.values().stream()
                .allMatch(node -> node.quorum.highWatermark().orElse(0) > offset);
        }

        boolean hasConsistentLeader() throws IOException {
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

        void kill(int nodeId) {
            running.remove(nodeId);
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

        void initializeElection(ElectionState election) {
            if (election.hasLeader() && !voters.contains(election.leaderId()))
                throw new IllegalArgumentException("Illegal election of observer " + election.leaderId());

            nodes.values().forEach(state -> {
                state.store.writeElectionState(election);
                if (election.hasLeader()) {
                    Optional<OffsetAndEpoch> endOffset = state.log.endOffsetForEpoch(election.epoch);
                    if (!endOffset.isPresent())
                        state.log.assignEpochStartOffset(election.epoch, state.log.endOffset());
                }
            });
        }

        void ifRunning(int nodeId, Consumer<RaftNode> action) {
            nodeIfRunning(nodeId).ifPresent(action);
        }

        void forRandomRunning(Consumer<RaftNode> action) {
            List<RaftNode> nodes = new ArrayList<>(running.values());
            if (!nodes.isEmpty()) {
                RaftNode randomNode = nodes.get(random.nextInt(nodes.size()));
                action.accept(randomNode);
            }
        }

        void withCurrentLeader(Consumer<RaftNode> action) {
            for (RaftNode node : running.values()) {
                if (node.quorum.isLeader()) {
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
            for (int voterId : nodes.keySet())
                start(voterId);
        }

        void start(int nodeId) {
            LogContext logContext = new LogContext("[Node " + nodeId + "] ");
            PersistentState persistentState = nodes.get(nodeId);
            MockNetworkChannel channel = new MockNetworkChannel(correlationIdCounter);
            QuorumState quorum = new QuorumState(nodeId, voters(), persistentState.store, logContext);

            // For the bootstrap server, we use a pretend VIP which internally routes
            // to any of the nodes randomly.
            List<InetSocketAddress> bootstrapServers = Collections.singletonList(
                new InetSocketAddress("localhost", 9000));

            KafkaRaftClient client = new KafkaRaftClient(channel, persistentState.log, quorum, time,
                new InetSocketAddress("localhost", 9990 + nodeId), bootstrapServers,
                ELECTION_TIMEOUT_MS, ELECTION_JITTER_MS, FETCH_TIMEOUT_MS, RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS,
                logContext, random);
            RaftNode node = new RaftNode(nodeId, client, persistentState.log, channel,
                    persistentState.store, quorum, logContext);
            node.initialize();
            running.put(nodeId, node);
        }
    }

    private static class RaftNode {
        final int nodeId;
        final KafkaRaftClient client;
        final MockLog log;
        final MockNetworkChannel channel;
        final MockQuorumStateStore store;
        final QuorumState quorum;
        final LogContext logContext;
        DistributedCounter counter;

        private RaftNode(int nodeId,
                         KafkaRaftClient client,
                         MockLog log,
                         MockNetworkChannel channel,
                         MockQuorumStateStore store,
                         QuorumState quorum,
                         LogContext logContext) {
            this.nodeId = nodeId;
            this.client = client;
            this.log = log;
            this.channel = channel;
            this.store = store;
            this.quorum = quorum;
            this.logContext = logContext;
        }

        void initialize() {
            this.counter = new DistributedCounter(client, logContext);
            try {
                counter.initialize();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void poll() {
            try {
                client.poll();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
                PersistentState state = nodeStateEntry.getValue();
                try {
                    nodeEpochs.put(nodeId, state.store.readElectionState().epoch);
                } catch (IOException e) {
                    fail("Unexpected IO exception from state store read" + e);
                }
            }
        }

        @Override
        public void verify() {
            for (Map.Entry<Integer, PersistentState> nodeStateEntry : cluster.nodes.entrySet()) {
                Integer nodeId = nodeStateEntry.getKey();
                PersistentState state = nodeStateEntry.getValue();
                Integer oldEpoch = nodeEpochs.get(nodeId);
                final Integer newEpoch;
                try {
                    newEpoch = state.store.readElectionState().epoch;
                } catch (IOException e) {
                    fail("Unexpected IO exception from state store read" + e);
                    break;
                }
                if (oldEpoch > newEpoch) {
                    fail("Non-monotonic update of high watermark detected: " +
                            oldEpoch + " -> " + newEpoch);
                }
                cluster.ifRunning(nodeId, nodeState -> {
                    assertEquals(newEpoch.intValue(), nodeState.quorum.epoch());
                });
                nodeEpochs.put(nodeId, newEpoch);
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

    private static class ConsistentCommittedData implements Invariant {
        final Cluster cluster;
        final Map<Long, Integer> committedSequenceNumbers = new HashMap<>();

        private ConsistentCommittedData(Cluster cluster) {
            this.cluster = cluster;
        }

        private int parseSequenceNumber(ByteBuffer value) {
            return (int) Type.INT32.read(value);
        }

        private void assertCommittedData(int nodeId, KafkaRaftClient manager, MockLog log) {
            OptionalLong highWatermark = manager.highWatermark();
            if (!highWatermark.isPresent()) {
                // We cannot do validation if the current high watermark is unknown
                return;
            }

            int nextExpectedSequence = 1;
            for (LogBatch batch : log.readBatches(0L, highWatermark)) {
                if (batch.isControlBatch) {
                    continue;
                }

                for (LogEntry entry : batch.entries) {
                    long offset = entry.offset;
                    assertTrue(offset < highWatermark.getAsLong());

                    int sequence = parseSequenceNumber(entry.record.value().duplicate());
                    assertEquals("Unexpected sequence found at offset " + offset + " on node " + nodeId,
                        nextExpectedSequence, sequence);

                    committedSequenceNumbers.putIfAbsent(offset, sequence);

                    int committedSequence = committedSequenceNumbers.get(offset);
                    assertEquals("Committed sequence at offset " + offset + " changed on node " + nodeId,
                        committedSequence, sequence);

                    nextExpectedSequence++;
                }
            }
        }

        @Override
        public void verify() {
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
            int correlationId = outbound.correlationId();
            int destinationId = outbound.destinationId();
            RaftRequest.Inbound inbound = new RaftRequest.Inbound(correlationId, outbound.data(),
                cluster.time.milliseconds());

            final int targetNodeId;
            if (destinationId < 0) {
                // We route requests to the bootstrap servers randomly
                targetNodeId = cluster.randomNodeId();
            } else {
                targetNodeId = destinationId;
            }

            if (!filters.get(targetNodeId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(targetNodeId).ifPresent(node -> {
                MockNetworkChannel destChannel = node.channel;
                inflight.put(correlationId, new InflightRequest(correlationId, senderId, targetNodeId));
                destChannel.mockReceive(inbound);
            });
        }

        void deliver(int senderId, RaftResponse.Outbound outbound) {
            int correlationId = outbound.correlationId();
            if (outbound.data instanceof FindQuorumResponseData)
                senderId = -1;

            RaftResponse.Inbound inbound = new RaftResponse.Inbound(correlationId, outbound.data(), senderId);
            InflightRequest inflightRequest = inflight.remove(correlationId);
            if (!filters.get(inflightRequest.sourceId).acceptInbound(inbound))
                return;

            cluster.nodeIfRunning(inflightRequest.sourceId).ifPresent(node -> {
                node.channel.mockReceive(inbound);
            });
        }

        void deliver(int senderId, RaftMessage message) {
            if (!filters.get(senderId).acceptOutbound(message)) {
                return;
            } else if (message instanceof RaftRequest.Outbound) {
                deliver(senderId, (RaftRequest.Outbound) message);
            } else if (message instanceof RaftResponse.Outbound) {
                deliver(senderId, (RaftResponse.Outbound) message);
            } else {
                throw new AssertionError("Illegal message type sent by node " + message);
            }
        }

        void filter(int nodeId, NetworkFilter filter) {
            filters.put(nodeId, filter);
        }

        void deliverRandom() {
            cluster.forRandomRunning(this::deliverTo);
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

}
