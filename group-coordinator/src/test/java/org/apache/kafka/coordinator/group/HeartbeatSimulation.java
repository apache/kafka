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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.MemberState;
import org.apache.kafka.clients.consumer.internals.SimConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A deterministic simulation of consumers using the consumer group protocol against the group
 * coordinator. The consumers run the real client code ({@link SimConsumer}) and the coordinator
 * the real shard ({@link HeartbeatSimCoordinator}); the simulation owns the clock, the network,
 * the workload and the faults, and checks invariants after every step.
 */
public class HeartbeatSimulation {

    static final String GROUP_ID = "group";
    static final int NUM_BROKERS = 3;

    /**
     * The parameters of a run, drawn from the seed.
     */
    public static final class Params {
        String profile;
        Map<String, Integer> topics = new TreeMap<>();
        int initialConsumers;
        int maxConsumers;
        double staticProbability;
        /**
         * auto: enable.auto.commit; manual: the application commits after processing the records
         * of each poll; none: no commits (scripted scenarios commit explicitly).
         */
        String commitMode;
        int autoCommitIntervalMs;
        String assignor;
        int sessionTimeoutMs;
        int heartbeatIntervalMs;
        int assignmentIntervalMs;
        int maxPollIntervalMs;
        int requestTimeoutMs;
        int retryBackoffMs;
        long chaosMs;
        long settleMs;

        // Faults during the chaos phase.
        double slowNetwork;
        double verySlowNetwork;
        double disconnectPerSecond;
        double failoverPerSecond;
        double appendFailure;
        double slowReplication;
        double slowCallback;
        double longProcessingPerSecond;
        // Workload during the chaos phase.
        double crashPerSecond;
        double closePerSecond;
        double startPerSecond;
        double unsubscribePerSecond;
        double changeSubscriptionPerSecond;

        static final List<String> PROFILES = List.of("quiet", "workload", "network", "coordinator", "chaos");

        /**
         * Draws the parameters from the random generator. The profile and the commit mode are
         * drawn too unless they are given.
         */
        static Params random(Random rng, Optional<String> profile, Optional<String> commitMode, Set<String> disabled) {
            Params p = new Params();
            p.profile = profile.orElseGet(() -> PROFILES.get(rng.nextInt(PROFILES.size())));
            int numTopics = 1 + rng.nextInt(2);
            for (int i = 0; i < numTopics; i++) {
                p.topics.put("t" + i, 1 + rng.nextInt(6));
            }
            p.initialConsumers = 1 + rng.nextInt(4);
            p.maxConsumers = p.initialConsumers + rng.nextInt(3);
            p.staticProbability = rng.nextInt(3) == 0 ? 0.5 : 0.0;
            String drawnCommitMode = rng.nextBoolean() ? "auto" : "manual";
            p.commitMode = commitMode.orElse(drawnCommitMode);
            p.autoCommitIntervalMs = 500 + rng.nextInt(2000);
            p.assignor = rng.nextBoolean() ? "uniform" : "range";
            p.sessionTimeoutMs = 6000 + rng.nextInt(4) * 1000;
            p.heartbeatIntervalMs = 500 + rng.nextInt(4) * 250;
            p.assignmentIntervalMs = rng.nextBoolean() ? 0 : 1000;
            p.maxPollIntervalMs = 8000 + rng.nextInt(5) * 1000;
            p.requestTimeoutMs = 3000 + rng.nextInt(3) * 1000;
            p.retryBackoffMs = 100;
            p.chaosMs = 60_000 + rng.nextInt(120_000);
            p.settleMs = 60_000;

            if (!p.profile.equals("quiet")) {
                p.drawWorkload(rng);
            }
            p.drawFaults(rng);
            disabled.forEach(p::disable);
            return p;
        }

        private void drawFaults(Random rng) {
            if (profile.equals("network") || profile.equals("chaos")) {
                slowNetwork = rng.nextDouble() * 0.1;
                verySlowNetwork = rng.nextDouble() * 0.01;
                disconnectPerSecond = rng.nextDouble() * 0.05;
            }
            if (profile.equals("coordinator") || profile.equals("chaos")) {
                failoverPerSecond = rng.nextDouble() * 0.03;
                appendFailure = rng.nextDouble() * 0.05;
                slowReplication = rng.nextDouble() * 0.05;
            }
        }

        private void drawWorkload(Random rng) {
            crashPerSecond = 0.01 + rng.nextDouble() * 0.03;
            closePerSecond = 0.01 + rng.nextDouble() * 0.03;
            startPerSecond = 0.02 + rng.nextDouble() * 0.05;
            unsubscribePerSecond = rng.nextDouble() * 0.03;
            changeSubscriptionPerSecond = topics.size() > 1 ? rng.nextDouble() * 0.03 : 0;
            longProcessingPerSecond = rng.nextDouble() * 0.02;
            slowCallback = rng.nextDouble() * 0.2;
        }

        /**
         * Turns off a kind of fault or workload.
         */
        private void disable(String name) {
            switch (name) {
                case "longProcessing":
                    longProcessingPerSecond = 0;
                    break;
                case "slowCallback":
                    slowCallback = 0;
                    break;
                case "appendFailure":
                    appendFailure = 0;
                    break;
                case "slowReplication":
                    slowReplication = 0;
                    break;
                case "failover":
                    failoverPerSecond = 0;
                    break;
                case "disconnect":
                    disconnectPerSecond = 0;
                    break;
                case "slowNetwork":
                    slowNetwork = 0;
                    verySlowNetwork = 0;
                    break;
                case "crash":
                    crashPerSecond = 0;
                    break;
                case "close":
                    closePerSecond = 0;
                    break;
                case "unsubscribe":
                    unsubscribePerSecond = 0;
                    break;
                case "changeSubscription":
                    changeSubscriptionPerSecond = 0;
                    break;
                case "static":
                    staticProbability = 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown fault or workload " + name);
            }
        }

        /**
         * Parameters for scripted scenarios: fixed timeouts and no random faults or workload.
         */
        static Params scripted(Map<String, Integer> topics, String commitMode) {
            Params p = scripted(topics);
            p.commitMode = commitMode;
            p.autoCommitIntervalMs = 1000;
            return p;
        }

        static Params scripted(Map<String, Integer> topics) {
            Params p = new Params();
            p.profile = "scripted";
            p.topics.putAll(topics);
            p.commitMode = "none";
            p.autoCommitIntervalMs = 5000;
            p.assignor = "range";
            p.sessionTimeoutMs = 6000;
            p.heartbeatIntervalMs = 500;
            p.assignmentIntervalMs = 0;
            p.maxPollIntervalMs = 10_000;
            p.requestTimeoutMs = 5000;
            p.retryBackoffMs = 100;
            return p;
        }

        boolean autoCommit() {
            return commitMode.equals("auto");
        }

        @Override
        public String toString() {
            return "profile=" + profile + " topics=" + topics + " consumers=" + initialConsumers + ".." + maxConsumers +
                " static=" + staticProbability + " commits=" + commitMode + " assignor=" + assignor +
                " session=" + sessionTimeoutMs + " hb=" + heartbeatIntervalMs + " assignmentInterval=" + assignmentIntervalMs +
                " maxPoll=" + maxPollIntervalMs + " requestTimeout=" + requestTimeoutMs + " chaos=" + chaosMs;
        }
    }

    /**
     * The outcome of a run: the violations, the end of the trace and counters of what happened.
     */
    public record Result(long seed, Params params, List<String> violations, List<String> trace, Map<String, Integer> stats) { }

    /** A run stops after this many violations. */
    private static final int MAX_VIOLATIONS = 20;

    private record Event(long timeMs, long seq, String description, Runnable action) { }

    private final long seed;
    private final Random rng;
    private final Params params;
    private final MockTime time;
    private final PriorityQueue<Event> events = new PriorityQueue<>(
        Comparator.comparingLong(Event::timeMs).thenComparingLong(Event::seq));
    private long seq;
    private final long startMs;
    private boolean chaos = true;
    private boolean verbose;
    private final boolean scripted;
    private final List<RequestRule> requestRules = new ArrayList<>();
    private String currentRequest = "";
    private boolean currentRequestAbandoned;
    private int writeCount;
    private final Map<Integer, List<Runnable>> writeActions = new HashMap<>();
    private final List<Predicate<String>> appendFailures = new ArrayList<>();

    private final Map<String, Uuid> topicIds = new TreeMap<>();
    private final HeartbeatSimCoordinator coordinator;
    private final MetadataResponse metadataResponse;
    private final List<Driver> drivers = new ArrayList<>();
    private int consumerCounter;
    private final Map<String, Integer> instanceIds = new TreeMap<>();

    private final List<String> violations = new ArrayList<>();
    private final Set<String> reportedViolations = new HashSet<>();
    private final Deque<String> trace = new ArrayDeque<>();
    private final Map<String, Integer> stats = new TreeMap<>();
    private static final int TRACE_SIZE = 3000;

    /**
     * A simulation with random parameters, workload and faults drawn from the seed.
     *
     * @param profile    The profile, drawn from the seed if empty.
     * @param commitMode The commit mode (auto or manual), drawn from the seed if empty.
     * @param disabled   The kinds of faults and workload that are turned off.
     */
    public HeartbeatSimulation(long seed, Optional<String> profile, Optional<String> commitMode, Set<String> disabled) {
        this(seed, new Random(seed), profile, commitMode, disabled);
    }

    private HeartbeatSimulation(long seed, Random rng, Optional<String> profile, Optional<String> commitMode, Set<String> disabled) {
        this(seed, rng, Params.random(rng, profile, commitMode, disabled), false);
    }

    /**
     * A simulation for a scripted scenario: the test starts the consumers and runs the
     * simulation up to the points it wants to check.
     */
    static HeartbeatSimulation scripted(Params params) {
        HeartbeatSimulation simulation = new HeartbeatSimulation(0, new Random(0), params, true);
        simulation.chaos = false;
        simulation.coordinator.elect(0, 0);
        return simulation;
    }

    private HeartbeatSimulation(long seed, Random rng, Params params, boolean scripted) {
        this.seed = seed;
        this.rng = rng;
        this.params = params;
        this.scripted = scripted;
        this.time = new MockTime(0, 1_000_000L, 0L);
        this.startMs = time.milliseconds();

        MetadataImageBuilder imageBuilder = new MetadataImageBuilder();
        params.topics.forEach((name, partitions) -> {
            Uuid id = new Uuid(rng.nextLong(), rng.nextLong());
            topicIds.put(name, id);
            imageBuilder.addTopic(id, name, partitions);
        });
        this.metadataResponse = RequestTestUtils.metadataUpdateWithIds(NUM_BROKERS, params.topics, topicIds);

        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, params.sessionTimeoutMs);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 1000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, params.heartbeatIntervalMs);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, params.assignmentIntervalMs);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(
            params.assignor.equals("uniform") ? "uniform" : "range",
            params.assignor.equals("uniform") ? "range" : "uniform"));
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 5000);
        this.coordinator = new HeartbeatSimCoordinator(
            time,
            new CoordinatorHooks(),
            configs,
            imageBuilder.buildCoordinatorMetadataImage(),
            topicIds
        );
    }

    public HeartbeatSimulation verbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    /**
     * Sets the level of the loggers. The clients and the coordinator log at debug level in the
     * tests of this module, which slows the simulation down by orders of magnitude.
     */
    static void setLogLevel(Level level) {
        Configurator.setRootLevel(level);
        Configurator.setLevel("org.apache.kafka", level);
    }

    /**
     * Restores the levels of the loggers from the configuration file.
     */
    static void resetLogLevel() {
        Configurator.reconfigure();
    }

    // ---------------------------------------------------------------------------------------------
    // Scheduler.
    // ---------------------------------------------------------------------------------------------

    private void schedule(long delayMs, String description, Runnable action) {
        events.add(new Event(time.milliseconds() + Math.max(0, delayMs), seq++, description, action));
    }

    private long now() {
        return time.milliseconds() - startMs;
    }

    private void trace(String line) {
        String entry = String.format("%7d %s", now(), line);
        if (verbose) System.out.println(entry);
        trace.addLast(entry);
        if (trace.size() > TRACE_SIZE) trace.removeFirst();
    }

    private void count(String stat) {
        stats.merge(stat, 1, Integer::sum);
    }

    /**
     * The kinds of violations that known issues of the protocol cause. A run reports them, but
     * only the other kinds fail it. {@link HeartbeatScenariosTest} reproduces the issues.
     */
    static final Set<String> KNOWN_ISSUES = Set.of(
        "stale-assignment-applied",
        "released-while-held",
        "released-by-stale-join",
        "released-by-stale-heartbeat",
        "stale-request-processed",
        "failed-leave-ghost",
        "remain-in-group-left",
        "remain-in-group-exception",
        "member-error-thrown",
        "callback-released-unowned",
        "commit-not-owned"
    );

    /**
     * Violations that are the consequences of an issue: they are explained when every consumer
     * they involve had a known issue before.
     */
    private static final Set<String> SYMPTOMS = Set.of(
        "double-assignment",
        "double-callback-ownership",
        "held-not-assigned",
        "callback-assigned-twice",
        "convergence"
    );

    private final Map<String, Set<String>> issuesByConsumer = new HashMap<>();

    private void violation(String kind, String detail) {
        if (KNOWN_ISSUES.contains(kind)) {
            for (String name : consumerNames(detail)) issuesByConsumer.computeIfAbsent(name, __ -> new TreeSet<>()).add(kind);
        }
        if (SYMPTOMS.contains(kind)) {
            Set<String> names = consumerNames(detail);
            // One consumer holding a partition that it does not own explains a double ownership.
            if (!names.isEmpty() && names.stream().anyMatch(issuesByConsumer::containsKey)) {
                count("explained:" + kind);
                return;
            }
            if (kind.equals("convergence") && issuesByConsumer.values().stream().anyMatch(i -> i.contains("failed-leave-ghost"))) {
                count("explained:" + kind);
                return;
            }
        }
        count("violation:" + kind);
        if (reportedViolations.add(kind + ":" + detail)) {
            String entry = String.format("[%d ms] %s: %s", now(), kind, detail);
            violations.add(entry);
            trace("VIOLATION " + entry);
        }
    }

    public Result run() {
        coordinator.elect(rng.nextInt(NUM_BROKERS), 0);
        for (int i = 0; i < params.initialConsumers; i++) {
            startConsumer(null, subscriptionFor(), rng.nextInt(2000));
        }
        schedule(1000, "chaos tick", this::chaosTick);
        schedule(params.chaosMs, "end of chaos", this::endChaos);
        long endMs = time.milliseconds() + params.chaosMs + params.settleMs;

        while (!events.isEmpty() && violations.size() < MAX_VIOLATIONS) {
            Event event = events.poll();
            if (event.timeMs() > endMs) break;
            if (event.timeMs() > time.milliseconds()) time.sleep(event.timeMs() - time.milliseconds());
            try {
                event.action().run();
            } catch (Throwable t) {
                exception(event.description(), t);
            }
            check();
        }
        if (violations.size() < MAX_VIOLATIONS) checkConvergence();
        return new Result(seed, params, violations, new ArrayList<>(trace), stats);
    }

    // ---------------------------------------------------------------------------------------------
    // Scripted scenarios.
    // ---------------------------------------------------------------------------------------------

    /**
     * A one-shot rule for the next request of a consumer that matches: it arrives after the
     * delay, or it is lost if the delay is negative.
     */
    private record RequestRule(String consumer, ApiKeys apiKey, Predicate<AbstractRequest> matches, long delayMs,
                               boolean loseResponse) { }

    Driver start(String name, String instanceId, Set<String> topics) {
        return startConsumer(name, instanceId, topics, 0);
    }

    /** Runs the simulation until the given time since the start. */
    void runUntil(long timeMs) {
        long endMs = startMs + timeMs;
        while (!events.isEmpty() && events.peek().timeMs() <= endMs) {
            Event event = events.poll();
            if (event.timeMs() > time.milliseconds()) time.sleep(event.timeMs() - time.milliseconds());
            try {
                event.action().run();
            } catch (Throwable t) {
                exception(event.description(), t);
            }
            check();
        }
        if (time.milliseconds() < endMs) time.sleep(endMs - time.milliseconds());
    }

    void at(long timeMs, String description, Runnable action) {
        schedule(startMs + timeMs - time.milliseconds(), description, action);
    }

    /** The next request of the consumer with the api key that matches arrives after the delay. */
    void delayRequest(Driver driver, ApiKeys apiKey, Predicate<AbstractRequest> matches, long delayMs) {
        delayRequest(driver.name, apiKey, matches, delayMs);
    }

    /** The next request of the consumer with the api key that matches arrives after the delay, or is lost if negative. */
    void delayRequest(String consumer, ApiKeys apiKey, Predicate<AbstractRequest> matches, long delayMs) {
        requestRules.add(new RequestRule(consumer, apiKey, matches, delayMs, false));
    }

    /** The next request of the consumer with the api key that matches is processed but its response is lost. */
    void loseResponse(String consumer, ApiKeys apiKey, Predicate<AbstractRequest> matches) {
        requestRules.add(new RequestRule(consumer, apiKey, matches, 0, true));
    }

    /** Runs the action after the coordinator wrote the given number of batches. */
    void afterWrite(int count, Runnable action) {
        writeActions.computeIfAbsent(count, __ -> new ArrayList<>()).add(action);
    }

    int writeCount() {
        return writeCount;
    }

    Optional<Driver> driver(String name) {
        return drivers.stream().filter(d -> d.name.equals(name)).findFirst();
    }

    /** The coordinator moves now; the new leader keeps the uncommitted batches or not. */
    void failover(int node, long electionDelayMs, boolean keepUncommitted) {
        // A failover is already in progress.
        if (coordinator.status() == HeartbeatSimCoordinator.Status.NONE) return;
        coordinator.resign(candidates -> keepUncommitted ? candidates.get(candidates.size() - 1) : candidates.get(0));
        trace("coordinator resigns " + (keepUncommitted ? "keeping" : "dropping") + " the uncommitted batches; node " +
            node + " elected in " + electionDelayMs + " ms");
        schedule(electionDelayMs, "elect", () -> coordinator.elect(node, 0));
    }

    /** The requests that the consumer sends to the brokers between the two times are lost. */
    void partition(Driver driver, long fromMs, long toMs) {
        driver.partitionedFromMs = startMs + fromMs;
        driver.partitionedToMs = startMs + toMs;
    }

    /** The append of the next write whose request description matches fails. */
    void failAppend(Predicate<String> request) {
        appendFailures.add(request);
    }

    void checkConvergenceNow() {
        checkConvergence();
    }

    List<String> violations() {
        return violations;
    }

    List<String> trace() {
        return new ArrayList<>(trace);
    }

    Optional<ConsumerGroup> group() {
        return coordinator.consumerGroup(GROUP_ID);
    }

    private void exception(String description, Throwable t) {
        if (t instanceof IllegalStateException && String.valueOf(t.getMessage()).equals("Invalid state transition from LEAVING to PREPARE_LEAVING")) {
            violation("remain-in-group-exception", description + ": " + t.getMessage());
        } else {
            violation("exception", description + ": " + stackTrace(t));
        }
    }

    private Set<String> consumerNames(String detail) {
        Set<String> names = new TreeSet<>();
        Matcher matcher = Pattern.compile("\\b(C\\d+)\\b").matcher(detail);
        while (matcher.find()) names.add(matcher.group(1));
        // Member ids of the convergence check.
        for (Driver d : drivers) {
            if (detail.contains(d.consumer.memberId())) names.add(d.name);
        }
        return names;
    }

    private static String stackTrace(Throwable t) {
        StringWriter writer = new StringWriter();
        t.printStackTrace(new PrintWriter(writer));
        String[] lines = writer.toString().split("\n");
        return String.join("\n", Arrays.copyOf(lines, Math.min(lines.length, 12)));
    }

    // ---------------------------------------------------------------------------------------------
    // Coordinator hooks and faults.
    // ---------------------------------------------------------------------------------------------

    private final class CoordinatorHooks implements HeartbeatSimCoordinator.Hooks {
        @Override
        public void schedule(long delayMs, String description, Runnable action) {
            HeartbeatSimulation.this.schedule(delayMs, description, action);
        }

        @Override
        public long replicationDelayMs() {
            if (scripted) return 1;
            if (chaos && rng.nextDouble() < params.slowReplication) return 1000 + rng.nextInt(8000);
            return 1 + rng.nextInt(10);
        }

        @Override
        public boolean failAppend() {
            if (scripted) {
                for (Iterator<Predicate<String>> it = appendFailures.iterator(); it.hasNext(); ) {
                    if (it.next().test(currentRequest)) {
                        it.remove();
                        trace("coordinator append fails for " + currentRequest);
                        markLeaveAppendFailure();
                        return true;
                    }
                }
                return false;
            }
            boolean fail = chaos && rng.nextDouble() < params.appendFailure;
            if (fail) {
                count("fault:append-failure");
                trace("coordinator append fails for " + currentRequest);
                markLeaveAppendFailure();
            }
            return fail;
        }

        @Override
        public void onInternalError(String description, Throwable error) {
            violation("coordinator-error", description + ": " + stackTrace(error));
        }

        @Override
        public void onWriteApplied(String description) {
            writeCount++;
            List<Runnable> actions = writeActions.remove(writeCount);
            if (actions != null) actions.forEach(action -> schedule(0, "after write " + writeCount, action));
        }

        @Override
        public void onHeartbeatApplied(ConsumerGroupHeartbeatRequestData request, ConsumerGroupMember before, ConsumerGroupMember after) {
            if (before == null || after == null) return;
            Driver driver = drivers.stream().filter(d -> d.consumer.memberId().equals(request.memberId())).findFirst().orElse(null);
            if (driver == null || driver.consumer.isClosed()) return;
            if (currentRequestAbandoned && (!before.assignedPartitions().equals(after.assignedPartitions()) ||
                !before.partitionsPendingRevocation().equals(after.partitionsPendingRevocation()))) {
                // A heartbeat that the client gave up on (its connection timed out) changed the
                // assignment of the member: the client does not get the response.
                violation("stale-request-processed", driver.name + " heartbeat at epoch " + request.memberEpoch() +
                    " processed after the client gave up on it; the client is " + driver.consumer.state() + " at epoch " +
                    driver.consumer.memberEpoch() + "; the member goes from " + describe(before.assignedPartitions()) + " to " +
                    describe(after.assignedPartitions()) + " at epoch " + after.memberEpoch());
            }
            // Leaving members release their partitions.
            if (request.memberEpoch() < 0 || driver.consumer.memberEpoch() < 0) return;
            checkReleasedPartitions(driver, request, before, after);
        }

        /**
         * Checks that the heartbeat did not release partitions from the member while its client
         * holds them or is assigning them.
         */
        private void checkReleasedPartitions(
            Driver driver,
            ConsumerGroupHeartbeatRequestData request,
            ConsumerGroupMember before,
            ConsumerGroupMember after
        ) {
            Set<TopicPartition> held = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
            held.addAll(driver.consumer.assignedPartitions());
            held.addAll(driver.consumer.ownedByCallbacks());
            after.assignedPartitions().forEach((topicId, partitions) ->
                partitions.keySet().forEach(p -> driver.releasedWhileTargeted.remove(new TopicPartition(topicName(topicId), p))));
            Set<TopicPartition> targeted = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
            driver.target.forEach((topicId, partitions) ->
                partitions.forEach(p -> targeted.add(new TopicPartition(topicName(topicId), p))));
            if (driver.consumer.reconciliationInProgress()) {
                // The partitions that the reconciliation in progress is assigning.
                held.addAll(targeted);
            } else {
                // The client may start to reconcile its target before it receives the response.
                for (TopicPartition tp : targeted) {
                    if (released(tp, before, after)) driver.releasedWhileTargeted.add(tp);
                }
            }
            for (TopicPartition tp : held) {
                if (!released(tp, before, after)) continue;
                String kind = request.memberEpoch() == 0 ? "join" : "heartbeat";
                boolean stale = driver.consumer.memberEpoch() != request.memberEpoch() || driver.consumer.state() == MemberState.JOINING;
                violation(stale ? "released-by-stale-" + kind : "released-while-held",
                    tp + " released from " + driver.name + " by a " + kind + " at epoch " + request.memberEpoch() +
                    " with owned " + (request.topicPartitions() == null ? "null" : describe(request.topicPartitions())) +
                    "; client " + driver.consumer + " target " + driver.target +
                    " reconciling=" + driver.consumer.reconciliationInProgress());
            }
        }

        private boolean released(TopicPartition tp, ConsumerGroupMember before, ConsumerGroupMember after) {
            Uuid topicId = topicIds.get(tp.topic());
            return owns(before, topicId, tp.partition()) && !owns(after, topicId, tp.partition());
        }

        @Override
        public void onOffsetCommit(OffsetCommitRequestData request, OffsetCommitResponseData response, Optional<ConsumerGroup> group) {
            // The coordinator accepts the commit of a member only for the partitions that the member
            // owns: assigned or pending revocation. Commits with a negative epoch come from outside
            // the group (an admin client, or a consumer that left), which only empty groups accept.
            if (request.generationIdOrMemberEpoch() < 0) {
                count("commit-from-outside-the-group");
                return;
            }
            ConsumerGroupMember member = group.map(g -> g.members().get(request.memberId())).orElse(null);
            String committer = drivers.stream().filter(d -> d.consumer.memberId().equals(request.memberId()))
                .map(d -> d.name).findFirst().orElse(request.memberId());
            for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : response.topics()) {
                Uuid topicId = topicIds.get(topic.name());
                for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                    if (partition.errorCode() != Errors.NONE.code()) continue;
                    if (owns(member, topicId, partition.partitionIndex())) continue;
                    String owner = group.flatMap(g -> g.members().values().stream()
                        .filter(m -> owns(m, topicId, partition.partitionIndex()))
                        .map(m -> drivers.stream().filter(d -> d.consumer.memberId().equals(m.memberId())).map(d -> d.name)
                            .findFirst().orElse(m.memberId()))
                        .findFirst()).orElse("nobody");
                    violation("commit-not-owned", committer + " committed " + topic.name() + "-" + partition.partitionIndex() +
                        " at epoch " + request.generationIdOrMemberEpoch() + " (member " +
                        (member == null ? "unknown" : "epoch " + member.memberEpoch()) + ") while owned by " + owner +
                        (currentRequestAbandoned ? ", in a request the client gave up on" : ""));
                }
            }
        }
    }

    private void markLeaveAppendFailure() {
        Matcher matcher = Pattern.compile("^(C\\d+) CONSUMER_GROUP_HEARTBEAT epoch -[12]$").matcher(currentRequest);
        if (matcher.matches()) {
            drivers.stream().filter(d -> d.name.equals(matcher.group(1))).forEach(d -> d.leaveAppendFailed = true);
        }
    }

    private void failover() {
        count("fault:failover");
        coordinator.resign(candidates -> candidates.get(rng.nextInt(candidates.size())));
        long electionDelay = rng.nextInt(2000);
        int node = rng.nextInt(NUM_BROKERS);
        long loadDuration = rng.nextInt(2000);
        trace("coordinator resigns; log kept up to " + coordinator.logEndOffset() + "; node " + node + " elected in " +
            electionDelay + " ms, loads in " + loadDuration + " ms");
        schedule(electionDelay, "elect", () -> coordinator.elect(node, loadDuration));
    }

    private long networkDelayMs() {
        if (scripted) return 1;
        if (chaos) {
            if (rng.nextDouble() < params.verySlowNetwork) return params.requestTimeoutMs + rng.nextInt(10_000);
            if (rng.nextDouble() < params.slowNetwork) return 50 + rng.nextInt(1000);
        }
        return 1 + rng.nextInt(3);
    }

    // ---------------------------------------------------------------------------------------------
    // Workload.
    // ---------------------------------------------------------------------------------------------

    private Set<String> subscriptionFor() {
        List<String> topics = new ArrayList<>(params.topics.keySet());
        if (topics.size() == 1 || rng.nextInt(3) > 0) return new TreeSet<>(topics);
        return new TreeSet<>(Set.of(topics.get(rng.nextInt(topics.size()))));
    }

    private List<Driver> running() {
        return drivers.stream().filter(d -> !d.consumer.isClosed() && !d.stopping).toList();
    }

    /**
     * Every second of the faulty phase, draws the workload and the faults of that second.
     */
    private void chaosTick() {
        if (!chaos) return;
        workloadTick();
        pickRunning(params.disconnectPerSecond).ifPresent(d -> {
            if (coordinator.coordinatorNodeId() >= 0) {
                count("fault:disconnect");
                String destination = "+" + coordinator.coordinatorNodeId();
                trace(d.name + " disconnects from " + destination);
                d.disconnect(destination);
            }
        });
        if (coordinator.status() != HeartbeatSimCoordinator.Status.NONE && rng.nextDouble() < params.failoverPerSecond) {
            failover();
        }
        schedule(1000, "chaos tick", this::chaosTick);
    }

    private void workloadTick() {
        pickRunning(params.crashPerSecond).ifPresent(d -> {
            trace(d.name + " crashes");
            count("workload:crash");
            d.consumer.crash();
            maybeRestart(d);
        });
        pickRunning(params.closePerSecond).ifPresent(d -> {
            count("workload:close");
            d.actions.add(d::close);
            maybeRestart(d);
        });
        if (running().size() < params.maxConsumers && rng.nextDouble() < params.startPerSecond) {
            count("workload:start");
            startConsumer(null, subscriptionFor(), 0);
        }
        pickRunning(params.unsubscribePerSecond).ifPresent(d -> {
            count("workload:unsubscribe");
            d.actions.add(d::unsubscribe);
            Set<String> topics = subscriptionFor();
            schedule(1000 + rng.nextInt(10_000), "resubscribe", () -> d.actions.add(() -> d.subscribe(topics)));
        });
        pickRunning(params.changeSubscriptionPerSecond).ifPresent(d -> {
            count("workload:change-subscription");
            Set<String> topics = subscriptionFor();
            d.actions.add(() -> d.subscribe(topics));
        });
        pickRunning(params.longProcessingPerSecond).ifPresent(d -> {
            count("workload:long-processing");
            d.longProcessing = true;
        });
    }

    /**
     * @return A running consumer drawn at random, with the given probability.
     */
    private Optional<Driver> pickRunning(double probability) {
        List<Driver> running = running();
        if (running.isEmpty() || rng.nextDouble() >= probability) return Optional.empty();
        return Optional.of(running.get(rng.nextInt(running.size())));
    }

    private void maybeRestart(Driver d) {
        if (rng.nextDouble() < 0.7) {
            long delay = 500 + rng.nextInt(params.sessionTimeoutMs * 2);
            Set<String> topics = d.subscription;
            String instanceId = d.instanceId;
            schedule(delay, "restart", () -> {
                if (!chaos) return;
                trace("restart of " + d.name + (instanceId != null ? " as " + instanceId : ""));
                startConsumer(instanceId, topics, 0);
            });
        }
    }

    private void snapshot() {
        for (Driver d : drivers) {
            if (d.consumer.isClosed()) continue;
            trace("state " + d.consumer + " " + d.consumer.networkState() + " lastNetworkRun=" + (d.lastNetworkRunMs - startMs) +
                " nextNetworkRun=" + (d.nextNetworkRunMs == Long.MAX_VALUE ? "none" : String.valueOf(d.nextNetworkRunMs - startMs)) +
                " nextAppStep=" + (d.nextAppStepMs == Long.MAX_VALUE ? "none" : String.valueOf(d.nextAppStepMs - startMs)));
        }
        schedule(10_000, "snapshot", this::snapshot);
    }

    private void endChaos() {
        chaos = false;
        trace("end of chaos");
        snapshot();
        for (Driver d : drivers) {
            d.longProcessing = false;
            if (!d.consumer.isClosed() && !d.stopping && d.subscription.isEmpty()) {
                Set<String> topics = subscriptionFor();
                d.actions.add(() -> d.subscribe(topics));
            }
        }
    }

    private void startConsumer(String instanceId, Set<String> topics, long delayMs) {
        startConsumer("C" + (++consumerCounter), instanceId, topics, delayMs);
    }

    private Driver startConsumer(String name, String instanceId, Set<String> topics, long delayMs) {
        if (instanceId == null && rng.nextDouble() < params.staticProbability) {
            instanceId = "instance-" + name;
        }
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, params.autoCommit());
        configs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, params.autoCommitIntervalMs);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, params.maxPollIntervalMs);
        configs.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, params.requestTimeoutMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, params.retryBackoffMs);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, params.retryBackoffMs);
        configs.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        configs.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 3_600_000);
        if (instanceId != null) configs.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        String memberId = new Uuid(rng.nextLong(), rng.nextLong()).toString();
        Driver driver = new Driver(name, instanceId);
        if (params.commitMode.equals("manual")) {
            String[] styles = {"sync-consumed", "async-consumed", "sync-tracked", "async-tracked"};
            if (scripted) {
                // Deterministic in scripted scenarios: odd consumers commit tracked offsets asynchronously,
                // including once after a revocation, even ones commit the consumed positions synchronously.
                int number = Integer.parseInt(name.replaceAll("\\D", "").isEmpty() ? "1" : name.replaceAll("\\D", ""));
                driver.commitStyle = number % 2 == 1 ? "async-tracked" : "sync-consumed";
                driver.commitsAfterRevocation = number % 2 == 1;
            } else {
                driver.commitStyle = styles[rng.nextInt(styles.length)];
                driver.commitsAfterRevocation = driver.commitStyle.endsWith("tracked") && rng.nextInt(3) == 0;
            }
        }
        SimConsumer consumer = new SimConsumer(name, memberId, time, configs, metadataResponse, driver);
        driver.consumer = consumer;
        driver.subscription = topics;
        drivers.add(driver);
        trace("start " + name + " member " + memberId.substring(0, 4) + (instanceId != null ? " instance " + instanceId : "") +
            " subscribing to " + topics);
        driver.actions.add(() -> driver.subscribe(topics));
        driver.scheduleAppStep(delayMs);
        driver.scheduleNetworkRun(delayMs);
        return driver;
    }

    /**
     * The application of one consumer and its network.
     */
    final class Driver implements SimConsumer.Hooks {
        final String name;
        final String instanceId;
        SimConsumer consumer;
        Set<String> subscription = Set.of();
        final Deque<Runnable> actions = new ArrayDeque<>();
        boolean longProcessing;
        boolean stopping;
        // Manual commits: what the application commits, and the offsets of the records it processed.
        String commitStyle = "none";
        boolean commitsAfterRevocation;
        boolean commitDue;
        final Map<TopicPartition, Long> processed = new HashMap<>();
        final Set<TopicPartition> releasedButTracked = new HashSet<>();
        long pausedUntilMs;
        int heartbeatsSent;
        boolean leaveAppendFailed;
        final Set<TopicPartition> releasedWhileTargeted = new HashSet<>();
        // The last assignment delivered to the client in a heartbeat response: the target it
        // reconciles towards, until it leaves the group or is fenced.
        Map<Uuid, Set<Integer>> target = Map.of();
        final Set<TopicPartition> callbackOwned = new HashSet<>();
        long lastNetworkRunMs;
        long networkStalledUntilMs;
        long partitionedFromMs = -1;
        long partitionedToMs = -1;
        long scriptedCallbackDurationMs;
        final List<String> callbacks = new ArrayList<>();
        CloseOptions.GroupMembershipOperation closeOperation;
        long nextNetworkRunMs = Long.MAX_VALUE;
        long nextAppStepMs = Long.MAX_VALUE;
        int lastPositiveEpoch;
        final Map<String, Connection> connections = new HashMap<>();
        final List<InFlight> inFlight = new ArrayList<>();

        Driver(String name, String instanceId) {
            this.name = name;
            this.instanceId = instanceId;
        }

        // Network thread.

        void scheduleNetworkRun(long delayMs) {
            long at = time.milliseconds() + delayMs;
            if (at >= nextNetworkRunMs) return;
            nextNetworkRunMs = at;
            schedule(delayMs, name + " network thread", () -> {
                if (time.milliseconds() < nextNetworkRunMs) return;
                nextNetworkRunMs = Long.MAX_VALUE;
                networkRun();
            });
        }

        /** The network thread does not run for the duration, e.g. it does not get the CPU. */
        void stallNetworkThread(long durationMs) {
            networkStalledUntilMs = time.milliseconds() + durationMs;
            trace(name + " network thread stalls for " + durationMs + " ms");
        }

        void networkRun() {
            if (consumer.isClosed()) return;
            if (time.milliseconds() < networkStalledUntilMs) {
                scheduleNetworkRun(networkStalledUntilMs - time.milliseconds());
                return;
            }
            lastNetworkRunMs = time.milliseconds();
            // NetworkClient#handleTimedOutRequests: a timed out request closes the connection to
            // its node, which fails all the requests in flight to that node. MockClient only
            // looks at its oldest request and then drops the next one without a response, so
            // the timeouts are handled here before the network thread polls the client.
            long nowMs = time.milliseconds();
            Set<String> timedOut = new LinkedHashSet<>();
            for (ClientRequest request : consumer.client().requests()) {
                if (nowMs - request.createdTimeMs() >= request.requestTimeoutMs()) timedOut.add(request.destination());
            }
            for (String destination : timedOut) {
                Connection connection = connections.remove(destination);
                if (connection != null) connection.closed = true;
                trace(name + " request to " + destination + " timed out");
                consumer.client().disconnect(destination);
            }
            if (!consumer.client().hasPendingMetadataUpdates()) {
                // Any metadata request returns the full metadata.
                consumer.client().prepareMetadataUpdate(metadataResponse);
            }
            consumer.networkThreadRunOnce();
            pumpRequests();
            observe();
            scheduleNetworkRun(20 + rng.nextInt(80));
        }

        void pumpRequests() {
            // Requests that the client disconnected without a response: the connection is gone.
            Set<ClientRequest> live = Collections.newSetFromMap(new IdentityHashMap<>());
            live.addAll(consumer.client().requests());
            for (InFlight f : inFlight) {
                if (!f.responded && !f.abandoned && !live.contains(f.request)) {
                    f.abandoned = true;
                    Connection connection = f.connection;
                    if (connections.get(connection.destination) == connection) {
                        connections.remove(connection.destination);
                        trace(name + " connection to " + connection.destination + " closed by the client");
                    }
                }
            }
            inFlight.removeIf(f -> f.responded || f.abandoned);

            for (ClientRequest request : consumer.newRequests()) {
                if (request.apiKey() != ApiKeys.FETCH && request.apiKey() != ApiKeys.CONSUMER_GROUP_HEARTBEAT &&
                    request.apiKey() != ApiKeys.OFFSET_COMMIT) {
                    trace(name + " sends " + request.apiKey() + " to " + request.destination());
                }
                Connection connection = connections.computeIfAbsent(request.destination(), dest -> new Connection(this, dest));
                InFlight f = new InFlight(request, connection);
                inFlight.add(f);
                connection.send(f);
            }
        }

        void disconnect(String destination) {
            Connection connection = connections.remove(destination);
            if (connection != null) connection.closed = true;
            consumer.client().disconnect(destination);
            scheduleNetworkRun(0);
        }

        // Application thread.

        void scheduleAppStep(long delayMs) {
            long at = time.milliseconds() + delayMs;
            if (at >= nextAppStepMs) return;
            nextAppStepMs = at;
            schedule(delayMs, name + " application thread", () -> {
                if (time.milliseconds() < nextAppStepMs) return;
                nextAppStepMs = Long.MAX_VALUE;
                appStep();
            });
        }

        /** The application thread does not call the consumer for the duration. */
        void pause(long durationMs) {
            pausedUntilMs = time.milliseconds() + durationMs;
            trace(name + " pauses for " + durationMs + " ms");
        }

        void appStep() {
            if (consumer.isClosed()) return;
            if (time.milliseconds() < pausedUntilMs && !consumer.inCall()) {
                scheduleAppStep(pausedUntilMs - time.milliseconds());
                return;
            }
            if (consumer.inCall()) {
                continueCall();
                return;
            }
            Runnable action = actions.poll();
            if (action != null) {
                action.run();
            } else if (commitDue && !commitStyle.equals("none")) {
                commitDue = false;
                commit();
            } else if (!subscription.isEmpty()) {
                consumer.poll(scripted ? 100 : rng.nextInt(500));
            } else {
                // Not subscribed: the application waits.
                scheduleAppStep(100);
                return;
            }
            scheduleAppStep(0);
        }

        /**
         * Runs one step of the call in progress. When the call returns, the application processes
         * the records before its next step.
         */
        private void continueCall() {
            String call = consumer.currentCall();
            boolean returned = consumer.applicationThreadStep();
            observe();
            if (!returned) {
                scheduleAppStep(consumer.waiting() ? 1 + rng.nextInt(20) : rng.nextInt(params.retryBackoffMs + 1));
                return;
            }
            if (consumer.isClosed()) {
                trace(name + " closed");
                return;
            }
            if (!call.startsWith("poll")) trace(name + " " + call + " returned");
            long processingMs;
            if (scripted) {
                processingMs = 0;
            } else if (longProcessing) {
                longProcessing = false;
                processingMs = params.maxPollIntervalMs + 1000 + rng.nextInt(params.maxPollIntervalMs);
                trace(name + " processes for " + processingMs + " ms");
            } else {
                processingMs = rng.nextInt(chaos ? 300 : 100);
            }
            scheduleAppStep(processingMs);
        }

        /** The application commits after processing the records of a poll. */
        void commit() {
            switch (commitStyle) {
                case "sync-consumed":
                    consumer.commitSync(Optional.empty(), 30_000);
                    break;
                case "async-consumed":
                    consumer.commitAsync(Optional.empty());
                    break;
                case "sync-tracked":
                    consumer.commitSync(Optional.of(trackedOffsets()), 30_000);
                    break;
                case "async-tracked":
                    consumer.commitAsync(Optional.of(trackedOffsets()));
                    break;
                default:
                    throw new IllegalStateException(commitStyle);
            }
        }

        private Map<TopicPartition, OffsetAndMetadata> trackedOffsets() {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            processed.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
            // An application that processes records asynchronously commits the last records of a
            // revoked partition once, after the revocation.
            releasedButTracked.forEach(processed::remove);
            releasedButTracked.clear();
            return offsets;
        }

        /** The application commits the offsets with commitSync once its current call returns. */
        void commitSyncLater(Map<TopicPartition, Long> offsets) {
            Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
            offsets.forEach((tp, offset) -> committed.put(tp, new OffsetAndMetadata(offset)));
            actions.add(() -> {
                trace(name + " commitSync(" + new TreeMap<>(offsets.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue))) + ")");
                consumer.commitSync(Optional.of(committed), 30_000);
            });
        }

        void subscribe(Set<String> topics) {
            if (consumer.isClosed()) return;
            subscription = topics;
            trace(name + " subscribes to " + topics);
            consumer.subscribe(topics);
        }

        void unsubscribe() {
            if (consumer.isClosed()) return;
            subscription = Set.of();
            trace(name + " unsubscribes");
            consumer.unsubscribe();
        }

        void close() {
            if (consumer.isClosed()) return;
            CloseOptions.GroupMembershipOperation operation = CloseOptions.GroupMembershipOperation.DEFAULT;
            if (instanceId != null && rng.nextBoolean()) operation = CloseOptions.GroupMembershipOperation.LEAVE_GROUP;
            if (instanceId == null && rng.nextInt(4) == 0) operation = CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP;
            close(operation);
        }

        /** The application calls close with the operation once its current call returns. */
        void closeLater(CloseOptions.GroupMembershipOperation operation) {
            actions.add(() -> close(operation));
        }

        void unsubscribeLater() {
            actions.add(this::unsubscribe);
        }

        void subscribeLater(Set<String> topics) {
            actions.add(() -> subscribe(topics));
        }

        void close(CloseOptions.GroupMembershipOperation operation) {
            if (consumer.isClosed()) return;
            stopping = true;
            trace(name + " closes with " + operation);
            closeOperation = operation;
            consumer.close(operation, 30_000);
        }

        void observe() {
            int epoch = consumer.memberEpoch();
            if (epoch > 0) {
                if (lastPositiveEpoch > 0 && epoch < lastPositiveEpoch) {
                    violation("epoch-regression", name + " went from epoch " + lastPositiveEpoch + " to " + epoch);
                }
                lastPositiveEpoch = epoch;
            } else {
                lastPositiveEpoch = 0;
                target = Map.of();
            }
            for (String error : consumer.drainErrors()) {
                count("client-error:" + error.split(":")[1].trim());
                if (instanceId != null && error.contains("UnreleasedInstanceId")) {
                    trace(name + " failed: " + error);
                } else if (error.startsWith("poll: UnknownMemberIdException") || error.startsWith("poll: StaleMemberEpochException")) {
                    violation("member-error-thrown", name + " " + error);
                } else {
                    violation("client-error", name + " " + error);
                }
            }
        }

        // Hooks.

        @Override
        public void wakeupNetworkThread(SimConsumer c) {
            scheduleNetworkRun(rng.nextInt(2));
        }

        @Override
        public void onCallback(SimConsumer c, String method, Set<TopicPartition> partitions) {
            trace(name + " " + method + " " + new TreeSet<>(partitions.stream().map(TopicPartition::toString).toList()));
            callbacks.add(method + " " + new TreeSet<>(partitions.stream().map(TopicPartition::toString).toList()));
            if (!method.equals("onPartitionsAssigned")) {
                if (commitsAfterRevocation) {
                    releasedButTracked.addAll(partitions);
                } else {
                    partitions.forEach(processed::remove);
                }
            }
            for (TopicPartition tp : partitions) {
                if (method.equals("onPartitionsAssigned")) {
                    if (!callbackOwned.add(tp)) {
                        violation("callback-assigned-twice", name + " " + tp + " passed to onPartitionsAssigned while owned");
                    }
                } else if (!callbackOwned.remove(tp)) {
                    violation("callback-released-unowned", name + " " + tp + " passed to " + method + " while not owned");
                }
            }
        }

        @Override
        public long callbackDurationMs(SimConsumer c, String method) {
            if (scripted) return scriptedCallbackDurationMs;
            if (chaos && rng.nextDouble() < params.slowCallback) {
                return rng.nextInt(params.maxPollIntervalMs);
            }
            return 0;
        }

        @Override
        public int recordsPerPoll(SimConsumer c, TopicPartition partition) {
            return scripted ? 1 : rng.nextInt(5);
        }

        @Override
        public void onRecords(SimConsumer c, Map<TopicPartition, Long> positions) {
            processed.putAll(positions);
            commitDue = true;
        }

        @Override
        public void onCommitCompleted(SimConsumer c, String call, Map<TopicPartition, OffsetAndMetadata> offsets, Throwable error) {
            String result = error == null ? "NONE" : error.getClass().getSimpleName();
            count("app-" + call + ":" + result);
            if (error != null) trace(name + " " + call + " failed: " + result);
        }

        @Override
        public void onStaleAssignmentApplied(SimConsumer c, Set<TopicPartition> assigned, Set<TopicPartition> added, String reason) {
            if (added.isEmpty() && c.assignedPartitions().containsAll(assigned)) return;
            violation("stale-assignment-applied", name + " applies " + new TreeSet<>(assigned.stream().map(TopicPartition::toString).toList()) +
                " (added " + new TreeSet<>(added.stream().map(TopicPartition::toString).toList()) + ") of an aborted reconciliation: " + reason);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Network.
    // ---------------------------------------------------------------------------------------------

    private static final class InFlight {
        final ClientRequest request;
        final Connection connection;
        boolean responded;
        boolean abandoned;
        boolean loseResponse;

        InFlight(ClientRequest request, Connection connection) {
            this.request = request;
            this.connection = connection;
        }
    }

    /**
     * A connection from a client to a broker. The requests arrive in order, and the broker reads
     * the next request only once it sent the response to the previous one.
     */
    private final class Connection {
        final Driver driver;
        final String destination;
        final Deque<InFlight> arrived = new ArrayDeque<>();
        boolean busy;
        boolean closed;
        long lastArrivalMs;
        long lastResponseMs;

        Connection(Driver driver, String destination) {
            this.driver = driver;
            this.destination = destination;
        }

        void send(InFlight f) {
            if (f.request.apiKey() == ApiKeys.CONSUMER_GROUP_HEARTBEAT) driver.heartbeatsSent++;
            if (time.milliseconds() >= driver.partitionedFromMs && time.milliseconds() < driver.partitionedToMs) {
                trace(driver.name + " " + f.request.apiKey() + " is lost in the network partition");
                return;
            }
            long delayMs = networkDelayMs();
            for (Iterator<RequestRule> it = requestRules.iterator(); it.hasNext(); ) {
                RequestRule rule = it.next();
                if (rule.consumer().equals(driver.name) && rule.apiKey() == f.request.apiKey() &&
                    rule.matches().test(f.request.requestBuilder().build(version(f.request)))) {
                    it.remove();
                    if (rule.loseResponse()) {
                        trace(driver.name + " " + f.request.apiKey() + " will lose its response");
                        f.loseResponse = true;
                        break;
                    }
                    if (rule.delayMs() < 0) {
                        trace(driver.name + " " + f.request.apiKey() + " is lost");
                        return;
                    }
                    trace(driver.name + " " + f.request.apiKey() + " is delayed by " + rule.delayMs() + " ms");
                    delayMs = rule.delayMs();
                    // A delayed request does not hold back the next ones of the connection.
                    schedule(delayMs, driver.name + " request arrives", () -> {
                        arrived.add(f);
                        maybeProcess();
                    });
                    return;
                }
            }
            long arrival = Math.max(lastArrivalMs, time.milliseconds() + delayMs);
            lastArrivalMs = arrival;
            schedule(arrival - time.milliseconds(), driver.name + " request arrives", () -> {
                if (closed && rng.nextBoolean()) {
                    trace(driver.name + " " + f.request.apiKey() + " lost with its connection");
                    return;
                }
                arrived.add(f);
                maybeProcess();
            });
        }

        void maybeProcess() {
            if (busy || arrived.isEmpty()) return;
            busy = true;
            InFlight f = arrived.poll();
            currentRequestAbandoned = f.abandoned || closed;
            handle(driver, f, response -> {
                busy = false;
                long delivery = Math.max(lastResponseMs, time.milliseconds() + networkDelayMs());
                lastResponseMs = delivery;
                schedule(delivery - time.milliseconds(), driver.name + " response arrives", () -> deliver(f, response));
                maybeProcess();
            });
            currentRequestAbandoned = false;
        }

        void deliver(InFlight f, AbstractResponse response) {
            SimConsumer consumer = driver.consumer;
            if (consumer.isClosed() || closed || f.abandoned) return;
            if (f.loseResponse) {
                trace(driver.name + " " + f.request.apiKey() + " response is lost");
                return;
            }
            boolean inFlight = false;
            for (ClientRequest request : consumer.client().requests()) {
                if (request == f.request) {
                    inFlight = true;
                    break;
                }
            }
            if (!inFlight) return;
            f.responded = true;
            if (response instanceof ConsumerGroupHeartbeatResponse heartbeat && heartbeat.data().errorCode() == Errors.NONE.code() &&
                heartbeat.data().assignment() != null) {
                Map<Uuid, Set<Integer>> assignment = new HashMap<>();
                heartbeat.data().assignment().topicPartitions().forEach(tp -> assignment.put(tp.topicId(), Set.copyOf(tp.partitions())));
                driver.target = assignment;
            }
            consumer.client().respondToRequest(f.request, response);
            driver.scheduleNetworkRun(0);
        }
    }

    private static short version(ClientRequest request) {
        AbstractRequest.Builder<?> builder = request.requestBuilder();
        return NodeApiVersions.create().latestUsableVersion(request.apiKey(), builder.oldestAllowedVersion(),
            builder.latestAllowedVersion());
    }

    private static int brokerId(String destination) {
        return Integer.parseInt(destination.startsWith("+") ? destination.substring(1) : destination);
    }

    private void handle(Driver driver, InFlight f, Consumer<AbstractResponse> respond) {
        ClientRequest request = f.request;
        short version = version(request);
        AbstractRequest body = request.requestBuilder().build(version);
        int nodeId = brokerId(request.destination());
        String clientId = driver.name;
        currentRequest = driver.name + " " + request.apiKey() +
            (body instanceof ConsumerGroupHeartbeatRequest hb ? " epoch " + hb.data().memberEpoch() : "");
        switch (request.apiKey()) {
            case FIND_COORDINATOR: {
                FindCoordinatorRequest find = (FindCoordinatorRequest) body;
                String key = find.data().coordinatorKeys().isEmpty() ? find.data().key() : find.data().coordinatorKeys().get(0);
                if (coordinator.status() == HeartbeatSimCoordinator.Status.NONE) {
                    respond.accept(FindCoordinatorResponse.prepareResponse(Errors.COORDINATOR_NOT_AVAILABLE, key, Node.noNode()));
                } else {
                    int node = coordinator.coordinatorNodeId();
                    respond.accept(FindCoordinatorResponse.prepareResponse(Errors.NONE, key, new Node(node, "localhost", 9092 + node)));
                }
                break;
            }
            case CONSUMER_GROUP_HEARTBEAT: {
                ConsumerGroupHeartbeatRequestData data = ((ConsumerGroupHeartbeatRequest) body).data().duplicate();
                coordinator.consumerGroupHeartbeat(nodeId, version, clientId, data, response -> {
                    traceHeartbeat(driver, data, response);
                    respond.accept(new ConsumerGroupHeartbeatResponse(response));
                });
                break;
            }
            case OFFSET_COMMIT: {
                OffsetCommitRequestData data = ((OffsetCommitRequest) body).data().duplicate();
                coordinator.commitOffsets(nodeId, version, clientId, data, (req, response) -> {
                    traceCommit(driver, req, response);
                    respond.accept(new OffsetCommitResponse(response));
                });
                break;
            }
            case OFFSET_FETCH: {
                OffsetFetchRequest fetch = (OffsetFetchRequest) body;
                OffsetFetchRequestData.OffsetFetchRequestGroup group = fetch.data().groups().get(0).duplicate();
                coordinator.fetchOffsets(nodeId, version, group, fetch.data().requireStable(), response -> {
                    count("offset-fetch:" + Errors.forCode(response.errorCode()));
                    respond.accept(new OffsetFetchResponse(new OffsetFetchResponseData().setGroups(List.of(response)), version));
                });
                break;
            }
            case FETCH: {
                FetchRequestData data = ((FetchRequest) body).data();
                FetchResponseData response = new FetchResponseData().setErrorCode(Errors.NONE.code()).setSessionId(0);
                for (FetchRequestData.FetchTopic topic : data.topics()) {
                    FetchResponseData.FetchableTopicResponse topicResponse = new FetchResponseData.FetchableTopicResponse()
                        .setTopicId(topic.topicId())
                        .setTopic(topic.topic());
                    for (FetchRequestData.FetchPartition partition : topic.partitions()) {
                        topicResponse.partitions().add(new FetchResponseData.PartitionData()
                            .setPartitionIndex(partition.partition())
                            .setHighWatermark(1_000_000)
                            .setLastStableOffset(1_000_000)
                            .setLogStartOffset(0)
                            .setRecords(MemoryRecords.EMPTY));
                    }
                    response.responses().add(topicResponse);
                }
                long waitMs = Math.min(data.maxWaitMs(), 100 + rng.nextInt(400));
                schedule(waitMs, "fetch wait", () -> respond.accept(FetchResponse.of(response)));
                break;
            }
            case LIST_OFFSETS: {
                ListOffsetsRequestData data = ((ListOffsetsRequest) body).data();
                ListOffsetsResponseData response = new ListOffsetsResponseData();
                for (ListOffsetsRequestData.ListOffsetsTopic topic : data.topics()) {
                    ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse = new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic.name());
                    for (ListOffsetsRequestData.ListOffsetsPartition partition : topic.partitions()) {
                        topicResponse.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                            .setPartitionIndex(partition.partitionIndex())
                            .setErrorCode(Errors.NONE.code())
                            .setOffset(0)
                            .setTimestamp(-1)
                            .setLeaderEpoch(-1));
                    }
                    response.topics().add(topicResponse);
                }
                respond.accept(new ListOffsetsResponse(response));
                break;
            }
            default:
                violation("unexpected-request", driver.name + " sent " + request.apiKey());
        }
    }

    private void traceHeartbeat(Driver driver, ConsumerGroupHeartbeatRequestData request, ConsumerGroupHeartbeatResponseData response) {
        Errors error = Errors.forCode(response.errorCode());
        if (request.memberEpoch() < 0 && driver.leaveAppendFailed) {
            driver.leaveAppendFailed = false;
            violation("failed-leave-ghost", driver.name + " left with epoch " + request.memberEpoch() +
                " but the write failed: the member stays without session timeout");
        }
        if (request.memberEpoch() == -1 && driver.instanceId == null && driver.closeOperation == CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP) {
            violation("remain-in-group-left", driver.name + " closed with REMAIN_IN_GROUP but sent a heartbeat to leave the group");
        }
        count("heartbeat:" + error);
        StringBuilder line = new StringBuilder(driver.name).append(" HB epoch ").append(request.memberEpoch());
        if (request.topicPartitions() != null) line.append(" owned ").append(describe(request.topicPartitions()));
        if (request.subscribedTopicNames() != null) line.append(" topics ").append(request.subscribedTopicNames());
        line.append(" -> ").append(error);
        if (error == Errors.NONE) {
            line.append(" epoch ").append(response.memberEpoch());
            if (response.assignment() != null) {
                line.append(" assignment ").append(describeAssignment(response.assignment()));
            }
        }
        trace(line.toString());
    }

    private void traceCommit(Driver driver, OffsetCommitRequestData request, OffsetCommitResponseData response) {
        StringBuilder line = new StringBuilder(driver.name).append(" commit epoch ").append(request.generationIdOrMemberEpoch());
        for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : response.topics()) {
            for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                Errors error = Errors.forCode(partition.errorCode());
                count("commit:" + error);
                line.append(" ").append(topic.name()).append("-").append(partition.partitionIndex()).append("=").append(error);
            }
        }
        trace(line.toString());
    }

    private String topicName(Uuid topicId) {
        for (Map.Entry<String, Uuid> entry : topicIds.entrySet()) {
            if (entry.getValue().equals(topicId)) return entry.getKey();
        }
        return topicId.toString();
    }

    private String describe(List<ConsumerGroupHeartbeatRequestData.TopicPartitions> partitions) {
        SortedSet<String> result = new TreeSet<>();
        for (ConsumerGroupHeartbeatRequestData.TopicPartitions tp : partitions) {
            for (int p : tp.partitions()) result.add(topicName(tp.topicId()) + "-" + p);
        }
        return result.toString();
    }

    private String describe(Map<Uuid, Map<Integer, Integer>> partitions) {
        SortedSet<String> result = new TreeSet<>();
        partitions.forEach((topicId, epochs) -> epochs.keySet().forEach(p -> result.add(topicName(topicId) + "-" + p)));
        return result.toString();
    }

    private String describeAssignment(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
        SortedSet<String> result = new TreeSet<>();
        for (var tp : assignment.topicPartitions()) {
            for (int p : tp.partitions()) result.add(topicName(tp.topicId()) + "-" + p);
        }
        return result.toString();
    }

    // ---------------------------------------------------------------------------------------------
    // Invariants.
    // ---------------------------------------------------------------------------------------------

    private static boolean contains(Map<Uuid, Map<Integer, Integer>> partitions, Uuid topicId, int partition) {
        Map<Integer, Integer> p = partitions.get(topicId);
        return p != null && p.containsKey(partition);
    }

    /**
     * @return True if the partition is assigned to the member or pending revocation.
     */
    private static boolean owns(ConsumerGroupMember member, Uuid topicId, int partition) {
        return member != null && (contains(member.assignedPartitions(), topicId, partition) ||
            contains(member.partitionsPendingRevocation(), topicId, partition));
    }

    /**
     * A consumer is a member in good standing if its next heartbeat would be accepted: the
     * coordinator knows its member id and its epoch is the current one, or the previous one with
     * owned partitions that are all assigned (a heartbeat after an error carries them).
     */
    private boolean inGoodStanding(SimConsumer consumer, ConsumerGroup group) {
        if (consumer.isClosed()) return false;
        int epoch = consumer.memberEpoch();
        if (epoch <= 0) return false;
        ConsumerGroupMember member = group.members().get(consumer.memberId());
        if (member == null) return false;
        if (epoch == member.memberEpoch()) return true;
        if (epoch < member.memberEpoch() && epoch == member.previousMemberEpoch()) {
            for (Map.Entry<Uuid, SortedSet<Integer>> entry : consumer.currentAssignment().entrySet()) {
                for (int partition : entry.getValue()) {
                    if (!contains(member.assignedPartitions(), entry.getKey(), partition)) return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Checks the invariants after a step of the simulation.
     */
    private void check() {
        Optional<ConsumerGroup> groupOpt = coordinator.consumerGroup(GROUP_ID);
        if (groupOpt.isEmpty()) return;
        ConsumerGroup group = groupOpt.get();
        checkPartitionEpochs(group);
        checkHeldPartitions(group);
        checkOwnership(group, "double-assignment", d -> d.consumer.assignedPartitions());
        checkOwnership(group, "double-callback-ownership", d -> d.consumer.ownedByCallbacks());
    }

    /**
     * Checks that the partition epochs of the coordinator match the assignments of the members.
     */
    private void checkPartitionEpochs(ConsumerGroup group) {
        Map<TopicIdPartition, String> serverOwners = new HashMap<>();
        for (ConsumerGroupMember member : group.members().values()) {
            for (Map<Uuid, Map<Integer, Integer>> partitions : List.of(member.assignedPartitions(), member.partitionsPendingRevocation())) {
                partitions.forEach((topicId, epochs) -> epochs.keySet().forEach(partition -> {
                    TopicIdPartition tp = new TopicIdPartition(topicId, partition, topicName(topicId));
                    String previous = serverOwners.put(tp, member.memberId());
                    if (previous != null && !previous.equals(member.memberId())) {
                        violation("coordinator-double-assignment", tp + " assigned to " + previous + " and " + member.memberId());
                    }
                    int partitionEpoch = group.currentPartitionEpoch(topicId, partition);
                    if (partitionEpoch != member.memberEpoch()) {
                        violation("partition-epoch", tp + " of member " + member.memberId() + " at epoch " +
                            member.memberEpoch() + " has partition epoch " + partitionEpoch);
                    }
                }));
            }
        }
        params.topics.forEach((topic, count) -> {
            Uuid topicId = topicIds.get(topic);
            for (int p = 0; p < count; p++) {
                int partitionEpoch = group.currentPartitionEpoch(topicId, p);
                if (partitionEpoch != -1 && !serverOwners.containsKey(new TopicIdPartition(topicId, p, topic))) {
                    violation("partition-epoch", topic + "-" + p + " has partition epoch " + partitionEpoch + " but no owner");
                }
            }
        });

    }

    /**
     * Checks that a consumer in good standing only holds partitions that the coordinator
     * assigns to it or that it is revoking.
     */
    private void checkHeldPartitions(ConsumerGroup group) {
        for (Driver d : drivers) {
            if (!inGoodStanding(d.consumer, group)) continue;
            ConsumerGroupMember member = group.members().get(d.consumer.memberId());
            Set<TopicPartition> held = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
            held.addAll(d.consumer.assignedPartitions());
            held.addAll(d.consumer.ownedByCallbacks());
            for (TopicPartition tp : held) {
                if (!owns(member, topicIds.get(tp.topic()), tp.partition())) {
                    if (d.releasedWhileTargeted.contains(tp)) {
                        violation("released-while-held", tp + " released from " + d.name + " while in its target assignment, " +
                            "then assigned by the client: " + d.consumer);
                    }
                    violation("held-not-assigned", tp + " held by " + d.consumer + " but the coordinator has epoch " +
                        member.memberEpoch() + "/" + member.previousMemberEpoch() + " " + member.state() + " assigned " +
                        describe(member.assignedPartitions()) + " pending revocation " + describe(member.partitionsPendingRevocation()));
                }
            }
        }
    }

    /**
     * Checks that no partition is held by two consumers in good standing.
     */
    private void checkOwnership(ConsumerGroup group, String kind, Function<Driver, Set<TopicPartition>> held) {
        Map<TopicPartition, List<String>> owners = new TreeMap<>(Comparator.comparing(TopicPartition::toString));
        for (Driver d : drivers) {
            if (!inGoodStanding(d.consumer, group)) continue;
            for (TopicPartition tp : held.apply(d)) {
                owners.computeIfAbsent(tp, __ -> new ArrayList<>()).add(d.name);
            }
        }
        owners.forEach((tp, names) -> {
            if (names.size() > 1) {
                StringBuilder detail = new StringBuilder(tp + " owned by " + names + ":");
                for (Driver d : drivers) {
                    if (names.contains(d.name)) {
                        ConsumerGroupMember member = group.members().get(d.consumer.memberId());
                        detail.append(" ").append(d.consumer).append(" server=").append(member == null ? "none" :
                            "epoch " + member.memberEpoch() + "/" + member.previousMemberEpoch() + " " + member.state());
                    }
                }
                violation(kind, detail.toString());
            }
        });
    }

    /**
     * After the chaos phase and the settle time, every running consumer is stable with the
     * target assignment and every partition of a subscribed topic is assigned.
     */
    private void checkConvergence() {
        Optional<ConsumerGroup> groupOpt = coordinator.consumerGroup(GROUP_ID);
        List<Driver> running = drivers.stream()
            .filter(d -> !d.consumer.isClosed() && !d.subscription.isEmpty() && d.consumer.state() != MemberState.FATAL)
            .toList();
        if (groupOpt.isEmpty()) {
            if (!running.isEmpty()) violation("convergence", "no consumer group at the end");
            return;
        }
        ConsumerGroup group = groupOpt.get();
        Set<String> expectedMembers = running.stream().map(d -> d.consumer.memberId()).collect(Collectors.toSet());
        Set<String> actualMembers = new TreeSet<>(group.members().keySet());
        if (!actualMembers.equals(new TreeSet<>(expectedMembers))) {
            violation("convergence", "members " + actualMembers + " but running consumers " + new TreeSet<>(expectedMembers));
        }
        if (group.groupEpoch() != group.assignmentEpoch()) {
            violation("convergence", "group epoch " + group.groupEpoch() + " but assignment epoch " + group.assignmentEpoch());
        }
        Set<TopicPartition> assigned = new HashSet<>();
        for (Driver d : running) {
            ConsumerGroupMember member = group.members().get(d.consumer.memberId());
            if (member != null) assigned.addAll(checkConverged(d, member, group));
        }
        Set<String> subscribed = new TreeSet<>();
        running.forEach(d -> subscribed.addAll(d.subscription));
        for (String topic : subscribed) {
            for (int p = 0; p < params.topics.get(topic); p++) {
                if (!assigned.contains(new TopicPartition(topic, p))) {
                    violation("convergence", topic + "-" + p + " is not assigned");
                }
            }
        }
    }

    /**
     * Checks that the consumer and its member are stable at the group epoch, and that the
     * consumer and its callbacks hold the target assignment of the member.
     *
     * @return The target assignment of the member.
     */
    private Set<TopicPartition> checkConverged(Driver d, ConsumerGroupMember member, ConsumerGroup group) {
        SimConsumer consumer = d.consumer;
        Assignment target = group.targetAssignment(member.memberId());
        Set<TopicPartition> targetPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
        target.partitions().forEach((topicId, partitions) ->
            partitions.forEach(p -> targetPartitions.add(new TopicPartition(topicName(topicId), p))));
        Set<TopicPartition> clientPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
        clientPartitions.addAll(consumer.assignedPartitions());
        Set<TopicPartition> callbackPartitions = new TreeSet<>(Comparator.comparing(TopicPartition::toString));
        callbackPartitions.addAll(consumer.ownedByCallbacks());
        boolean stable = consumer.state() == MemberState.STABLE &&
            member.state() == org.apache.kafka.coordinator.group.modern.MemberState.STABLE &&
            consumer.memberEpoch() == member.memberEpoch() && member.memberEpoch() == group.groupEpoch();
        if (!stable || !clientPartitions.equals(targetPartitions) || !callbackPartitions.equals(targetPartitions)) {
            violation("convergence", d.name + " client " + consumer.state() + " epoch " + consumer.memberEpoch() + " " +
                clientPartitions + " callbacks " + callbackPartitions + "; server " + member.state() + " epoch " +
                member.memberEpoch() + " target " + targetPartitions + " group epoch " + group.groupEpoch());
        }
        return targetPartitions;
    }
}
