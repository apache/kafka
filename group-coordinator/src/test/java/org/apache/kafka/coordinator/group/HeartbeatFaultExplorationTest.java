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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Explores every placement of one fault, and of every pair of faults, in small scripted
 * scenarios of the heartbeat simulation. The scenarios and the timings are fixed, so a run is
 * determined by its faults. After the faults, the simulation runs for several session timeouts
 * and checks that the group converged, besides the invariants checked after every step.
 * <p>
 * Every scenario runs with auto-commit and with commits by the application.
 * <p>
 * A run fails on a violation that is not in {@link HeartbeatSimulation#KNOWN_ISSUES}. The
 * following system properties change the runs; Gradle sets them from the project properties of
 * the same name.
 * <ul>
 *   <li>{@code heartbeat.sim.depth}: 1 for every single fault (the default), 2 for every pair of
 *       faults too.</li>
 *   <li>{@code heartbeat.sim.scenario}: runs one scenario, e.g. {@code join-join-leave} or
 *       {@code join-join-leave/manual}.</li>
 *   <li>{@code heartbeat.sim.faults}: runs one combination of faults, e.g.
 *       {@code "lose-request C1 3,append-failure 7"}.</li>
 *   <li>{@code heartbeat.sim.threads}: the number of runs in parallel.</li>
 *   <li>{@code heartbeat.sim.verbose}: prints the trace of the runs of one combination.</li>
 * </ul>
 */
public class HeartbeatFaultExplorationTest {

    /** Faults are placed in the first part of the scenario, while the members join and leave. */
    private static final long WINDOW_MS = 8_000;
    private static final long END_MS = 40_000;

    /**
     * A fault placed by its kind, the consumer it applies to, if any, and its position: the
     * position of the heartbeat request of the consumer, of the coordinator write, or the second
     * at which the application pauses.
     */
    record Fault(String kind, String consumer, int position) {
        @Override
        public String toString() {
            return kind + (consumer == null ? "" : " " + consumer) + " " + position;
        }

        static Fault parse(String text) {
            String[] parts = text.trim().split(" ");
            return parts.length == 3 ? new Fault(parts[0], parts[1], Integer.parseInt(parts[2])) :
                new Fault(parts[0], null, Integer.parseInt(parts[1]));
        }

        void apply(HeartbeatSimulation sim) {
            switch (kind) {
                case "lose-request":
                    sim.delayRequest(consumer, ApiKeys.CONSUMER_GROUP_HEARTBEAT, nth(position), -1);
                    break;
                case "delay-request":
                    // Past the request timeout (5 seconds): the client sends again on a new connection.
                    sim.delayRequest(consumer, ApiKeys.CONSUMER_GROUP_HEARTBEAT, nth(position), 6000);
                    break;
                case "lose-response":
                    sim.loseResponse(consumer, ApiKeys.CONSUMER_GROUP_HEARTBEAT, nth(position));
                    break;
                case "append-failure":
                    AtomicInteger writes = new AtomicInteger();
                    sim.failAppend(request -> writes.incrementAndGet() == position);
                    break;
                case "failover-keep":
                    sim.afterWrite(position, () -> sim.failover(1, 500, true));
                    break;
                case "failover-drop":
                    sim.afterWrite(position, () -> sim.failover(1, 500, false));
                    break;
                case "pause":
                    // Longer than max.poll.interval.ms (10 seconds).
                    sim.at(position * 1000L, "pause " + consumer, () -> sim.driver(consumer).ifPresent(d -> d.pause(11_000)));
                    break;
                default:
                    throw new IllegalArgumentException(kind);
            }
        }

        private static Predicate<AbstractRequest> nth(int n) {
            AtomicInteger count = new AtomicInteger();
            return request -> count.incrementAndGet() == n;
        }
    }

    /**
     * A scenario: the consumers and what their applications do over time.
     */
    record Scenario(String name, List<String> consumers, Consumer<HeartbeatSimulation> script) { }

    static List<Scenario> scenarios() {
        return List.of(
            new Scenario("join-join-leave", List.of("C1", "C2"), sim -> {
                sim.start("C1", null, Set.of("t0"));
                sim.at(1500, "start C2", () -> sim.start("C2", null, Set.of("t0")));
                sim.at(5000, "close C2", () -> sim.driver("C2").ifPresent(d ->
                    d.closeLater(CloseOptions.GroupMembershipOperation.DEFAULT)));
            }),
            new Scenario("unsubscribe-resubscribe", List.of("C1", "C2"), sim -> {
                sim.start("C1", null, Set.of("t0"));
                sim.at(1500, "start C2", () -> sim.start("C2", null, Set.of("t0")));
                sim.at(4000, "C1 unsubscribes", () -> sim.driver("C1").ifPresent(HeartbeatSimulation.Driver::unsubscribeLater));
                sim.at(6000, "C1 subscribes", () -> sim.driver("C1").ifPresent(d -> d.subscribeLater(Set.of("t0"))));
            }),
            new Scenario("static-restart", List.of("C1", "C2"), sim -> {
                sim.start("C1", "instance-1", Set.of("t0"));
                sim.at(1500, "start C2", () -> sim.start("C2", null, Set.of("t0")));
                sim.at(4000, "close C1", () -> sim.driver("C1").ifPresent(d ->
                    d.closeLater(CloseOptions.GroupMembershipOperation.DEFAULT)));
                sim.at(5000, "restart C1", () -> sim.start("C3", "instance-1", Set.of("t0")));
            })
        ).stream().flatMap(s -> COMMIT_MODES.stream().map(mode -> new Scenario(s.name() + "/" + mode, s.consumers(), s.script()))).toList();
    }

    /** The commit modes the scenarios run with: auto-commit, and commits by the application. */
    static final List<String> COMMIT_MODES = List.of("auto", "manual");

    static String commitMode(Scenario scenario) {
        return scenario.name().substring(scenario.name().lastIndexOf('/') + 1);
    }

    static HeartbeatSimulation simulation(Scenario scenario) {
        return HeartbeatSimulation.scripted(HeartbeatSimulation.Params.scripted(Map.of("t0", 2), commitMode(scenario)));
    }

    static HeartbeatSimulation run(Scenario scenario, List<Fault> faults) {
        HeartbeatSimulation sim = simulation(scenario);
        faults.forEach(fault -> fault.apply(sim));
        scenario.script().accept(sim);
        sim.runUntil(END_MS);
        sim.checkConvergenceNow();
        return sim;
    }

    /**
     * The fault placements of a scenario, from a run without faults: every heartbeat of the
     * consumers and every coordinator write in the window, and every second of the window for
     * the pauses.
     */
    static List<Fault> placements(Scenario scenario) {
        HeartbeatSimulation sim = simulation(scenario);
        scenario.script().accept(sim);
        sim.runUntil(WINDOW_MS);
        List<Fault> faults = new ArrayList<>();
        Set<String> names = new TreeSet<>(scenario.consumers());
        for (String name : names) {
            int heartbeats = sim.driver(name).map(d -> d.heartbeatsSent).orElse(0);
            for (int k = 1; k <= heartbeats; k++) {
                faults.add(new Fault("lose-request", name, k));
                faults.add(new Fault("delay-request", name, k));
                faults.add(new Fault("lose-response", name, k));
            }
            for (int second = 0; second < WINDOW_MS / 1000; second++) {
                faults.add(new Fault("pause", name, second));
            }
        }
        for (int k = 1; k <= sim.writeCount(); k++) {
            faults.add(new Fault("append-failure", null, k));
            faults.add(new Fault("failover-keep", null, k));
            faults.add(new Fault("failover-drop", null, k));
        }
        return faults;
    }

    /**
     * The combinations of faults to run for the scenario: none, every placement alone and, at
     * depth 2, every pair of placements.
     */
    static List<List<Fault>> combinations(Scenario scenario, int depth) {
        List<Fault> placements = placements(scenario);
        List<List<Fault>> combinations = new ArrayList<>();
        combinations.add(List.of());
        for (int i = 0; i < placements.size(); i++) {
            combinations.add(List.of(placements.get(i)));
            if (depth >= 2) {
                for (int j = i + 1; j < placements.size(); j++) {
                    combinations.add(List.of(placements.get(i), placements.get(j)));
                }
            }
        }
        return combinations;
    }

    @BeforeAll
    public static void setUpLogging() {
        HeartbeatSimulation.setLogLevel(Level.OFF);
    }

    @AfterAll
    public static void resetLogging() {
        HeartbeatSimulation.resetLogLevel();
    }

    @Test
    public void testFaultPlacements() throws Exception {
        int depth = HeartbeatSimulationTest.property("heartbeat.sim.depth").map(Integer::parseInt).orElse(1);
        int threads = HeartbeatSimulationTest.property("heartbeat.sim.threads").map(Integer::parseInt).orElse(4);
        Optional<String> only = HeartbeatSimulationTest.property("heartbeat.sim.scenario");
        Optional<List<Fault>> faults = HeartbeatSimulationTest.property("heartbeat.sim.faults")
            .map(value -> Arrays.stream(value.split(",")).map(Fault::parse).toList());
        boolean verbose = faults.isPresent() && Boolean.getBoolean("heartbeat.sim.verbose");

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        Map<String, List<List<Fault>>> combinations = new TreeMap<>();
        Map<String, List<Future<List<String>>>> runs = new TreeMap<>();
        for (Scenario scenario : scenarios()) {
            if (only.isPresent() && !scenario.name().equals(only.get()) && !scenario.name().startsWith(only.get() + "/")) continue;
            List<List<Fault>> combos = faults.map(List::of).orElseGet(() -> combinations(scenario, depth));
            combinations.put(scenario.name(), combos);
            List<Future<List<String>>> futures = new ArrayList<>();
            for (List<Fault> combo : combos) {
                // Only the violations are kept: the simulations of all the runs do not fit in memory.
                futures.add(executor.submit(() -> {
                    HeartbeatSimulation sim = run(scenario, combo);
                    if (verbose) {
                        sim.trace().forEach(System.out::println);
                        sim.violations().forEach(System.out::println);
                    }
                    return List.copyOf(sim.violations());
                }));
            }
            runs.put(scenario.name(), futures);
        }

        Map<String, Integer> known = new TreeMap<>();
        Map<String, List<String>> unknown = new TreeMap<>();
        try {
            for (Map.Entry<String, List<Future<List<String>>>> entry : runs.entrySet()) {
                List<List<Fault>> combos = combinations.get(entry.getKey());
                for (int i = 0; i < combos.size(); i++) {
                    for (String violation : entry.getValue().get(i).get()) {
                        String kind = entry.getKey() + " " + HeartbeatSimulationTest.kindOf(violation);
                        if (HeartbeatSimulation.KNOWN_ISSUES.contains(HeartbeatSimulationTest.kindOf(violation))) {
                            known.merge(kind, 1, Integer::sum);
                        } else {
                            unknown.computeIfAbsent(kind, k -> new ArrayList<>()).add(combos.get(i) + " -> " + violation);
                        }
                    }
                }
                System.out.println(entry.getKey() + ": " + combos.size() + " runs");
            }
        } finally {
            executor.shutdownNow();
        }
        known.forEach((kind, count) -> System.out.println("Known " + kind + ": " + count + " runs"));
        unknown.forEach((kind, list) -> {
            System.out.println("Unknown " + kind + ": " + list.size() + " runs");
            list.stream().limit(10).forEach(line -> System.out.println("    " + line.split("\n")[0]));
        });
        assertEquals(List.of(), new ArrayList<>(unknown.keySet()));
    }
}
