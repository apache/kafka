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

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Runs the heartbeat simulation over a range of seeds. A run fails on a violation that is not in
 * {@link HeartbeatSimulation#KNOWN_ISSUES}.
 * <p>
 * The following system properties change the runs. Gradle sets them from the project properties
 * of the same name, e.g. {@code -Pheartbeat.sim.seeds=5000}.
 * <ul>
 *   <li>{@code heartbeat.sim.seeds} and {@code heartbeat.sim.firstSeed}: the seeds to run.</li>
 *   <li>{@code heartbeat.sim.threads}: the number of seeds that run in parallel.</li>
 *   <li>{@code heartbeat.sim.profile}: quiet, workload, network, coordinator or chaos. Drawn from
 *       the seed by default.</li>
 *   <li>{@code heartbeat.sim.commitMode}: auto or manual. Drawn from the seed by default.</li>
 *   <li>{@code heartbeat.sim.disable}: the kinds of faults and workload to turn off, separated by
 *       commas, e.g. {@code appendFailure,longProcessing}.</li>
 *   <li>{@code heartbeat.sim.verbose}: prints the trace of every run with a violation.</li>
 *   <li>{@code heartbeat.sim.logLevel}: the level of the client and coordinator loggers, off by
 *       default.</li>
 * </ul>
 */
public class HeartbeatSimulationTest {

    @BeforeAll
    public static void setUpLogging() {
        HeartbeatSimulation.setLogLevel(Level.toLevel(property("heartbeat.sim.logLevel").orElse("OFF")));
    }

    @AfterAll
    public static void resetLogging() {
        HeartbeatSimulation.resetLogLevel();
    }

    @Test
    public void testSimulation() throws Exception {
        int seeds = property("heartbeat.sim.seeds").map(Integer::parseInt).orElse(50);
        long firstSeed = property("heartbeat.sim.firstSeed").map(Long::parseLong).orElse(1L);
        int threads = property("heartbeat.sim.threads").map(Integer::parseInt).orElse(4);
        Optional<String> profile = property("heartbeat.sim.profile");
        Optional<String> commitMode = property("heartbeat.sim.commitMode");
        Set<String> disabled = property("heartbeat.sim.disable").map(value -> Set.of(value.split(","))).orElse(Set.of());
        boolean verbose = Boolean.getBoolean("heartbeat.sim.verbose");

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<HeartbeatSimulation.Result>> futures = new ArrayList<>();
        for (long seed = firstSeed; seed < firstSeed + seeds; seed++) {
            long s = seed;
            futures.add(executor.submit(() -> new HeartbeatSimulation(s, profile, commitMode, disabled).run()));
        }
        Map<String, Integer> totals = new TreeMap<>();
        Map<String, Set<Long>> seedsByViolation = new TreeMap<>();
        List<String> failures = new ArrayList<>();
        try {
            for (Future<HeartbeatSimulation.Result> future : futures) {
                HeartbeatSimulation.Result result = future.get();
                result.stats().forEach((stat, count) -> totals.merge(stat, count, Integer::sum));
                result.violations().forEach(violation ->
                    seedsByViolation.computeIfAbsent(kindOf(violation), kind -> new TreeSet<>()).add(result.seed()));
                boolean unknown = result.violations().stream().anyMatch(v -> !HeartbeatSimulation.KNOWN_ISSUES.contains(kindOf(v)));
                if (unknown || (verbose && !result.violations().isEmpty())) {
                    System.out.println(describe(result, verbose || seeds == 1));
                }
                if (unknown) failures.add("seed " + result.seed() + " (" + result.params() + ")");
            }
        } finally {
            executor.shutdownNow();
        }
        System.out.println("Totals: " + totals);
        seedsByViolation.forEach((kind, s) ->
            System.out.println(kind + ": " + s.size() + " seeds, e.g. " + s.stream().limit(20).toList()));
        assertEquals(List.of(), failures);
    }

    @Test
    public void testRunIsDeterministic() {
        for (long seed = 1; seed <= SEEDS_CHECKED_FOR_DETERMINISM; seed++) {
            HeartbeatSimulation.Result first = new HeartbeatSimulation(seed, Optional.of("chaos"), Optional.empty(), Set.of()).run();
            HeartbeatSimulation.Result second = new HeartbeatSimulation(seed, Optional.of("chaos"), Optional.empty(), Set.of()).run();
            assertEquals(first.trace(), second.trace(), "seed " + seed);
            assertEquals(first.violations(), second.violations(), "seed " + seed);
        }
    }

    private static final int SEEDS_CHECKED_FOR_DETERMINISM = 5;

    private static String describe(HeartbeatSimulation.Result result, boolean withTrace) {
        StringBuilder out = new StringBuilder();
        out.append("=== seed ").append(result.seed()).append(" (").append(result.params()).append(")\n");
        result.violations().forEach(violation -> out.append(violation).append("\n"));
        if (withTrace) {
            out.append("--- trace\n");
            result.trace().forEach(line -> out.append(line).append("\n"));
        }
        return out.toString();
    }

    static String kindOf(String violation) {
        int start = violation.indexOf(']') + 2;
        return violation.substring(start, violation.indexOf(':', start));
    }

    static Optional<String> property(String name) {
        return Optional.ofNullable(System.getProperty(name)).filter(value -> !value.isEmpty());
    }
}
