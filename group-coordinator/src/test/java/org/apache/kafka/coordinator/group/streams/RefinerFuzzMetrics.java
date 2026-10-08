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
package org.apache.kafka.coordinator.group.streams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * What {@link RefinerFuzzSimulator} measures while it runs one scenario under one refiner, and how the measurements
 * of many scenarios add up.
 *
 * <p>All of it is measured against the simulator's ground truth of where state actually lives and how far it has
 * been restored, not against what the refiner believes, so the measurements compare refiners on equal terms.
 */
final class RefinerFuzzMetrics {

    private RefinerFuzzMetrics() {
    }

    /**
     * The measurements of one scenario run.
     *
     * @param coldHandOvers
     *        How often a stateful active task was granted to a member whose process did not hold its state within
     *        {@code acceptable.recovery.lag}, so that processing the task stalled behind a restore. This is what a
     *        warm-up refiner exists to avoid.
     * @param unavoidableColdHandOvers
     *        The cold hand-overs for which no process in the group held the task's state within
     *        {@code acceptable.recovery.lag}, not even on disk, so that no refiner could have avoided them.
     * @param diskOnlyColdHandOvers
     *        The cold hand-overs for which the task's state was within {@code acceptable.recovery.lag} only on the
     *        disk of a process that held no role for the task, typically the process of a member that just left. A
     *        refiner can only avoid those by handing the task to a member of that process.
     * @param notProcessingTaskTicks
     *        Summed over all ticks, the stateful active tasks nobody was processing: either no member held them, or
     *        the member holding them was still restoring them. The availability cost of the scenario.
     * @param ticks
     *        How many ticks the scenario ran for.
     * @param converged
     *        Whether the group reached its final target assignment within the scenario's tick limit.
     * @param refinementSteps
     *        How many assignment epochs a refinement step minted on a settled group, on top of the epochs of
     *        new target assignments.
     * @param maxConcurrentWarmups
     *        The largest number of warm-up tasks the members held at once.
     * @param maxExtraCopies
     *        The largest number of task copies, over all tasks, beyond the active and the standby copies the target
     *        assignment asks for.
     * @param wastedWarmups
     *        Warm-up tasks that were revoked without their member taking the task over, as an active or a standby
     *        task, which throws their restore work away.
     * @param refineCalls
     *        How often the refiner was called.
     * @param refineTotalMs
     *        The time spent in all refiner calls together, in milliseconds.
     * @param refineMaxMs
     *        The time the slowest refiner call took, in milliseconds.
     */
    record ScenarioMetrics(
        long coldHandOvers,
        long unavoidableColdHandOvers,
        long diskOnlyColdHandOvers,
        long notProcessingTaskTicks,
        int ticks,
        boolean converged,
        int refinementSteps,
        int maxConcurrentWarmups,
        int maxExtraCopies,
        long wastedWarmups,
        int refineCalls,
        double refineTotalMs,
        double refineMaxMs
    ) {

        String describe() {
            return String.format(Locale.ROOT,
                "cold=%d (unavoidable %d, disk only %d) notProcessing=%d ticks=%d%s steps=%d maxWarmups=%d maxExtraCopies=%d "
                    + "wastedWarmups=%d refineCalls=%d refineTotal=%.2fms refineMax=%.3fms",
                coldHandOvers, unavoidableColdHandOvers, diskOnlyColdHandOvers, notProcessingTaskTicks, ticks, converged ? "" : " (NOT CONVERGED)",
                refinementSteps, maxConcurrentWarmups, maxExtraCopies, wastedWarmups, refineCalls, refineTotalMs,
                refineMaxMs);
        }
    }

    /**
     * The outcome of one scenario under one refiner.
     *
     * @param scenarioSeed The seed the scenario was generated from, which reproduces it.
     * @param metrics      What was measured.
     * @param violations   The invariants the refiner broke, empty if it broke none.
     */
    record ScenarioResult(long scenarioSeed, ScenarioMetrics metrics, List<String> violations) {
    }

    /**
     * The measurements of many scenarios added up.
     */
    static final class Totals {

        private int scenarios;
        private int failedScenarios;
        private int unconvergedScenarios;
        private long coldHandOvers;
        private long unavoidableColdHandOvers;
        private long diskOnlyColdHandOvers;
        private long notProcessingTaskTicks;
        private long ticks;
        private long refinementSteps;
        private long wastedWarmups;
        private int maxConcurrentWarmups;
        private int maxExtraCopies;
        private long refineCalls;
        private double refineTotalMs;
        private final List<Double> refineMaxMs = new ArrayList<>();

        void add(final ScenarioResult result) {
            final ScenarioMetrics metrics = result.metrics();
            scenarios++;
            if (!result.violations().isEmpty()) {
                failedScenarios++;
            }
            if (!metrics.converged()) {
                unconvergedScenarios++;
            }
            coldHandOvers += metrics.coldHandOvers();
            unavoidableColdHandOvers += metrics.unavoidableColdHandOvers();
            diskOnlyColdHandOvers += metrics.diskOnlyColdHandOvers();
            notProcessingTaskTicks += metrics.notProcessingTaskTicks();
            ticks += metrics.ticks();
            refinementSteps += metrics.refinementSteps();
            wastedWarmups += metrics.wastedWarmups();
            maxConcurrentWarmups = Math.max(maxConcurrentWarmups, metrics.maxConcurrentWarmups());
            maxExtraCopies = Math.max(maxExtraCopies, metrics.maxExtraCopies());
            refineCalls += metrics.refineCalls();
            refineTotalMs += metrics.refineTotalMs();
            refineMaxMs.add(metrics.refineMaxMs());
        }

        int scenarios() {
            return scenarios;
        }

        int failedScenarios() {
            return failedScenarios;
        }

        long coldHandOvers() {
            return coldHandOvers;
        }

        long notProcessingTaskTicks() {
            return notProcessingTaskTicks;
        }

        long ticks() {
            return ticks;
        }

        long refinementSteps() {
            return refinementSteps;
        }

        static String header() {
            return String.format(Locale.ROOT,
                "%-8s %9s %6s %7s %8s %8s %8s %13s %8s %7s %7s %8s %8s %11s %9s %9s %9s",
                "refiner", "scenarios", "failed", "unconv", "cold", "unavoid", "diskOnly", "notProcessing", "ticks", "steps",
                "wasted", "maxWarm", "maxXtra", "refineCalls", "mean(ms)", "p99(ms)", "max(ms)");
        }

        String row(final String refiner) {
            return String.format(Locale.ROOT,
                "%-8s %9d %6d %7d %8d %8d %8d %13d %8d %7d %7d %8d %8d %11d %9.4f %9.3f %9.3f",
                refiner, scenarios, failedScenarios, unconvergedScenarios, coldHandOvers, unavoidableColdHandOvers,
                diskOnlyColdHandOvers, notProcessingTaskTicks, ticks, refinementSteps, wastedWarmups, maxConcurrentWarmups, maxExtraCopies,
                refineCalls, refineCalls == 0 ? 0.0 : refineTotalMs / refineCalls, percentile(0.99), percentile(1.0));
        }

        /**
         * How these totals compare with the baseline's, as relative changes.
         */
        String compareWith(final Totals baseline) {
            return String.format(Locale.ROOT, "cold %s, task-ticks not processing %s, ticks %s",
                change(coldHandOvers, baseline.coldHandOvers), change(notProcessingTaskTicks, baseline.notProcessingTaskTicks),
                change(ticks, baseline.ticks));
        }

        private static String change(final long value, final long baseline) {
            return baseline == 0 ? value + " vs 0" : String.format(Locale.ROOT, "%+.1f%%", 100.0 * (value - baseline) / baseline);
        }

        /**
         * The given percentile of the slowest refiner call per scenario.
         */
        double percentile(final double percentile) {
            if (refineMaxMs.isEmpty()) {
                return 0.0;
            }
            final List<Double> sorted = new ArrayList<>(refineMaxMs);
            Collections.sort(sorted);
            final int index = (int) Math.ceil(percentile * sorted.size()) - 1;
            return sorted.get(Math.max(0, Math.min(sorted.size() - 1, index)));
        }
    }
}
