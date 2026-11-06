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

import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.coordinator.group.streams.RefinerFuzzMetrics.ScenarioResult;
import org.apache.kafka.coordinator.group.streams.RefinerFuzzMetrics.Totals;
import org.apache.kafka.coordinator.group.streams.RefinerFuzzScenario.Profile;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A randomized, deterministic testbed for the broker-side {@link AssignmentRefiner} implementations.
 *
 * <p>Each scenario ({@link RefinerFuzzScenario}) is generated from a seed and run by {@link RefinerFuzzSimulator}
 * through the real reconciler until the group has converged on its final target assignment. Every refiner call is
 * checked against the invariants in {@link RefinerInvariants}, and the run is measured ({@link RefinerFuzzMetrics}).
 * Every scenario is also run under {@link NoOpAssignmentRefiner}, the baseline a refiner has to improve on along
 * its target dimension.
 *
 * <p>In CI, a fixed seed runs {@value #CI_SCENARIOS} scenarios per refiner. The test fails on any invariant
 * violation, on a scenario that does not converge, on a refiner not improving on the baseline along its target
 * dimension over all scenarios together, and on any aggregate measurement rising more than
 * {@value #PINNED_TOLERANCE_PERCENT}% above its pinned value in {@link RefinerUnderTest}. The pinned values are upper
 * bounds: a change that improves a measurement passes, and should lower the pinned value with it. The refiner call
 * times are reported but not asserted.
 *
 * <p>{@link #main} runs any seed, scenario count and profile, and prints the measurements, for example
 * {@code --refiner WARMUP --seed 42 --scenarios 2000 --profile LARGE}. A failure names the scenario seed that
 * reproduces it, which {@code --scenario-seed <seed> --trace} replays step by step.
 */
public class RefinerFuzzTest {

    static final long CI_SEED = 1071L;
    static final int CI_SCENARIOS = 100;
    static final int PINNED_TOLERANCE_PERCENT = 5;

    /**
     * The refiners under test, each with the invariants it is held to, what it has to improve on the baseline, and
     * its pinned measurements for the CI seed.
     */
    enum RefinerUnderTest {
        NO_OP(NoOpAssignmentRefiner::new, false, new Pinned(388, 1638, 2242, 0)),

        /**
         * The warm-up refiner exists to avoid cold hand-overs: over all scenarios, it has to hand over fewer tasks
         * cold than the baseline and keep the group processing more of the time.
         *
         * <p>This is not required of every single scenario, because the sticky assignor computes its next target
         * assignment from the current assignment, which is exactly what a refiner shapes: in a rare scenario the
         * two refiners end up with different target assignments, and the one the warm-up refiner gets can cost it a
         * cold hand-over the baseline does not have. Those scenarios are counted and reported.
         */
        WARMUP(AssignmentRefinerImpl::new, true, new Pinned(130, 1078, 2517, 190)) {
            @Override
            boolean isWorseThanBaseline(final ScenarioResult result, final ScenarioResult baseline) {
                return result.metrics().coldHandOvers() > baseline.metrics().coldHandOvers();
            }

            @Override
            List<String> compareWithBaseline(final Totals totals, final Totals baseline) {
                final List<String> failures = new ArrayList<>();
                if (baseline.coldHandOvers() > 0 && totals.coldHandOvers() >= baseline.coldHandOvers()) {
                    failures.add("target dimension: " + totals.coldHandOvers() + " cold hand-overs in total, no "
                        + "fewer than the " + baseline.coldHandOvers() + " of " + NO_OP);
                }
                if (totals.notProcessingTaskTicks() >= baseline.notProcessingTaskTicks()) {
                    failures.add("target dimension: " + totals.notProcessingTaskTicks() + " task-ticks not processing "
                        + "in total, no fewer than the " + baseline.notProcessingTaskTicks() + " of " + NO_OP);
                }
                return failures;
            }
        };

        final Supplier<AssignmentRefiner> factory;
        final boolean checkWarmupInvariants;
        final Pinned pinned;

        RefinerUnderTest(
            final Supplier<AssignmentRefiner> factory,
            final boolean checkWarmupInvariants,
            final Pinned pinned
        ) {
            this.factory = factory;
            this.checkWarmupInvariants = checkWarmupInvariants;
            this.pinned = pinned;
        }

        /**
         * Whether the refiner did worse than the baseline along its target dimension in a single scenario.
         */
        boolean isWorseThanBaseline(final ScenarioResult result, final ScenarioResult baseline) {
            return false;
        }

        /**
         * What this refiner has to improve on the baseline over all scenarios together.
         */
        List<String> compareWithBaseline(final Totals totals, final Totals baseline) {
            return List.of();
        }
    }

    /**
     * Upper bounds, before the tolerance, on the aggregate measurements of the {@value #CI_SCENARIOS} scenarios of
     * the CI seed.
     */
    record Pinned(long coldHandOvers, long notProcessingTaskTicks, long ticks, long refinementSteps) {

        List<String> check(final Totals totals) {
            final List<String> regressions = new ArrayList<>();
            check("cold hand-overs", totals.coldHandOvers(), coldHandOvers, regressions);
            check("task-ticks not processing", totals.notProcessingTaskTicks(), notProcessingTaskTicks, regressions);
            check("ticks to converge", totals.ticks(), ticks, regressions);
            check("refinement steps", totals.refinementSteps(), refinementSteps, regressions);
            return regressions;
        }

        private static void check(final String name, final long actual, final long pinned, final List<String> regressions) {
            final long bound = pinned + Math.max(1, pinned * PINNED_TOLERANCE_PERCENT / 100);
            if (actual > bound) {
                regressions.add(String.format(Locale.ROOT, "regression: %d %s, above the pinned %d (+%d%%)",
                    actual, name, pinned, PINNED_TOLERANCE_PERCENT));
            }
        }
    }

    /**
     * The outcome of running many scenarios under one refiner and the baseline.
     */
    record FuzzRun(
        RefinerUnderTest refiner,
        Totals totals,
        Totals baseline,
        List<Long> worseThanBaseline,
        List<String> failures
    ) {

        String report() {
            final StringBuilder builder = new StringBuilder()
                .append(Totals.header()).append('\n')
                .append(totals.row(refiner.name())).append('\n');
            if (refiner != RefinerUnderTest.NO_OP) {
                builder.append(baseline.row(RefinerUnderTest.NO_OP.name())).append('\n')
                    .append(comparison()).append('\n');
            }
            failures.forEach(failure -> builder.append(failure).append('\n'));
            return builder.toString();
        }

        String comparison() {
            return refiner + " vs " + RefinerUnderTest.NO_OP + ": " + totals.compareWith(baseline) + "; worse in "
                + worseThanBaseline.size() + " of " + totals.scenarios() + " scenarios"
                + (worseThanBaseline.isEmpty() ? "" : " " + worseThanBaseline.subList(0, Math.min(10, worseThanBaseline.size())));
        }
    }

    @ParameterizedTest
    @EnumSource(RefinerUnderTest.class)
    public void shouldKeepInvariantsAndImproveOnBaseline(final RefinerUnderTest refiner) {
        final FuzzRun run = fuzz(refiner, CI_SEED, CI_SCENARIOS, Profile.CI, null);

        final List<String> failures = new ArrayList<>(run.failures());
        failures.addAll(refiner.compareWithBaseline(run.totals(), run.baseline()));
        failures.addAll(refiner.pinned.check(run.totals()));
        assertEquals(List.of(), failures, run::report);
    }

    /**
     * Runs {@code scenarioCount} scenarios, generated from seeds drawn from {@code masterSeed}, under the refiner and
     * under the baseline.
     *
     * @param verbose Where to print one line of measurements per scenario, or {@code null} for nowhere.
     */
    static FuzzRun fuzz(
        final RefinerUnderTest refiner,
        final long masterSeed,
        final int scenarioCount,
        final Profile profile,
        final PrintStream verbose
    ) {
        return fuzz(refiner, drawSeeds(masterSeed, scenarioCount), profile, verbose, null);
    }

    private static FuzzRun fuzz(
        final RefinerUnderTest refiner,
        final List<Long> scenarioSeeds,
        final Profile profile,
        final PrintStream verbose,
        final PrintStream trace
    ) {
        final Totals totals = new Totals();
        final Totals baselineTotals = new Totals();
        final List<Long> worseThanBaseline = new ArrayList<>();
        final List<String> failures = new ArrayList<>();

        for (final long scenarioSeed : scenarioSeeds) {
            final RefinerFuzzScenario scenario = RefinerFuzzScenario.generate(scenarioSeed, profile);
            final ScenarioResult result = new RefinerFuzzSimulator(
                scenario, refiner.factory.get(), refiner.checkWarmupInvariants, trace).run();
            final ScenarioResult baseline = refiner == RefinerUnderTest.NO_OP
                ? result
                : new RefinerFuzzSimulator(scenario, new NoOpAssignmentRefiner(), false, null).run();
            totals.add(result);
            baselineTotals.add(baseline);

            if (refiner.isWorseThanBaseline(result, baseline)) {
                worseThanBaseline.add(scenarioSeed);
            }
            final List<String> scenarioFailures = result.violations();
            if (!scenarioFailures.isEmpty()) {
                failures.add(String.format(Locale.ROOT,
                    "scenario %d failed (replay: RefinerFuzzTest --refiner %s --profile %s --scenario-seed %d --trace)"
                        + "%n  %s%n  %s%n  %s", scenarioSeed, refiner, profile, scenarioSeed, scenario.describe(),
                    result.metrics().describe(), String.join("\n  ", scenarioFailures)));
            }
            if (verbose != null) {
                verbose.printf(Locale.ROOT, "%-8s %20d %s%n", refiner, scenarioSeed, result.metrics().describe());
            }
        }
        return new FuzzRun(refiner, totals, baselineTotals, worseThanBaseline, failures);
    }

    /**
     * Runs the testbed by hand.
     *
     * <pre>
     *   --refiner &lt;NO_OP|WARMUP|all&gt;   the refiners to run (default: all)
     *   --seed &lt;long&gt;                  the master seed the scenario seeds are drawn from (default: the CI seed)
     *   --scenarios &lt;n&gt;                how many scenarios to run (default: {@value #CI_SCENARIOS})
     *   --profile &lt;CI|LARGE|XLARGE&gt;    how big the scenarios get (default: CI); an XLARGE scenario takes
     *                                   seconds to minutes
     *   --scenario-seed &lt;long&gt;         run only the scenario with this seed, instead of drawing seeds
     *   --verbose                       print the measurements of every scenario
     *   --trace                         print every step of every scenario
     * </pre>
     */
    public static void main(final String[] args) {
        final ManualRun options = ManualRun.parse(args);
        final List<Long> scenarioSeeds = options.scenarioSeeds();
        System.out.printf(Locale.ROOT, "%d scenarios, profile %s, %s%n", scenarioSeeds.size(), options.profile(),
            options.scenarioSeed() != null ? "scenario seed " + options.scenarioSeed() : "master seed " + options.masterSeed());

        final List<FuzzRun> runs = new ArrayList<>();
        for (final RefinerUnderTest refiner : options.refiners()) {
            final long start = System.nanoTime();
            runs.add(fuzz(refiner, scenarioSeeds, options.profile(), options.verbose() ? System.out : null,
                options.trace() ? System.out : null));
            System.out.printf(Locale.ROOT, "%s took %.1f s%n", refiner, (System.nanoTime() - start) / 1e9);
        }

        printReport(runs);
        Exit.exit(runs.stream().allMatch(run -> run.failures().isEmpty()) ? 0 : 1);
    }

    private static void printReport(final List<FuzzRun> runs) {
        System.out.println();
        System.out.println(Totals.header());
        runs.forEach(run -> System.out.println(run.totals().row(run.refiner().name())));
        if (runs.stream().noneMatch(run -> run.refiner() == RefinerUnderTest.NO_OP)) {
            System.out.println(runs.get(0).baseline().row(RefinerUnderTest.NO_OP.name()));
        }
        System.out.println();
        runs.stream()
            .filter(run -> run.refiner() != RefinerUnderTest.NO_OP)
            .forEach(run -> System.out.println(run.comparison()));
        for (final FuzzRun run : runs) {
            if (!run.failures().isEmpty()) {
                System.out.println();
                System.out.println(run.refiner() + ": " + run.failures().size() + " failures");
                run.failures().forEach(System.out::println);
            }
        }
    }

    /**
     * The options of a run by hand, see {@link #main}.
     */
    private record ManualRun(
        List<RefinerUnderTest> refiners,
        long masterSeed,
        int scenarioCount,
        Profile profile,
        Long scenarioSeed,
        boolean verbose,
        boolean trace
    ) {

        static ManualRun parse(final String[] args) {
            String refiner = "all";
            long masterSeed = CI_SEED;
            int scenarioCount = CI_SCENARIOS;
            Profile profile = Profile.CI;
            Long scenarioSeed = null;
            boolean verbose = false;
            boolean trace = false;

            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--refiner" -> refiner = args[++index];
                    case "--seed" -> masterSeed = Long.parseLong(args[++index]);
                    case "--scenarios" -> scenarioCount = Integer.parseInt(args[++index]);
                    case "--profile" -> profile = Profile.valueOf(args[++index].toUpperCase(Locale.ROOT));
                    case "--scenario-seed" -> scenarioSeed = Long.parseLong(args[++index]);
                    case "--verbose" -> verbose = true;
                    case "--trace" -> trace = true;
                    default -> throw new IllegalArgumentException("Unknown argument " + args[index]
                        + "; see the javadoc of RefinerFuzzTest#main.");
                }
            }

            final List<RefinerUnderTest> refiners = refiner.equalsIgnoreCase("all")
                ? List.of(RefinerUnderTest.values())
                : List.of(RefinerUnderTest.valueOf(refiner.toUpperCase(Locale.ROOT)));
            return new ManualRun(refiners, masterSeed, scenarioCount, profile, scenarioSeed, verbose, trace);
        }

        List<Long> scenarioSeeds() {
            return scenarioSeed != null ? List.of(scenarioSeed) : drawSeeds(masterSeed, scenarioCount);
        }
    }

    private static List<Long> drawSeeds(final long masterSeed, final int scenarioCount) {
        final Random seeds = new Random(masterSeed);
        final List<Long> scenarioSeeds = new ArrayList<>();
        for (int scenario = 0; scenario < scenarioCount; scenario++) {
            scenarioSeeds.add(seeds.nextLong());
        }
        return scenarioSeeds;
    }
}
