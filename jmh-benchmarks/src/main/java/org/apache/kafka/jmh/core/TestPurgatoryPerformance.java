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
package org.apache.kafka.jmh.core;

import org.apache.kafka.server.purgatory.DelayedOperation;
import org.apache.kafka.server.purgatory.DelayedOperationKey;
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.ShutdownableThread;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import static java.lang.String.format;

public class TestPurgatoryPerformance {

    public static void main(String[] args) throws Exception {
        TestArgumentDefinition def = new TestArgumentDefinition(args);
        def.checkRequiredArgs();

        int numRequests = def.numRequests();
        double requestRate = def.requestRate();
        int numPossibleKeys = def.numPossibleKeys();
        int numKeys = def.numKeys();
        long timeout = def.timeout();
        double pct75 = def.pct75();
        double pct50 = def.pct50();
        boolean verbose = def.verbose();

        List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        gcMXBeans.sort(Comparator.comparing(MemoryManagerMXBean::getName));
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        LatencySamples latencySamples = new LatencySamples(1000000, pct75, pct50);
        IntervalSamples intervalSamples = new IntervalSamples(1000000, requestRate);

        DelayedOperationPurgatory<FakeOperation> purgatory =
                new DelayedOperationPurgatory<>("fake purgatory", 0, 1000);
        CompletionQueue queue = new CompletionQueue();

        List<String> gcNames = gcMXBeans.stream().map(MemoryManagerMXBean::getName).collect(Collectors.toList());
        CountDownLatch latch = new CountDownLatch(numRequests);
        long initialCpuTimeNano = getProcessCpuTimeNanos(osMXBean).orElseThrow();
        long start = System.currentTimeMillis();
        Random rand = new Random();
        List<FakeOperationKey> keys = IntStream.range(0, numKeys)
                .mapToObj(i -> new FakeOperationKey(format("fakeKey%d", rand.nextInt(numPossibleKeys))))
                .collect(Collectors.toList());

        AtomicLong requestArrivalTime = new AtomicLong(start);
        AtomicLong end = new AtomicLong(0);
        Runnable task = () -> generateTask(numRequests, timeout, purgatory, queue, intervalSamples,
                latencySamples, requestArrivalTime, latch, keys, end);

        Thread generateThread = new Thread(task);
        generateThread.start();
        generateThread.join();
        latch.await();

        long done = System.currentTimeMillis();
        queue.shutdown();

        if (verbose) {
            latencySamples.printStats();
            intervalSamples.printStats();
            System.out.printf("# enqueue rate (%d requests):%n", numRequests);
            String gcCountHeader = gcNames.stream().map(gc -> "<" + gc + " count>").collect(Collectors.joining(" "));
            String gcTimeHeader = gcNames.stream().map(gc -> "<" + gc + " time ms>").collect(Collectors.joining(" "));
            System.out.printf("# <elapsed time ms>\t<target rate>\t<actual rate>\t<process cpu time ms>\t%s\t%s%n", gcCountHeader, gcTimeHeader);
        }

        double targetRate = numRequests * 1000d / (requestArrivalTime.get() - start);
        double actualRate = numRequests * 1000d / (end.get() - start);

        Optional<Long> cpuTime = getProcessCpuTimeNanos(osMXBean).map(x -> (x - initialCpuTimeNano) / 1000000L);
        String gcCounts = gcMXBeans.stream()
                .map(GarbageCollectorMXBean::getCollectionCount)
                .map(String::valueOf)
                .collect(Collectors.joining(" "));
        String gcTimes = gcMXBeans.stream()
                .map(GarbageCollectorMXBean::getCollectionTime)
                .map(String::valueOf)
                .collect(Collectors.joining(" "));

        System.out.printf("%d\t%f\t%f\t%d\t%s\t%s%n", done - start, targetRate, actualRate, cpuTime.orElse(-1L), gcCounts, gcTimes);
        purgatory.shutdown();
    }

    private static Optional<Long> getProcessCpuTimeNanos(OperatingSystemMXBean osMXBean) {
        try {
            return Optional.of(Long.parseLong(Class.forName("com.sun.management.OperatingSystemMXBean")
                    .getMethod("getProcessCpuTime").invoke(osMXBean).toString()));
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            try {
                return Optional.of(Long.parseLong(Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
                        .getMethod("getProcessCpuTimeByNS").invoke(osMXBean).toString()));
            } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | NoSuchMethodException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static class TestArgumentDefinition {
        private final OptionParser parser;
        private final ArgumentAcceptingOptionSpec<Integer> keySpaceSizeOpt;
        private final ArgumentAcceptingOptionSpec<Double> numRequestsOpt;
        private final ArgumentAcceptingOptionSpec<Double> requestRateOpt;
        private final ArgumentAcceptingOptionSpec<Integer> numKeysOpt;
        private final ArgumentAcceptingOptionSpec<Long> timeoutOpt;
        private final ArgumentAcceptingOptionSpec<Double> pct75Opt;
        private final ArgumentAcceptingOptionSpec<Double> pct50Opt;
        private final ArgumentAcceptingOptionSpec<Boolean> verboseOpt;
        private final OptionSet options;

        public TestArgumentDefinition(String[] args) {
            this.parser = new OptionParser(false);
            this.keySpaceSizeOpt = parser
                    .accepts("key-space-size", "The total number of possible keys")
                    .withRequiredArg()
                    .describedAs("total_num_possible_keys")
                    .ofType(Integer.class)
                    .defaultsTo(100);
            this.numRequestsOpt = parser
                    .accepts("num", "The number of requests")
                    .withRequiredArg()
                    .describedAs("num_requests")
                    .ofType(Double.class);
            this.requestRateOpt = parser
                    .accepts("rate", "The request rate per second")
                    .withRequiredArg()
                    .describedAs("request_per_second")
                    .ofType(Double.class);
            this.numKeysOpt = parser
                    .accepts("keys", "The number of keys for each request")
                    .withRequiredArg()
                    .describedAs("num_keys")
                    .ofType(Integer.class)
                    .defaultsTo(3);
            this.timeoutOpt = parser
                    .accepts("timeout", "The request timeout in ms")
                    .withRequiredArg()
                    .describedAs("timeout_milliseconds")
                    .ofType(Long.class);
            this.pct75Opt = parser
                    .accepts("pct75", "75th percentile of request latency in ms (log-normal distribution)")
                    .withRequiredArg()
                    .describedAs("75th_percentile")
                    .ofType(Double.class);
            this.pct50Opt = parser
                    .accepts("pct50", "50th percentile of request latency in ms (log-normal distribution)")
                    .withRequiredArg()
                    .describedAs("50th_percentile")
                    .ofType(Double.class);
            this.verboseOpt = parser
                    .accepts("verbose", "show additional information")
                    .withRequiredArg()
                    .describedAs("true|false")
                    .ofType(Boolean.class)
                    .defaultsTo(true);
            this.options = parser.parse(args);
        }

        public void checkRequiredArgs() {
            CommandLineUtils.checkRequiredArgs(parser, options, numRequestsOpt, requestRateOpt, timeoutOpt, pct75Opt, pct50Opt);
        }

        public int numRequests() {
            return options.valueOf(numRequestsOpt).intValue();
        }

        public double requestRate() {
            return options.valueOf(requestRateOpt);
        }

        public int numPossibleKeys() {
            return options.valueOf(keySpaceSizeOpt);
        }

        public int numKeys() {
            return options.valueOf(numKeysOpt);
        }

        public long timeout() {
            return options.valueOf(timeoutOpt);
        }

        public double pct75() {
            return options.valueOf(pct75Opt);
        }

        public double pct50() {
            return options.valueOf(pct50Opt);
        }

        public boolean verbose() {
            return options.valueOf(verboseOpt);
        }
    }

    private static void generateTask(int numRequests,
                                     long timeout,
                                     DelayedOperationPurgatory<FakeOperation> purgatory,
                                     CompletionQueue queue,
                                     IntervalSamples intervalSamples,
                                     LatencySamples latencySamples,
                                     AtomicLong requestArrivalTime,
                                     CountDownLatch latch,
                                     List<FakeOperationKey> keys,
                                     AtomicLong end) {
        int i = numRequests;
        while (i > 0) {
            i -= 1;
            long requestArrivalInterval = intervalSamples.next();
            long latencyToComplete = latencySamples.next();
            long now = System.currentTimeMillis();
            requestArrivalTime.addAndGet(requestArrivalInterval);

            if (requestArrivalTime.get() > now) {
                try {
                    Thread.sleep(requestArrivalTime.get() - now);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            FakeOperation request = new FakeOperation(timeout, latencyToComplete, latch);
            if (latencyToComplete < timeout) {
                queue.add(request);
            }

            purgatory.tryCompleteElseWatch(request, keys);
        }
        end.set(System.currentTimeMillis());
    }

    /**
     * log-normal distribution (<a href="http://en.wikipedia.org/wiki/Log-normal_distribution">...</a>)
     * mu: the mean of the underlying normal distribution (not the mean of this log-normal distribution)
     * sigma: the standard deviation of the underlying normal distribution (not the stdev of this log-normal distribution)
     */
    private static class LogNormalDistribution {
        private final Random random = new Random();
        private final double mu;
        private final double sigma;

        private LogNormalDistribution(double mu, double sigma) {
            this.mu = mu;
            this.sigma = sigma;
        }

        public double next() {
            double n = random.nextGaussian() * sigma + mu;
            return Math.exp(n);
        }
    }

    /**
     * Samples of Latencies to completion
     * They are drawn from a log normal distribution.
     * A latency value can never be negative. A log-normal distribution is a convenient way to
     * model such a random variable.
     */
    private static class LatencySamples {
        private final Random random = new Random();
        private final List<Long> samples;

        public LatencySamples(int sampleSize, double pct75, double pct50) {
            this.samples = new ArrayList<>(sampleSize);
            double normalMean = Math.log(pct50);
            double normalStDev = (Math.log(pct75) - normalMean) / 0.674490d; // 0.674490 is 75th percentile point in N(0,1)
            LogNormalDistribution dist = new LogNormalDistribution(normalMean, normalStDev);
            for (int i = 0; i < sampleSize; i++) {
                samples.add((long) dist.next());
            }
        }

        public long next() {
            return samples.get(random.nextInt(samples.size()));
        }

        public void printStats() {
            List<Long> samples = this.samples.stream().sorted().collect(Collectors.toList());

            long p75 = samples.get((int) (samples.size() * 0.75d));
            long p50 = samples.get((int) (samples.size() * 0.5d));

            System.out.printf("# latency samples: pct75 = %d, pct50 = %d, min = %d, max = %d%n", p75, p50,
                    samples.stream().min(Comparator.comparingDouble(s -> s)).get(),
                    samples.stream().max(Comparator.comparingDouble(s -> s)).get());
        }
    }

    /**
     * Samples of Request arrival intervals
     * The request arrival is modeled as a Poisson process.
     * So, the internals are drawn from an exponential distribution.
     */
    private static class IntervalSamples {
        private final Random random = new Random();
        private final List<Long> samples;

        public IntervalSamples(int sampleSize, double requestPerSecond) {
            this.samples = new ArrayList<>(sampleSize);
            ExponentialDistribution dist = new ExponentialDistribution(requestPerSecond / 1000d);
            double residue = 0;
            for (int i = 0; i < sampleSize; i++) {
                double interval = dist.next() + residue;
                long roundedInterval = (long) interval;
                residue = interval - (double) roundedInterval;
                samples.add(roundedInterval);
            }
        }

        public long next() {
            return samples.get(random.nextInt(samples.size()));
        }

        public void printStats() {
            System.out.printf(
                    "# interval samples: rate = %f, min = %d, max = %d%n", 
                    1000d / (samples.stream().mapToDouble(s -> s).sum() / samples.size()), 
                    samples.stream().min(Comparator.comparingDouble(s -> s)).get(), 
                    samples.stream().max(Comparator.comparingDouble(s -> s)).get());
        }
    }

    /**
     * exponential distribution (<a href="http://en.wikipedia.org/wiki/Exponential_distribution">...</a>)
     * lambda : the rate parameter of the exponential distribution
     */
    private static class ExponentialDistribution {
        private final Random random = new Random();
        private final double lambda;

        private ExponentialDistribution(double lambda) {
            this.lambda = lambda;
        }

        public double next() {
            return Math.log(1d - random.nextDouble()) / (-lambda);
        }
    }

    private static class CompletionQueue {
        private final DelayQueue<Scheduled> delayQueue = new DelayQueue<>();
        private final ShutdownableThread thread = new ShutdownableThread("completion thread", false) {
            @Override
            public void doWork() {
                try {
                    Scheduled scheduled = delayQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (scheduled != null) {
                        scheduled.operation.forceComplete();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        public CompletionQueue() {
            thread.start();
        }

        public void add(FakeOperation operation) {
            delayQueue.add(new Scheduled(operation));
        }

        public void shutdown() throws InterruptedException {
            thread.shutdown();
        }

    }

    private static class Scheduled implements Delayed {
        final FakeOperation operation;

        public Scheduled(FakeOperation operation) {
            this.operation = operation;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(Math.max(operation.completesAt - System.currentTimeMillis(), 0), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof Scheduled) {
                Scheduled other = (Scheduled) o;
                if (operation.completesAt < other.operation.completesAt)
                    return -1;
                else if (operation.completesAt > other.operation.completesAt)
                    return 1;
            }
            return 0;
        }
    }

    private static class FakeOperationKey implements DelayedOperationKey {
        private final String key;

        public FakeOperationKey(String key) {
            this.key = key;
        }

        @Override
        public String keyLabel() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FakeOperationKey that = (FakeOperationKey) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    private static class FakeOperation extends DelayedOperation {
        final long completesAt;
        final long latencyMs;
        final CountDownLatch latch;

        public FakeOperation(long delayMs, long latencyMs, CountDownLatch latch) {
            super(delayMs);
            this.latencyMs = latencyMs;
            this.latch = latch;
            completesAt = System.currentTimeMillis() + delayMs;
        }

        @Override
        public void onExpiration() {

        }

        @Override
        public void onComplete() {
            latch.countDown();
        }

        @Override
        public boolean tryComplete() {
            return System.currentTimeMillis() >= completesAt && forceComplete();
        }
    }
}
