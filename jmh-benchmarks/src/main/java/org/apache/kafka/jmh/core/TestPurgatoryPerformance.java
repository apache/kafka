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

import kafka.server.DelayedOperation;
import kafka.server.DelayedOperationPurgatory;

import org.apache.kafka.server.util.ShutdownableThread;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TestPurgatoryPerformance {

    /**
     * The number of requests
     */
    @Param({"100"})
    private Integer numRequests;
    /**
     * The request rate per second
     */
    @Param({"10.0"})
    private Double requestRate;
    /**
     * The total number of possible keys
     */
    @Param({"100"})
    private Integer numPossibleKeys;
    /**
     * The number of keys for each request
     */
    @Param({"3"})
    private Integer numKeys;
    /**
     * The request timeout in ms
     */
    @Param({"1000"})
    private Long timeout;
    /**
     * 75th percentile of request latency in ms (log-normal distribution)
     */
    @Param({"0.75"})
    private Double pct75;
    /**
     * 50th percentile of request latency in ms (log-normal distribution)
     */
    @Param({"0.5"})
    private Double pct50;

    private DelayedOperationPurgatory<FakeOperation> purgatory;
    private Random rand;

    private CompletionQueue queue;

    @Setup(Level.Invocation)
    public void setUpInvocation() {
        rand = new Random();
    }

    @Setup(Level.Trial)
    public void setUpTrial() {
        purgatory = DelayedOperationPurgatory.apply("fake purgatory",
                0, 1000, true, true);
        queue = new CompletionQueue();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws InterruptedException {
        queue.shutdown();
        purgatory.shutdown();
    }

    @Benchmark
    public void testPurgatoryPerformance() throws InterruptedException {

        LatencySamples latencySamples = new LatencySamples(1000000, pct75, pct50);
        IntervalSamples intervalSamples = new IntervalSamples(1000000, requestRate);
        CountDownLatch latch = new CountDownLatch(numRequests);
        
        List<String> keys = IntStream.range(0, numRequests)
                .mapToObj(i -> String.format("fakeKey%d", rand.nextInt(numPossibleKeys)))
                .collect(Collectors.toList());

        AtomicLong requestArrivalTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong end = new AtomicLong(0);
        Runnable generate = () -> generateTask(intervalSamples, latencySamples, requestArrivalTime, latch, keys, end);

        Thread generateThread = new Thread(generate);
        generateThread.start();
        generateThread.join();
        latch.await();
    }

    private void generateTask(IntervalSamples intervalSamples,
                              LatencySamples latencySamples,
                              AtomicLong requestArrivalTime,
                              CountDownLatch latch,
                              List<String> keys,
                              AtomicLong end) {
        Integer i = numRequests;
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

            purgatory.tryCompleteElseWatch(request, CollectionConverters.asScala(
                    keys.stream().map(k -> (Object) k).collect(Collectors.toList())
            ).toSeq());
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

    private static class FakeOperation extends DelayedOperation {
        final long completesAt;
        final long latencyMs;
        final CountDownLatch latch;

        public FakeOperation(long delayMs, long latencyMs, CountDownLatch latch) {
            super(delayMs, Option.empty());
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
