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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.utils.ThreadUtils;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HdrHistogramTest {

    private static final double[] QUANTILES = new double[]{0.5, 0.75, 0.95, 0.98, 0.99, 0.999};

    private static final long MAX_VALUE = TimeUnit.MINUTES.toMillis(1L);
    private static final long REGULAR_VALUE = 100L;
    private static final int NUM_SIGNIFICANT_DIGITS = 3;

    @Test
    public void testHdrVsYammerUniform() {
        List<Long> samples = new ArrayList<>();
        for (long i = 0; i <= MAX_VALUE; i++) { // 60k iterations with the current constants
            samples.add(i);
        }
        testHdrHistogramVsYammerHistogram(samples, "uniform");
    }

    @Test
    public void testHdrVsYammerNormal() {
        List<Long> samples = new ArrayList<>();
        for (long i = 0; i <= MAX_VALUE; i++) { // 60k iterations with the current constants
            samples.add(ThreadLocalRandom.current().nextLong(MAX_VALUE));
        }
        testHdrHistogramVsYammerHistogram(samples, "normal");
    }

    @Test
    public void testHdrVsYammerBimodal() {
        List<Long> samples = new ArrayList<>();
        for (long i = 0; i <= MAX_VALUE; i++) { // 60k iterations with the current constants
            if (i % 500 == 0) {
                // generate a tail-like latency value, and do it once in every 500 iterations, so that the
                // p999 value is expected to reflect it
                samples.add(ThreadLocalRandom.current().nextLong(MAX_VALUE));
            } else {
                // generate a normal latency value
                samples.add(ThreadLocalRandom.current().nextLong(REGULAR_VALUE));
            }
        }
        testHdrHistogramVsYammerHistogram(samples, "bimodal");
    }

    private void testHdrHistogramVsYammerHistogram(List<Long> samples, String distributionLabel) {
        HdrHistogram hdrHistogram = new HdrHistogram(MAX_VALUE, NUM_SIGNIFICANT_DIGITS);
        Histogram yammerHistogram = new MetricsRegistry().newHistogram(new MetricName("", "", ""),
            true);

        Collections.sort(samples);
        double[] expectedQuantileValues = new double[QUANTILES.length];
        for (int i = 0; i < QUANTILES.length; i++) {
            int quantileIndex = (int) (Math.ceil(samples.size() * QUANTILES[i])) - 1;
            expectedQuantileValues[i] = samples.get(quantileIndex);
        }
        Collections.shuffle(samples);

        for (long sample : samples) {
            hdrHistogram.record(sample);
            yammerHistogram.update(sample);
        }

        System.out.printf("Testing HdrHistogram vs Yammer histogram for %s distribution%n",
            distributionLabel);
        long now = System.currentTimeMillis();
        int numYammerWins = 0;
        for (int i = 0; i < QUANTILES.length; i++) {
            double quantile = QUANTILES[i];
            double hdrHistogramValue = hdrHistogram.measurePercentile(now, quantile * 100);
            double yammerHistogramValue = yammerHistogram.getSnapshot().getValue(quantile);
            double expectedValue = expectedQuantileValues[i];
            System.out.printf(
                "Values for quantile %f: HdrHistogram: %f, Yammer histogram: %f, Expected: %f%n",
                quantile, hdrHistogramValue, yammerHistogramValue, expectedValue);
            if (Math.abs(expectedValue - hdrHistogramValue) > Math.abs(
                expectedValue - yammerHistogramValue)) {
                numYammerWins++;
            }
        }
        System.out.printf("HdrHistogram was more accurate: %d out of %d times%n",
            QUANTILES.length - numYammerWins, QUANTILES.length);
        assertTrue(numYammerWins <= QUANTILES.length / 2);
    }

    @Test
    public void testCount() throws Exception {
        int numUpdates = 100_000;
        HdrHistogram hdrHistogram = new HdrHistogram(
            MAX_VALUE, NUM_SIGNIFICANT_DIGITS);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < numUpdates; i++) {
            executorService.submit(() -> hdrHistogram.record(1L));
        }
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(1L, TimeUnit.SECONDS));
        assertEquals(numUpdates, hdrHistogram.count(System.currentTimeMillis()));
    }

    @Test
    public void testMax() throws Exception {
        int numSignificantDigits = 5;
        int numUpdates = 30_000;
        double expectedMax = 0.0;
        long[] values = new long[numUpdates];
        for (int i = 0; i < numUpdates; i++) {
            long value = ThreadLocalRandom.current().nextLong(MAX_VALUE);
            values[i] = value;
            expectedMax = Math.max(expectedMax, value);
        }

        HdrHistogram hdrHistogram = new HdrHistogram(MAX_VALUE, numSignificantDigits);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < numUpdates; i++) {
            final long value = values[i];
            executorService.submit(() -> hdrHistogram.record(value));
        }
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(1L, TimeUnit.SECONDS));

        double histogramMax = hdrHistogram.max(System.currentTimeMillis());
        assertEquals(expectedMax, histogramMax);
    }

    @Test
    public void testHistogramDataReset() {
        long maxSnapshotAgeMs = 10L;
        HdrHistogram hdrHistogram = new HdrHistogram(maxSnapshotAgeMs, MAX_VALUE, 3);
        int numEventsInFirstCycle = 1000;
        for (int i = 0; i < numEventsInFirstCycle; i++) {
            hdrHistogram.record(i);
        }
        long now = System.currentTimeMillis();
        assertEquals(numEventsInFirstCycle, hdrHistogram.count(now));
        int numEventsInSecondCycle = 2000;
        for (int i = 0; i < numEventsInSecondCycle; i++) {
            hdrHistogram.record(i);
        }
        assertEquals(numEventsInFirstCycle, hdrHistogram.count(now));
        assertEquals(numEventsInFirstCycle, hdrHistogram.count(now + maxSnapshotAgeMs));
        assertEquals(numEventsInSecondCycle, hdrHistogram.count(now + 1 + maxSnapshotAgeMs));
    }

    @Test
    public void testLatestHistogramRace() throws InterruptedException, ExecutionException {
        long maxSnapshotAgeMs = 10L;
        long now = System.currentTimeMillis();
        HdrHistogram hdrHistogram = new HdrHistogram(maxSnapshotAgeMs, MAX_VALUE, 1);
        ExecutorService countExecutor = Executors.newFixedThreadPool(2);
        for (int i = 1; i < 10000; i++) {
            int numEvents = 2;
            for (int j = 0; j < numEvents; j++) {
                hdrHistogram.record(i);
            }
            final long moreThanMaxAge = now + maxSnapshotAgeMs + 1;
            now = moreThanMaxAge;
            CountDownLatch latch = new CountDownLatch(1);
            Callable<Long> countTask = () -> {
                try {
                    assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
                    return hdrHistogram.count(moreThanMaxAge);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            Future<Long> t1Future = countExecutor.submit(countTask);
            Future<Long> t2Future = countExecutor.submit(countTask);
            latch.countDown();
            long t1Count = t1Future.get();
            long t2Count = t2Future.get();
            assertTrue(
                numEvents == t1Count && numEvents == t2Count,
                String.format("Expected %d events in both threads, got %d in T1 and %d in T2",
                    numEvents, t1Count, t2Count));
        }
        ThreadUtils.shutdownExecutorServiceQuietly(countExecutor, 500, TimeUnit.MILLISECONDS);
    }
}
