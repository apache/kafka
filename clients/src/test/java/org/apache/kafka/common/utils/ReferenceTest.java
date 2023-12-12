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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LongSummaryStatistics;

public class ReferenceTest {

    private static final Logger log = LoggerFactory.getLogger(ReferenceTest.class);
    private static final int RUNS = 10;
    private static final int STEPS = 10;
    private static final long SPINS_PER_STEP = 200_000L;

    static {
        // Warm up first to make sure that no classloading takes place
        iter(100_000L);
    }

    private static long iter(long count) {
        long before = System.nanoTime();
        for (int i = 0; i < count; i++) {
            Thread.yield();
        }
        long after = System.nanoTime();
        return after - before;
    }

    private static String testSpins(int runs, int steps, long spinsPerStep) {
        long[] x = new long[runs * steps];
        long[] y = new long[runs * steps];
        Utils.sleep(1000);
        int c = 0;
        for (int i = 0; i < runs; i++) {
            for (int j = 0; j < steps; j++) {
                long spins = (j + 1) * spinsPerStep;
                x[c] = spins;
                y[c] = iter(spins);
                c++;
            }
        }

        return String.format("runs: %d steps: %d spins: %d ", runs, steps, spinsPerStep) + linearRegression(x, y);
    }

    private static String linearRegression(long[] xr, long[] yr) {
        LongSummaryStatistics x = new LongSummaryStatistics();
        LongSummaryStatistics y = new LongSummaryStatistics();
        LongSummaryStatistics x2 = new LongSummaryStatistics();
        LongSummaryStatistics y2 = new LongSummaryStatistics();
        LongSummaryStatistics xy = new LongSummaryStatistics();

        for (int i = 0; i < xr.length; i++) {
            long xv = xr[i];
            long yv = yr[i];
            x.accept(xv);
            x2.accept(xv * xv);
            y.accept(yv);
            y2.accept(yv * yv);
            xy.accept(xv * yv);
        }
        double slope = (xy.getAverage() - x.getAverage() * y.getAverage())
                / (x2.getAverage() - x.getAverage() * x.getAverage());
        double inter = y.getAverage() - slope * x.getAverage();

        double sst = 0;
        double ssr = 0;
        for (int i = 0; i < xr.length; i++) {
            double a = yr[i] - y.getAverage();
            sst += a * a;
            double b = yr[i] - (slope * xr[i] + inter);
            ssr += b * b;
        }
        double rSquared = 1 - (ssr / sst);

        return String.format("ns/op: %f R2: %f", slope, rSquared);
    }

    /**
     * Run a single test with fixed parameters which appear to produce the consistent results
     * This test intentionally fails in order to make results visible to the test harness.
     */
    @Test
    public void runTest() {
        throw new AssertionFailedError(testSpins(RUNS, STEPS, SPINS_PER_STEP));
    }

    /**
     * This is a heavyweight test which prints multiple results to statistically compare test parameterization
     */
    @Disabled
    @Test
    public void calibrate() {
        for (int i = 0; i < 5; i++) {
            log.error("{}", testSpins(10, 10, 200_000));
            log.error("{}", testSpins(5, 20, 200_000));
            log.error("{}", testSpins(5, 10, 400_000));
            log.error("{}", testSpins(20, 5, 200_000));
            log.error("{}", testSpins(10, 5, 400_000));
            log.error("{}", testSpins(20, 10, 100_000));
            log.error("{}", testSpins(10, 20, 100_000));
        }
    }

}
