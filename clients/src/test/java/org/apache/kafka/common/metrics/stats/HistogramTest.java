/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;


import org.apache.kafka.common.metrics.stats.Histogram.BinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme;
import org.junit.Test;

public class HistogramTest {

    private static final double EPS = 0.0000001d;

    @Test
    public void testHistogram() {
        BinScheme scheme = new ConstantBinScheme(12, -5, 5);
        Histogram hist = new Histogram(scheme);
        for (int i = -5; i < 5; i++)
            hist.record(i);
        for (int i = 0; i < 10; i++)
            assertEquals(scheme.fromBin(i + 1), hist.value(i / 10.0 + EPS), EPS);
    }

    @Test
    public void testConstantBinScheme() {
        ConstantBinScheme scheme = new ConstantBinScheme(5, -5, 5);
        assertEquals("A value below the lower bound should map to the first bin", 0, scheme.toBin(-5.01));
        assertEquals("A value above the upper bound should map to the last bin", 4, scheme.toBin(5.01));
        assertEquals("Check boundary of bucket 1", 1, scheme.toBin(-5));
        assertEquals("Check boundary of bucket 4", 4, scheme.toBin(5));
        assertEquals("Check boundary of bucket 3", 3, scheme.toBin(4.9999));
        checkBinningConsistency(new ConstantBinScheme(4, 0, 5));
        checkBinningConsistency(scheme);
    }

    @Test
    public void testLinearBinScheme() {
        LinearBinScheme scheme = new LinearBinScheme(10, 10);
        checkBinningConsistency(scheme);
    }

    private void checkBinningConsistency(BinScheme scheme) {
        for (int bin = 0; bin < scheme.bins(); bin++) {
            double fromBin = scheme.fromBin(bin);
            int binAgain = scheme.toBin(fromBin + EPS);
            assertEquals("unbinning and rebinning the bin " + bin
                         + " gave a different result ("
                         + fromBin
                         + " was placed in bin "
                         + binAgain
                         + " )", bin, binAgain);
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        System.out.println("[-100, 100]:");
        for (BinScheme scheme : Arrays.asList(new ConstantBinScheme(1000, -100, 100),
                                              new ConstantBinScheme(100, -100, 100),
                                              new ConstantBinScheme(10, -100, 100))) {
            Histogram h = new Histogram(scheme);
            for (int i = 0; i < 10000; i++)
                h.record(200.0 * random.nextDouble() - 100.0);
            for (double quantile = 0.0; quantile < 1.0; quantile += 0.05)
                System.out.printf("%5.2f: %.1f, ", quantile, h.value(quantile));
            System.out.println();
        }

        System.out.println("[0, 1000]");
        for (BinScheme scheme : Arrays.asList(new LinearBinScheme(1000, 1000),
                                              new LinearBinScheme(100, 1000),
                                              new LinearBinScheme(10, 1000))) {
            Histogram h = new Histogram(scheme);
            for (int i = 0; i < 10000; i++)
                h.record(1000.0 * random.nextDouble());
            for (double quantile = 0.0; quantile < 1.0; quantile += 0.05)
                System.out.printf("%5.2f: %.1f, ", quantile, h.value(quantile));
            System.out.println();
        }
    }

}
