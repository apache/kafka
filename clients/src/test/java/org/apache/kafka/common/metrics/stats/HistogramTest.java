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
package org.apache.kafka.common.metrics.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Random;


import org.apache.kafka.common.metrics.stats.Histogram.BinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme;
import org.junit.jupiter.api.Test;

public class HistogramTest {

    private static final double EPS = 0.0000001d;

    @Test
    public void testHistogram() {
        BinScheme scheme = new ConstantBinScheme(10, -5, 5);
        Histogram hist = new Histogram(scheme);
        for (int i = -5; i < 5; i++)
            hist.record(i);
        for (int i = 0; i < 10; i++)
            assertEquals(scheme.fromBin(i), hist.value(i / 10.0 + EPS), EPS);
    }

    @Test
    public void testConstantBinScheme() {
        ConstantBinScheme scheme = new ConstantBinScheme(5, -5, 5);
        assertEquals(0, scheme.toBin(-5.01), "A value below the lower bound should map to the first bin");
        assertEquals(4, scheme.toBin(5.01), "A value above the upper bound should map to the last bin");
        assertEquals(0, scheme.toBin(-5.0001), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(-5.0000), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(-4.99999), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(-3.00001), "Check boundary of bucket 0");
        assertEquals(1, scheme.toBin(-3), "Check boundary of bucket 1");
        assertEquals(1, scheme.toBin(-1.00001), "Check boundary of bucket 1");
        assertEquals(2, scheme.toBin(-1), "Check boundary of bucket 2");
        assertEquals(2, scheme.toBin(0.99999), "Check boundary of bucket 2");
        assertEquals(3, scheme.toBin(1), "Check boundary of bucket 3");
        assertEquals(3, scheme.toBin(2.99999), "Check boundary of bucket 3");
        assertEquals(4, scheme.toBin(3), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(4.9999), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.000), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.001), "Check boundary of bucket 4");
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(5), 0.001d);
        assertEquals(-5.0, scheme.fromBin(0), 0.001d);
        assertEquals(-3.0, scheme.fromBin(1), 0.001d);
        assertEquals(-1.0, scheme.fromBin(2), 0.001d);
        assertEquals(1.0, scheme.fromBin(3), 0.001d);
        assertEquals(3.0, scheme.fromBin(4), 0.001d);
        checkBinningConsistency(scheme);
    }

    @Test
    public void testConstantBinSchemeWithPositiveRange() {
        ConstantBinScheme scheme = new ConstantBinScheme(5, 0, 5);
        assertEquals(0, scheme.toBin(-1.0), "A value below the lower bound should map to the first bin");
        assertEquals(4, scheme.toBin(5.01), "A value above the upper bound should map to the last bin");
        assertEquals(0, scheme.toBin(-0.0001), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(0.0000), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(0.0001), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(0.9999), "Check boundary of bucket 0");
        assertEquals(1, scheme.toBin(1.0000), "Check boundary of bucket 1");
        assertEquals(1, scheme.toBin(1.0001), "Check boundary of bucket 1");
        assertEquals(1, scheme.toBin(1.9999), "Check boundary of bucket 1");
        assertEquals(2, scheme.toBin(2.0000), "Check boundary of bucket 2");
        assertEquals(2, scheme.toBin(2.0001), "Check boundary of bucket 2");
        assertEquals(2, scheme.toBin(2.9999), "Check boundary of bucket 2");
        assertEquals(3, scheme.toBin(3.0000), "Check boundary of bucket 3");
        assertEquals(3, scheme.toBin(3.0001), "Check boundary of bucket 3");
        assertEquals(3, scheme.toBin(3.9999), "Check boundary of bucket 3");
        assertEquals(4, scheme.toBin(4.0000), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(4.9999), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.0000), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.0001), "Check boundary of bucket 4");
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(5), 0.001d);
        assertEquals(0.0, scheme.fromBin(0), 0.001d);
        assertEquals(1.0, scheme.fromBin(1), 0.001d);
        assertEquals(2.0, scheme.fromBin(2), 0.001d);
        assertEquals(3.0, scheme.fromBin(3), 0.001d);
        assertEquals(4.0, scheme.fromBin(4), 0.001d);
        checkBinningConsistency(scheme);
    }

    @Test
    public void testLinearBinScheme() {
        LinearBinScheme scheme = new LinearBinScheme(10, 10);
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(11), 0.001d);
        assertEquals(0.0, scheme.fromBin(0), 0.001d);
        assertEquals(0.2222, scheme.fromBin(1), 0.001d);
        assertEquals(0.6666, scheme.fromBin(2), 0.001d);
        assertEquals(1.3333, scheme.fromBin(3), 0.001d);
        assertEquals(2.2222, scheme.fromBin(4), 0.001d);
        assertEquals(3.3333, scheme.fromBin(5), 0.001d);
        assertEquals(4.6667, scheme.fromBin(6), 0.001d);
        assertEquals(6.2222, scheme.fromBin(7), 0.001d);
        assertEquals(8.0000, scheme.fromBin(8), 0.001d);
        assertEquals(10.000, scheme.fromBin(9), 0.001d);
        assertEquals(0, scheme.toBin(0.0000));
        assertEquals(0, scheme.toBin(0.2221));
        assertEquals(1, scheme.toBin(0.2223));
        assertEquals(2, scheme.toBin(0.6667));
        assertEquals(3, scheme.toBin(1.3334));
        assertEquals(4, scheme.toBin(2.2223));
        assertEquals(5, scheme.toBin(3.3334));
        assertEquals(6, scheme.toBin(4.6667));
        assertEquals(7, scheme.toBin(6.2223));
        assertEquals(8, scheme.toBin(8.0000));
        assertEquals(9, scheme.toBin(10.000));
        assertEquals(9, scheme.toBin(10.001));
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(10), 0.001d);
        checkBinningConsistency(scheme);
    }

    private void checkBinningConsistency(BinScheme scheme) {
        for (int bin = 0; bin < scheme.bins(); bin++) {
            double fromBin = scheme.fromBin(bin);
            int binAgain = scheme.toBin(fromBin + EPS);
            assertEquals(bin, binAgain, "unbinning and rebinning the bin " + bin
                         + " gave a different result ("
                         + fromBin
                         + " was placed in bin "
                         + binAgain
                         + " )");
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
