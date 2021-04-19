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

package org.apache.kafka.trogdor.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class HistogramTest {
    private static Histogram createHistogram(int maxValue, int... values) {
        Histogram histogram = new Histogram(maxValue);
        for (int value : values) {
            histogram.add(value);
        }
        return histogram;
    }

    @Test
    public void testHistogramAverage() {
        Histogram empty = createHistogram(1);
        assertEquals(0, (int) empty.summarize(new float[0]).average());

        Histogram histogram = createHistogram(70, 1, 2, 3, 4, 5, 6, 1);

        assertEquals(3, (int) histogram.summarize(new float[0]).average());
        histogram.add(60);
        assertEquals(10, (int) histogram.summarize(new float[0]).average());
    }

    @Test
    public void testHistogramSamples() {
        Histogram empty = createHistogram(100);
        assertEquals(0, empty.summarize(new float[0]).numSamples());
        Histogram histogram = createHistogram(100, 4, 8, 2, 4, 1, 100, 150);
        assertEquals(7, histogram.summarize(new float[0]).numSamples());
        histogram.add(60);
        assertEquals(8, histogram.summarize(new float[0]).numSamples());
    }

    @Test
    public void testHistogramPercentiles() {
        Histogram histogram = createHistogram(100, 1, 2, 3, 4, 5, 6, 80, 90);
        float[] percentiles = new float[] {0.5f, 0.90f, 0.99f, 1f};
        Histogram.Summary summary = histogram.summarize(percentiles);
        assertEquals(8, summary.numSamples());
        assertEquals(4, summary.percentiles().get(0).value());
        assertEquals(80, summary.percentiles().get(1).value());
        assertEquals(80, summary.percentiles().get(2).value());
        assertEquals(90, summary.percentiles().get(3).value());
        histogram.add(30);
        histogram.add(30);
        histogram.add(30);

        summary = histogram.summarize(new float[] {0.5f});
        assertEquals(11, summary.numSamples());
        assertEquals(5, summary.percentiles().get(0).value());

        Histogram empty = createHistogram(100);
        summary = empty.summarize(new float[] {0.5f});
        assertEquals(0, summary.percentiles().get(0).value());

        histogram = createHistogram(1000);
        histogram.add(100);
        histogram.add(200);
        summary = histogram.summarize(new float[] {0f, 0.5f, 1.0f});
        assertEquals(0, summary.percentiles().get(0).value());
        assertEquals(100, summary.percentiles().get(1).value());
        assertEquals(200, summary.percentiles().get(2).value());
    }
};

