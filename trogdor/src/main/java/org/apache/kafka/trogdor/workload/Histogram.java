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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A histogram that can easily find the average, median etc of a large number of samples in a
 * restricted domain.
 */
public class Histogram {
    private final int[] counts;

    public Histogram(int maxValue) {
        this.counts = new int[maxValue + 1];
    }

    /**
     * Add a new value to the histogram.
     *
     * Note that the value will be clipped to the maximum value available in the Histogram instance.
     * So if the histogram has 100 buckets, inserting 101 will increment the last bucket.
     */
    public void add(int value) {
        if (value < 0) {
            throw new RuntimeException("invalid negative value.");
        }
        if (value >= counts.length) {
            value = counts.length - 1;
        }
        synchronized (this) {
            int curCount = counts[value];
            if (curCount < Integer.MAX_VALUE) {
                counts[value] = counts[value] + 1;
            }
        }
    }

    /**
     * Add a new value to the histogram.
     *
     * Note that the value will be clipped to the maximum value available in the Histogram instance.
     * This method is provided for convenience, but handles the same numeric range as the method which
     * takes an int.
     */
    public void add(long value) {
        if (value > Integer.MAX_VALUE) {
            add(Integer.MAX_VALUE);
        } else if (value < Integer.MIN_VALUE) {
            add(Integer.MIN_VALUE);
        } else {
            add((int) value);
        }
    }

    public static class Summary {
        /**
         * The total number of samples.
         */
        private final long numSamples;

        /**
         * The average of all samples.
         */
        private final float average;

        /**
         * Percentile information.
         *
         * percentile(fraction=0.99) will have a value which is greater than or equal to 99%
         * of the samples.  percentile(fraction=0.5) is the median sample.  And so forth.
         */
        private final List<PercentileSummary> percentiles;

        Summary(long numSamples, float average, List<PercentileSummary> percentiles) {
            this.numSamples = numSamples;
            this.average = average;
            this.percentiles = percentiles;
        }

        public long numSamples() {
            return numSamples;
        }

        public float average() {
            return average;
        }

        public List<PercentileSummary> percentiles() {
            return percentiles;
        }
    }

    /**
     * Information about a percentile.
     */
    public static class PercentileSummary {
        /**
         * The fraction of samples which are less than or equal to the value of this percentile.
         */
        private final float fraction;

        /**
         * The value of this percentile.
         */
        private final int value;

        PercentileSummary(float fraction, int value) {
            this.fraction = fraction;
            this.value = value;
        }

        public float fraction() {
            return fraction;
        }

        public int value() {
            return value;
        }
    }

    public Summary summarize() {
        return summarize(new float[0]);
    }

    public Summary summarize(float[] percentiles) {
        int[] countsCopy = new int[counts.length];
        synchronized (this) {
            System.arraycopy(counts, 0, countsCopy, 0, counts.length);
        }
        // Verify that the percentiles array is sorted and positive.
        float prev = 0f;
        for (float percentile : percentiles) {
            if (percentile < prev) {
                throw new RuntimeException("Invalid percentiles fraction array.  Bad element " +
                        percentile + ".  The array must be sorted and non-negative.");
            }
            if (percentile > 1.0f) {
                throw new RuntimeException("Invalid percentiles fraction array.  Bad element " +
                        percentile + ".  Elements must be less than or equal to 1.");
            }
        }
        // Find out how many total samples we have, and what the average is.
        long numSamples = 0;
        float total = 0f;
        for (int i = 0; i < countsCopy.length; i++) {
            long count = countsCopy[i];
            numSamples = numSamples + count;
            total = total + (i * count);
        }
        float average = (numSamples == 0) ? 0.0f : (total / numSamples);

        List<PercentileSummary> percentileSummaries =
            summarizePercentiles(countsCopy, percentiles, numSamples);
        return new Summary(numSamples, average, percentileSummaries);
    }

    private List<PercentileSummary> summarizePercentiles(int[] countsCopy, float[] percentiles,
                                                         long numSamples) {
        if (percentiles.length == 0) {
            return Collections.emptyList();
        }
        List<PercentileSummary> summaries = new ArrayList<>(percentiles.length);
        int i = 0, j = 0;
        long seen = 0, next = (long) (numSamples * percentiles[0]);
        while (true) {
            if (i == countsCopy.length - 1) {
                for (; j < percentiles.length; j++) {
                    summaries.add(new PercentileSummary(percentiles[j], i));
                }
                return summaries;
            }
            seen += countsCopy[i];
            while (seen >= next) {
                summaries.add(new PercentileSummary(percentiles[j], i));
                j++;
                if (j == percentiles.length) {
                    return summaries;
                }
                next = (long) (numSamples * percentiles[j]);
            }
            i++;
        }
    }
}
