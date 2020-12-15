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

import java.util.Arrays;

public class Histogram {

    private final BinScheme binScheme;
    private final float[] hist;
    private double count;

    public Histogram(BinScheme binScheme) {
        this.hist = new float[binScheme.bins()];
        this.count = 0.0f;
        this.binScheme = binScheme;
    }

    public void record(double value) {
        this.hist[binScheme.toBin(value)] += 1.0f;
        this.count += 1.0d;
    }

    public double value(double quantile) {
        if (count == 0.0d)
            return Double.NaN;
        if (quantile > 1.00d)
            return Float.POSITIVE_INFINITY;
        if (quantile < 0.00d)
            return Float.NEGATIVE_INFINITY;
        float sum = 0.0f;
        float quant = (float) quantile;
        for (int i = 0; i < this.hist.length - 1; i++) {
            sum += this.hist[i];
            if (sum / count > quant)
                return binScheme.fromBin(i);
        }
        return binScheme.fromBin(this.hist.length - 1);
    }

    public float[] counts() {
        return this.hist;
    }

    public void clear() {
        Arrays.fill(this.hist, 0.0f);
        this.count = 0;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("{");
        for (int i = 0; i < this.hist.length - 1; i++) {
            b.append(String.format("%.10f", binScheme.fromBin(i)));
            b.append(':');
            b.append(String.format("%.0f", this.hist[i]));
            b.append(',');
        }
        b.append(Float.POSITIVE_INFINITY);
        b.append(':');
        b.append(String.format("%.0f", this.hist[this.hist.length - 1]));
        b.append('}');
        return b.toString();
    }

    /**
     * An algorithm for determining the bin in which a value is to be placed as well as calculating the upper end
     * of each bin.
     */
    public interface BinScheme {

        /**
         * Get the number of bins.
         *
         * @return the number of bins
         */
        int bins();

        /**
         * Determine the 0-based bin number in which the supplied value should be placed.
         *
         * @param value the value
         * @return the 0-based index of the bin
         */
        int toBin(double value);

        /**
         * Determine the value at the upper range of the specified bin.
         *
         * @param bin the 0-based bin number
         * @return the value at the upper end of the bin; or {@link Float#NEGATIVE_INFINITY negative infinity}
         * if the bin number is negative or {@link Float#POSITIVE_INFINITY positive infinity} if the 0-based
         * bin number is greater than or equal to the {@link #bins() number of bins}.
         */
        double fromBin(int bin);
    }

    /**
     * A scheme for calculating the bins where the width of each bin is a constant determined by the range of values
     * and the number of bins.
     */
    public static class ConstantBinScheme implements BinScheme {
        private static final int MIN_BIN_NUMBER = 0;
        private final double min;
        private final double max;
        private final int bins;
        private final double bucketWidth;
        private final int maxBinNumber;

        /**
         * Create a bin scheme with the specified number of bins that all have the same width.
         *
         * @param bins the number of bins; must be at least 2
         * @param min the minimum value to be counted in the bins
         * @param max the maximum value to be counted in the bins
         */
        public ConstantBinScheme(int bins, double min, double max) {
            if (bins < 2)
                throw new IllegalArgumentException("Must have at least 2 bins.");
            this.min = min;
            this.max = max;
            this.bins = bins;
            this.bucketWidth = (max - min) / bins;
            this.maxBinNumber = bins - 1;
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b < MIN_BIN_NUMBER) {
                return Float.NEGATIVE_INFINITY;
            }
            if (b > maxBinNumber) {
                return Float.POSITIVE_INFINITY;
            }
            return min + b * bucketWidth;
        }

        public int toBin(double x) {
            int binNumber = (int) ((x - min) / bucketWidth);
            if (binNumber < MIN_BIN_NUMBER) {
                return MIN_BIN_NUMBER;
            }
            return Math.min(binNumber, maxBinNumber);
        }
    }

    /**
     * A scheme for calculating the bins where the width of each bin is one more than the previous bin, and therefore
     * the bin widths are increasing at a linear rate. However, the bin widths are scaled such that the specified range
     * of values will all fit within the bins (e.g., the upper range of the last bin is equal to the maximum value).
     */
    public static class LinearBinScheme implements BinScheme {
        private final int bins;
        private final double max;
        private final double scale;

        /**
         * Create a linear bin scheme with the specified number of bins and the maximum value to be counted in the bins.
         *
         * @param numBins the number of bins; must be at least 2
         * @param max the maximum value to be counted in the bins
         */
        public LinearBinScheme(int numBins, double max) {
            if (numBins < 2)
                throw new IllegalArgumentException("Must have at least 2 bins.");
            this.bins = numBins;
            this.max = max;
            double denom = numBins * (numBins - 1.0) / 2.0;
            this.scale = max / denom;
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b > this.bins - 1) {
                return Float.POSITIVE_INFINITY;
            } else if (b < 0.0000d) {
                return Float.NEGATIVE_INFINITY;
            } else {
                return this.scale * (b * (b + 1.0)) / 2.0;
            }
        }

        public int toBin(double x) {
            if (x < 0.0d) {
                throw new IllegalArgumentException("Values less than 0.0 not accepted.");
            } else if (x > this.max) {
                return this.bins - 1;
            } else {
                return (int) (-0.5 + 0.5 * Math.sqrt(1.0 + 8.0 * x / this.scale));
            }
        }
    }
}