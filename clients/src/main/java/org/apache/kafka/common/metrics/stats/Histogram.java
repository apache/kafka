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
        this.count += 1.0f;
    }

    public double value(double quantile) {
        if (count == 0.0d)
            return Double.NaN;
        float sum = 0.0f;
        float quant = (float) quantile;
        for (int i = 0; i < this.hist.length - 1; i++) {
            sum += this.hist[i];
            if (sum / count > quant)
                return binScheme.fromBin(i);
        }
        return Float.POSITIVE_INFINITY;
    }

    public float[] counts() {
        return this.hist;
    }

    public void clear() {
        for (int i = 0; i < this.hist.length; i++)
            this.hist[i] = 0.0f;
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
        b.append(this.hist[this.hist.length - 1]);
        b.append('}');
        return b.toString();
    }

    public interface BinScheme {
        public int bins();

        public int toBin(double value);

        public double fromBin(int bin);
    }

    public static class ConstantBinScheme implements BinScheme {
        private final double min;
        private final double max;
        private final int bins;
        private final double bucketWidth;

        public ConstantBinScheme(int bins, double min, double max) {
            if (bins < 2)
                throw new IllegalArgumentException("Must have at least 2 bins.");
            this.min = min;
            this.max = max;
            this.bins = bins;
            this.bucketWidth = (max - min) / (bins - 2);
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b == 0)
                return Double.NEGATIVE_INFINITY;
            else if (b == bins - 1)
                return Double.POSITIVE_INFINITY;
            else
                return min + (b - 1) * bucketWidth;
        }

        public int toBin(double x) {
            if (x < min)
                return 0;
            else if (x > max)
                return bins - 1;
            else
                return (int) ((x - min) / bucketWidth) + 1;
        }
    }

    public static class LinearBinScheme implements BinScheme {
        private final int bins;
        private final double max;
        private final double scale;

        public LinearBinScheme(int numBins, double max) {
            this.bins = numBins;
            this.max = max;
            this.scale = max / (numBins * (numBins - 1) / 2);
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b == this.bins - 1) {
                return Float.POSITIVE_INFINITY;
            } else {
                double unscaled = (b * (b + 1.0)) / 2.0;
                return unscaled * this.scale;
            }
        }

        public int toBin(double x) {
            if (x < 0.0d) {
                throw new IllegalArgumentException("Values less than 0.0 not accepted.");
            } else if (x > this.max) {
                return this.bins - 1;
            } else {
                double scaled = x / this.scale;
                return (int) (-0.5 + Math.sqrt(2.0 * scaled + 0.25));
            }
        }
    }

}
