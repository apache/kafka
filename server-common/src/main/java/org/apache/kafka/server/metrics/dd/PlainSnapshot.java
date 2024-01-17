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

package org.apache.kafka.server.metrics.dd;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;

import static java.lang.Math.floor;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A statistical snapshot of a {@link PlainSnapshot}.
 * This simpler version of UniformSnapshot allocates and copies less and does not sort by default.
 */
public class PlainSnapshot extends Snapshot {

    private final long[] values;
    private final double samplingRate;
    private boolean sorted = false;

    /**
     * Create a new {@link Snapshot} with the given values.
     *
     * @param values an unordered set of values in the reservoir
     */
    public PlainSnapshot(Collection<Long> values, double samplingRate) {
        this.samplingRate = samplingRate;
        final Object[] copy = values.toArray();
        this.values = new long[copy.length];
        for (int i = 0; i < copy.length; i++) {
            this.values[i] = (Long) copy[i];
        }
    }

    /**
     * Create a new {@link Snapshot} with the given values.
     *
     * @param values an unordered set of values in the reservoir that can be used by this class directly
     */
    public PlainSnapshot(long[] values, double samplingRate) {
        // specialized for UniformReservoir which already
        // passes a copy of the data. No need to copy again.
        this.values = values;
        this.samplingRate = samplingRate;

    }

    /**
     * Returns the value at the given quantile.
     *
     * @param quantile a given quantile, in {@code [0..1]}
     * @return the value in the distribution at {@code quantile}
     */
    @Override
    public double getValue(double quantile) {
        ensureSorted();
        if (quantile < 0.0 || quantile > 1.0 || Double.isNaN(quantile)) {
            throw new IllegalArgumentException(quantile + " is not in [0..1]");
        }

        if (values.length == 0) {
            return 0.0;
        }

        final double pos = quantile * (values.length + 1);
        final int index = (int) pos;

        if (index < 1) {
            return values[0];
        }

        if (index >= values.length) {
            return values[values.length - 1];
        }

        final double lower = values[index - 1];
        final double upper = values[index];
        return lower + (pos - floor(pos)) * (upper - lower);
    }

    private void ensureSorted() {
        if (!sorted) {
            Arrays.sort(this.values);
            sorted = true;
        }
    }

    /**
     * Returns the number of values in the snapshot.
     *
     * @return the number of values
     */
    @Override
    public int size() {
        return values.length;
    }

    /**
     * Returns the entire set of values in the snapshot.
     *
     * The data is not copied
     *
     * @return the entire set of values
     */
    @Override
    public long[] getValues() {
        return values;
    }

    @Override
    public double getSamplingRate() {
        return samplingRate;
    }

    /**
     * Returns the highest value in the snapshot.
     *
     * @return the highest value
     */
    @Override
    public long getMax() {
        ensureSorted();
        if (values.length == 0) {
            return 0;
        }
        return values[values.length - 1];
    }

    /**
     * Returns the lowest value in the snapshot.
     *
     * @return the lowest value
     */
    @Override
    public long getMin() {
        ensureSorted();
        if (values.length == 0) {
            return 0;
        }
        return values[0];
    }

    /**
     * Returns the arithmetic mean of the values in the snapshot.
     *
     * @return the arithmetic mean
     */
    @Override
    public double getMean() {
        ensureSorted();
        if (values.length == 0) {
            return 0;
        }

        double sum = 0;
        for (long value : values) {
            sum += value;
        }
        return sum / values.length;
    }

    /**
     * Returns the standard deviation of the values in the snapshot.
     *
     * @return the standard deviation value
     */
    @Override
    public double getStdDev() {
        ensureSorted();
        // two-pass algorithm for variance, avoids numeric overflow

        if (values.length <= 1) {
            return 0;
        }

        final double mean = getMean();
        double sum = 0;

        for (long value : values) {
            final double diff = value - mean;
            sum += diff * diff;
        }

        final double variance = sum / (values.length - 1);
        return Math.sqrt(variance);
    }

    /**
     * Writes the values of the snapshot to the given stream.
     *
     * @param output an output stream
     */
    @Override
    public void dump(OutputStream output) {
        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(output, UTF_8))) {
            for (long value : values) {
                out.printf("%d%n", value);
            }
        }
    }
}
