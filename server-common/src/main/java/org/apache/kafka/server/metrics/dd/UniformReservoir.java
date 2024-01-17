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


import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;


/**
 * A random sampling reservoir of a stream of {@code long}s. Uses Vitter's Algorithm R to produce a
 * statistically representative sample.
 *
 * @see <a href="http://www.cs.umd.edu/~samir/498/vitter.pdf">Random Sampling with a Reservoir</a>
 * <p>
 * Copied from https://github.com/dropwizard/metrics
 */
public class UniformReservoir implements Reservoir {
    private static final int DEFAULT_SIZE = 1028;
    private final AtomicLong count = new AtomicLong();

    private final AtomicLongArray values;

    /**
     * Creates a new {@link UniformReservoir} of 1028 elements, which offers a 99.9% confidence level
     * with a 5% margin of error assuming a normal distribution.
     */
    public UniformReservoir() {
        this(DEFAULT_SIZE);
    }

    /**
     * Creates a new {@link UniformReservoir}.
     *
     * @param size the number of samples to keep in the sampling reservoir
     */
    public UniformReservoir(int size) {
        this.values = new AtomicLongArray(size);
        this.clear();
    }

    @Override
    public int size() {
        final long c = count.get();
        if (c > values.length()) {
            return values.length();
        }
        return (int) c;
    }

    @Override
    public void update(long value) {
        final long c = count.incrementAndGet();
        if (c <= values.length()) {
            values.set((int) c - 1, value);
        } else {
            final long r = ThreadLocalRandom.current().nextLong(c);
            if (r < values.length()) {
                values.set((int) r, value);
            }
        }
    }

    public void update(double value) {
        if (value >= Long.MAX_VALUE)
            return;
        long val = (long) value; // truncation should be okay here
        update(val);
    }

    @Override
    public void clear() {
        for (int i = 0; i < values.length(); i++) {
            values.set(i, 0);
        }
        count.set(0);
    }


    @Override
    public Snapshot getSnapshot() {
        final long c = count.get();
        // don't call size() to avoid race conditions
        final int size = (int) Math.min(values.length(), c);
        double samplingRate;
        if (c == 0 || c <= size) {
            samplingRate = 1.0;
        } else {
            samplingRate = Math.min((double) size / c, 1.0);
        }
        long[] copy = new long[size];
        for (int i = 0; i < size; i++) {
            copy[i] = values.get(i);
        }
        return new PlainSnapshot(copy, samplingRate);
    }
}

