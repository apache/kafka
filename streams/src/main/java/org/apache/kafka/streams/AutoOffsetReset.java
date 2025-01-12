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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.StrategyType;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Duration;
import java.util.Optional;

/**
 * Sets the {@code auto.offset.reset} configuration when
 * {@link Topology#addSource(AutoOffsetReset, String, String...) adding a source processor}
 * or when creating {@link KStream} or {@link KTable} via {@link StreamsBuilder}.
 */
public class AutoOffsetReset {
    protected final StrategyType offsetResetStrategy;
    protected final Optional<Duration> duration;

    private AutoOffsetReset(final StrategyType offsetResetStrategy, final Optional<Duration> duration) {
        this.offsetResetStrategy = offsetResetStrategy;
        this.duration = duration;
    }

    protected AutoOffsetReset(final AutoOffsetReset autoOffsetReset) {
        this(autoOffsetReset.offsetResetStrategy, autoOffsetReset.duration);
    }

    /**
     * Creates an {@code AutoOffsetReset} instance representing "none".
     *
     * @return An {@link AutoOffsetReset} instance for no reset.
     */
    public static AutoOffsetReset none() {
        return new AutoOffsetReset(StrategyType.NONE, Optional.empty());
    }

    /**
     * Creates an {@code AutoOffsetReset} instance representing "earliest".
     *
     * @return An {@link AutoOffsetReset} instance for the "earliest" offset.
     */
    public static AutoOffsetReset earliest() {
        return new AutoOffsetReset(StrategyType.EARLIEST, Optional.empty());
    }

    /**
     * Creates an {@code AutoOffsetReset} instance representing "latest".
     * 
     * @return An {@code AutoOffsetReset} instance for the "latest" offset.
     */
    public static AutoOffsetReset latest() {
        return new AutoOffsetReset(StrategyType.LATEST, Optional.empty());
    }

    /**
     * Creates an {@code AutoOffsetReset} instance for the specified reset duration.
     * 
     * @param duration The duration to use for the offset reset; must be non-negative.
     * @return An {@code AutoOffsetReset} instance with the specified duration.
     * @throws IllegalArgumentException If the duration is negative.
     */
    public static AutoOffsetReset byDuration(final Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Duration cannot be negative");
        }
        return new AutoOffsetReset(StrategyType.BY_DURATION, Optional.of(duration));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AutoOffsetReset that = (AutoOffsetReset) o;
        return offsetResetStrategy == that.offsetResetStrategy && duration.equals(that.duration);
    }

    @Override
    public int hashCode() {
        int result = offsetResetStrategy.hashCode();
        result = 31 * result + duration.hashCode();
        return result;
    }
}
