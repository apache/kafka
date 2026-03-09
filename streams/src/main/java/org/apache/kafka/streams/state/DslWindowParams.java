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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.DslStoreFormat;
import org.apache.kafka.streams.kstream.EmitStrategy;

import java.time.Duration;
import java.util.Objects;

/**
 * {@code DslWindowParams} is a wrapper class for all parameters that function
 * as inputs to {@link DslStoreSuppliers#windowStore(DslWindowParams)}.
 */
public class DslWindowParams {

    private final String name;
    private final Duration retentionPeriod;
    private final Duration windowSize;
    private final boolean retainDuplicates;
    private final EmitStrategy emitStrategy;
    private final boolean isSlidingWindow;
    private final DslStoreFormat dslStoreFormat;

    /**
     * @deprecated Since 4.3. Use {@link #DslWindowParams(String, Duration, Duration, boolean, EmitStrategy, boolean, DslStoreFormat)} Params(String, DslStoreFormat)} instead.
     * @param name             name of the store (cannot be {@code null})
     * @param retentionPeriod  length of time to retain data in the store (cannot be negative)
     *                         (note that the retention period must be at least long enough to contain the
     *                         windowed data's entire life cycle, from window-start through window-end,
     *                         and for the entire grace period)
     * @param windowSize       size of the windows (cannot be negative)
     * @param retainDuplicates whether to retain duplicates. Turning this on will automatically disable
     *                         caching and means that null values will be ignored.
     * @param emitStrategy     defines how to emit results
     * @param isSlidingWindow  whether the requested store is a sliding window
     * @param isTimestamped    whether the requested store should be timestamped (see {@link TimestampedWindowStore}
     */
    @Deprecated
    public DslWindowParams(
            final String name,
            final Duration retentionPeriod,
            final Duration windowSize,
            final boolean retainDuplicates,
            final EmitStrategy emitStrategy,
            final boolean isSlidingWindow,
            final boolean isTimestamped
    ) {
        this.name = Objects.requireNonNull(name);
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.emitStrategy = emitStrategy;
        this.isSlidingWindow = isSlidingWindow;
        // If isTimestamped is false and the user is still calling the old deprecated constructor, we should assume they mean plain.
        this.dslStoreFormat = isTimestamped ? DslStoreFormat.TIMESTAMPED : DslStoreFormat.PLAIN;
    }

    /**
     * @param name             name of the store (cannot be {@code null})
     * @param retentionPeriod  length of time to retain data in the store (cannot be negative)
     *                         (note that the retention period must be at least long enough to contain the
     *                         windowed data's entire life cycle, from window-start through window-end,
     *                         and for the entire grace period)
     * @param windowSize       size of the windows (cannot be negative)
     * @param retainDuplicates whether to retain duplicates. Turning this on will automatically disable
     *                         caching and means that null values will be ignored.
     * @param emitStrategy     defines how to emit results
     * @param isSlidingWindow  whether the requested store is a sliding window
     * @param dslStoreFormat   indicate the dsl store format (see {@link DslStoreFormat}
     */
    public DslWindowParams(
            final String name,
            final Duration retentionPeriod,
            final Duration windowSize,
            final boolean retainDuplicates,
            final EmitStrategy emitStrategy,
            final boolean isSlidingWindow,
            final DslStoreFormat dslStoreFormat
    ) {
        this.name = Objects.requireNonNull(name);
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.emitStrategy = emitStrategy;
        this.isSlidingWindow = isSlidingWindow;
        this.dslStoreFormat = dslStoreFormat;
    }

    public String name() {
        return name;
    }

    public Duration retentionPeriod() {
        return retentionPeriod;
    }

    public Duration windowSize() {
        return windowSize;
    }

    public boolean retainDuplicates() {
        return retainDuplicates;
    }

    public EmitStrategy emitStrategy() {
        return emitStrategy;
    }

    public boolean isSlidingWindow() {
        return isSlidingWindow;
    }

    /**
     * @deprecated Since 4.3. Use {@link #dslStoreFormat()} instead to check the store format.
     * @return {@code true} if the store format is {@link DslStoreFormat#TIMESTAMPED}, {@code false} otherwise
     */
    @Deprecated
    public boolean isTimestamped() {
        return dslStoreFormat == DslStoreFormat.TIMESTAMPED;
    }

    /**
     * Returns the store format for this window store.
     *`
     * @return the {@link DslStoreFormat} specifying whether to use plain, timestamped, or headers-aware stores
     */
    public DslStoreFormat dslStoreFormat() {
        return dslStoreFormat;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DslWindowParams that = (DslWindowParams) o;
        return retainDuplicates == that.retainDuplicates
                && Objects.equals(name, that.name)
                && Objects.equals(retentionPeriod, that.retentionPeriod)
                && Objects.equals(windowSize, that.windowSize)
                && Objects.equals(emitStrategy, that.emitStrategy)
                && Objects.equals(isSlidingWindow, that.isSlidingWindow)
                && Objects.equals(dslStoreFormat, that.dslStoreFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                retentionPeriod,
                windowSize,
                retainDuplicates,
                emitStrategy,
                isSlidingWindow,
                dslStoreFormat
        );
    }

    @Override
    public String toString() {
        return "DslWindowParams{" +
                "name='" + name + '\'' +
                ", retentionPeriod=" + retentionPeriod +
                ", windowSize=" + windowSize +
                ", retainDuplicates=" + retainDuplicates +
                ", emitStrategy=" + emitStrategy +
                ", isSlidingWindow=" + isSlidingWindow +
                ", dslStoreFormat=" + dslStoreFormat +
                '}';
    }
}