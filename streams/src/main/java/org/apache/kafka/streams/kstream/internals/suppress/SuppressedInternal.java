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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.internals.suppress.TimeDefinitions.TimeDefinition;

import java.time.Duration;
import java.util.Objects;

public class SuppressedInternal<K> implements Suppressed<K>, NamedSuppressed<K> {
    private static final Duration DEFAULT_SUPPRESSION_TIME = Duration.ofMillis(Long.MAX_VALUE);
    private static final StrictBufferConfigImpl DEFAULT_BUFFER_CONFIG = (StrictBufferConfigImpl) BufferConfig.unbounded();

    private final String name;
    private final BufferConfigInternal bufferConfig;
    private final Duration timeToWaitForMoreEvents;
    private final TimeDefinition<K> timeDefinition;
    private final boolean safeToDropTombstones;

    /**
     * @param safeToDropTombstones Note: it's *only* safe to drop tombstones for windowed KTables in "final results" mode.
     *                             In that case, we have a priori knowledge that we have never before emitted any
     *                             results for a given key, and therefore the tombstone is unnecessary (albeit
     *                             idempotent and correct). We decided that the unnecessary tombstones would not be
     *                             desirable in the output stream, though, hence the ability to drop them.
     *
     *                             A alternative is to remember whether a result has previously been emitted
     *                             for a key and drop tombstones in that case, but it would be a little complicated to
     *                             figure out when to forget the fact that we have emitted some result (currently, the
     *                             buffer immediately forgets all about a key when we emit, which helps to keep it
     *                             compact).
     */
    public SuppressedInternal(final String name,
                              final Duration suppressionTime,
                              final BufferConfig bufferConfig,
                              final TimeDefinition<K> timeDefinition,
                              final boolean safeToDropTombstones) {
        this.name = name;
        this.timeToWaitForMoreEvents = suppressionTime == null ? DEFAULT_SUPPRESSION_TIME : suppressionTime;
        this.timeDefinition = timeDefinition == null ? TimeDefinitions.RecordTimeDefintion.instance() : timeDefinition;
        this.bufferConfig = bufferConfig == null ? DEFAULT_BUFFER_CONFIG : (BufferConfigInternal) bufferConfig;
        this.safeToDropTombstones = safeToDropTombstones;
    }

    @Override
    public Suppressed<K> withName(final String name) {
        return new SuppressedInternal<>(name, timeToWaitForMoreEvents, bufferConfig, timeDefinition, safeToDropTombstones);
    }

    @Override
    public String name() {
        return name;
    }

    BufferConfigInternal bufferConfig() {
        return bufferConfig;
    }

    TimeDefinition<K> timeDefinition() {
        return timeDefinition;
    }

    Duration timeToWaitForMoreEvents() {
        return timeToWaitForMoreEvents == null ? Duration.ZERO : timeToWaitForMoreEvents;
    }

    boolean safeToDropTombstones() {
        return safeToDropTombstones;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SuppressedInternal<?> that = (SuppressedInternal<?>) o;
        return safeToDropTombstones == that.safeToDropTombstones &&
            Objects.equals(name, that.name) &&
            Objects.equals(bufferConfig, that.bufferConfig) &&
            Objects.equals(timeToWaitForMoreEvents, that.timeToWaitForMoreEvents) &&
            Objects.equals(timeDefinition, that.timeDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, bufferConfig, timeToWaitForMoreEvents, timeDefinition, safeToDropTombstones);
    }

    @Override
    public String toString() {
        return "SuppressedInternal{" +
                "name='" + name + '\'' +
                ", bufferConfig=" + bufferConfig +
                ", timeToWaitForMoreEvents=" + timeToWaitForMoreEvents +
                ", timeDefinition=" + timeDefinition +
                ", safeToDropTombstones=" + safeToDropTombstones +
                '}';
    }
}
