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

package org.apache.kafka.image.loader.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * These are the metrics which are managed by the SnapshotEmitter class.
 */
public final class SnapshotEmitterMetrics implements AutoCloseable {
    private final static MetricName LATEST_SNAPSHOT_GENERATED_BYTES = getMetricName(
        "SnapshotEmitter", "LatestSnapshotGeneratedBytes");
    private final static MetricName LATEST_SNAPSHOT_GENERATED_AGE_MS = getMetricName(
        "SnapshotEmitter", "LatestSnapshotGeneratedAgeMs");

    private final Optional<MetricsRegistry> registry;
    private final Time time;
    private final AtomicLong latestSnapshotGeneratedBytes;
    private final AtomicLong latestSnapshotGeneratedTimeMs;

    /**
     * Create a new LoaderMetrics object.
     *
     * @param registry  The metrics registry, or Optional.empty if this is a test and we don't have one.
     */
    public SnapshotEmitterMetrics(
        Optional<MetricsRegistry> registry,
        Time time,
        long initialLatestSnapshotGeneratedBytes
    ) {
        this.registry = registry;
        this.time = time;
        this.latestSnapshotGeneratedBytes = new AtomicLong(initialLatestSnapshotGeneratedBytes);
        this.latestSnapshotGeneratedTimeMs = new AtomicLong(monoTimeInMs());
        registry.ifPresent(r -> r.newGauge(LATEST_SNAPSHOT_GENERATED_BYTES, new Gauge<Long>() {
            @Override
            public Long value() {
                return latestSnapshotGeneratedBytes();
            }
        }));
        registry.ifPresent(r -> r.newGauge(LATEST_SNAPSHOT_GENERATED_AGE_MS, new Gauge<Long>() {
            @Override
            public Long value() {
                return latestSnapshotGeneratedAgeMs();
            }
        }));
    }

    long monoTimeInMs() {
        return TimeUnit.NANOSECONDS.toMillis(time.nanoseconds());
    }

    public void setLatestSnapshotGeneratedBytes(long value) {
        this.latestSnapshotGeneratedBytes.set(value);
    }

    public long latestSnapshotGeneratedBytes() {
        return this.latestSnapshotGeneratedBytes.get();
    }

    public void setLatestSnapshotGeneratedTimeMs(long timeMs) {
        this.latestSnapshotGeneratedTimeMs.set(timeMs);
    }

    public long latestSnapshotGeneratedTimeMs() {
        return this.latestSnapshotGeneratedTimeMs.get();
    }

    public long latestSnapshotGeneratedAgeMs() {
        return time.milliseconds() - this.latestSnapshotGeneratedTimeMs.get();
    }

    @Override
    public void close() {
        registry.ifPresent(r -> Arrays.asList(
            LATEST_SNAPSHOT_GENERATED_BYTES,
            LATEST_SNAPSHOT_GENERATED_AGE_MS
        ).forEach(r::removeMetric));
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.server", type, name);
    }
}
