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
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * These are the metrics which are managed by the MetadataLoader class.
 */
public final class MetadataLoaderMetrics implements AutoCloseable {
    private final static MetricName CURRENT_METADATA_VERSION = getMetricName(
        "MetadataLoader", "CurrentMetadataVersion");
    private final static MetricName HANDLE_LOAD_SNAPSHOT_COUNT = getMetricName(
        "MetadataLoader", "HandleLoadSnapshotCount");
    public final static MetricName CURRENT_CONTROLLER_ID = getMetricName(
        "MetadataLoader", "CurrentControllerId");

    private final Optional<MetricsRegistry> registry;
    private final AtomicReference<MetadataVersion> currentMetadataVersion =
            new AtomicReference<>(MetadataVersion.MINIMUM_KRAFT_VERSION);
    private final AtomicInteger currentControllerId = new AtomicInteger(-1);
    private final AtomicLong handleLoadSnapshotCount = new AtomicLong(0);
    private final Consumer<Long> batchProcessingTimeNsUpdater;
    private final Consumer<Integer> batchSizesUpdater;
    private final AtomicReference<MetadataProvenance> lastAppliedProvenance;

    /**
     * Create a new LoaderMetrics object.
     *
     * @param registry                      The metrics registry, or Optional.empty if this is a
     *                                      test and we don't have one.
     * @param batchProcessingTimeNsUpdater  Updates the batch processing time histogram.
     * @param batchSizesUpdater             Updates the batch sizes histogram.
     */
    public MetadataLoaderMetrics(
        Optional<MetricsRegistry> registry,
        Consumer<Long> batchProcessingTimeNsUpdater,
        Consumer<Integer> batchSizesUpdater,
        AtomicReference<MetadataProvenance> lastAppliedProvenance
    ) {
        this.registry = registry;
        this.batchProcessingTimeNsUpdater = batchProcessingTimeNsUpdater;
        this.batchSizesUpdater = batchSizesUpdater;
        this.lastAppliedProvenance = lastAppliedProvenance;
        registry.ifPresent(r -> r.newGauge(CURRENT_METADATA_VERSION, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return Integer.valueOf(currentMetadataVersion().featureLevel());
            }
        }));
        registry.ifPresent(r -> r.newGauge(CURRENT_CONTROLLER_ID, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return currentControllerId();
            }
        }));
        registry.ifPresent(r -> r.newGauge(HANDLE_LOAD_SNAPSHOT_COUNT, new Gauge<Long>() {
            @Override
            public Long value() {
                return handleLoadSnapshotCount();
            }
        }));
    }

    /**
     * Update the batch processing time histogram.
     */
    public void updateBatchProcessingTimeNs(long elapsedNs) {
        batchProcessingTimeNsUpdater.accept(elapsedNs);
    }

    /**
     * Update the batch size histogram.
     */
    public void updateBatchSize(int size) {
        batchSizesUpdater.accept(size);
    }

    /**
     * Set the provenance of the last image which has been processed by all publishers.
     */
    public void updateLastAppliedImageProvenance(MetadataProvenance lastAppliedProvenance) {
        this.lastAppliedProvenance.set(lastAppliedProvenance);
    }

    /**
     * Retrieve the last offset which has been processed by all publishers.
     */
    public long lastAppliedOffset() {
        return this.lastAppliedProvenance.get().lastContainedOffset();
    }

    public void setCurrentMetadataVersion(MetadataVersion metadataVersion) {
        this.currentMetadataVersion.set(metadataVersion);
    }

    public MetadataVersion currentMetadataVersion() {
        return this.currentMetadataVersion.get();
    }

    public int currentControllerId() {
        return this.currentControllerId.get();
    }

    public void setCurrentControllerId(int newCurrentControllerId) {
        this.currentControllerId.set(newCurrentControllerId);
    }

    public long incrementHandleLoadSnapshotCount() {
        return this.handleLoadSnapshotCount.incrementAndGet();
    }

    public long handleLoadSnapshotCount() {
        return this.handleLoadSnapshotCount.get();
    }

    @Override
    public void close() {
        registry.ifPresent(r -> Arrays.asList(
            CURRENT_METADATA_VERSION,
            CURRENT_CONTROLLER_ID,
            HANDLE_LOAD_SNAPSHOT_COUNT
        ).forEach(r::removeMetric));
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.server", type, name);
    }
}
