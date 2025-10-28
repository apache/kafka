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

import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.metrics.TimeRatio;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * These are the metrics which are managed by the MetadataLoader class.
 */
public final class MetadataLoaderMetrics implements AutoCloseable {
    private static final MetricName CURRENT_METADATA_VERSION = getMetricName(
        "MetadataLoader", "CurrentMetadataVersion");
    private static final MetricName HANDLE_LOAD_SNAPSHOT_COUNT = getMetricName(
        "MetadataLoader", "HandleLoadSnapshotCount");
    private static final MetricName CURRENT_CONTROLLER_ID = getMetricName(
        "MetadataLoader", "CurrentControllerId");
    private static final MetricName AVERAGE_IDLE_RATIO = getMetricName(
        "MetadataLoader", "AvgIdleRatio");
    private static final String FINALIZED_LEVEL_METRIC_NAME = "FinalizedLevel";
    private static final String FEATURE_NAME_TAG = "featureName";

    private final Optional<MetricsRegistry> registry;
    private final AtomicReference<MetadataVersion> currentMetadataVersion =
            new AtomicReference<>(MetadataVersion.MINIMUM_VERSION);
    private final Map<String, Short> finalizedFeatureLevels = new ConcurrentHashMap<>();
    private final AtomicInteger currentControllerId = new AtomicInteger(-1);
    private final AtomicLong handleLoadSnapshotCount = new AtomicLong(0);
    private final Consumer<Long> batchProcessingTimeNsUpdater;
    private final Consumer<Integer> batchSizesUpdater;
    private final AtomicReference<MetadataProvenance> lastAppliedProvenance;
    private final TimeRatio avgIdleTimeRatio;

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
        this.avgIdleTimeRatio = new TimeRatio(1);
        registry.ifPresent(r -> r.newGauge(CURRENT_METADATA_VERSION, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return (int) currentMetadataVersion().featureLevel();
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
        registry.ifPresent(r -> r.newGauge(AVERAGE_IDLE_RATIO, new Gauge<Double>() {
            @Override
            public Double value() {
                synchronized (avgIdleTimeRatio) {
                    return avgIdleTimeRatio.measure();
                }
            }
        }));
    }

    public void updateIdleTime(long idleDurationMs, long currentTimeMs) {
        synchronized (avgIdleTimeRatio) {
            avgIdleTimeRatio.record((double) idleDurationMs, currentTimeMs);
        }
    }

    private void addFinalizedFeatureLevelMetric(String featureName) {
        registry.ifPresent(r -> r.newGauge(
            getFeatureNameTagMetricName(
                "MetadataLoader",
                FINALIZED_LEVEL_METRIC_NAME,
                featureName
            ),
            new Gauge<Short>() {
                @Override
                public Short value() {
                    return finalizedFeatureLevel(featureName);
                }
            }
        ));
    }

    private void removeFinalizedFeatureLevelMetric(String featureName) {
        registry.ifPresent(r -> r.removeMetric(
            getFeatureNameTagMetricName(
                "MetadataLoader",
                FINALIZED_LEVEL_METRIC_NAME,
                featureName
            )
        ));
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

    /**
     * Remove the FinalizedLevel metric for features who are no longer part of the
     * current features image.
     * Note that metadata.version and kraft.version are not included in
     * the features image, so they are not removed.
     * @param newFinalizedLevels The new finalized feature levels from the features image
     */
    public void maybeRemoveFinalizedFeatureLevelMetrics(Map<String, Short> newFinalizedLevels) {
        final var iter = finalizedFeatureLevels.keySet().iterator();
        while (iter.hasNext()) {
            final var featureName = iter.next();
            if (newFinalizedLevels.containsKey(featureName) ||
                featureName.equals(MetadataVersion.FEATURE_NAME) ||
                featureName.equals(KRaftVersion.FEATURE_NAME)) {
                continue;
            }
            removeFinalizedFeatureLevelMetric(featureName);
            iter.remove();
        }
    }

    /**
     * Record the finalized feature level and ensure the metric is registered.
     * 
     * @param featureName The name of the feature
     * @param featureLevel The finalized level for the feature
     */
    public void recordFinalizedFeatureLevel(String featureName, short featureLevel) {
        final var metricNotRegistered = finalizedFeatureLevels.put(featureName, featureLevel) == null;
        if (metricNotRegistered) addFinalizedFeatureLevelMetric(featureName);
    }

    /**
     * Get the finalized feature level for a feature.
     * 
     * @param featureName The name of the feature
     * @return The finalized level for the feature
     */
    public short finalizedFeatureLevel(String featureName) {
        return finalizedFeatureLevels.get(featureName);
    }

    @Override
    public void close() {
        registry.ifPresent(r -> List.of(
            CURRENT_METADATA_VERSION,
            CURRENT_CONTROLLER_ID,
            HANDLE_LOAD_SNAPSHOT_COUNT,
            AVERAGE_IDLE_RATIO
        ).forEach(r::removeMetric));
        for (var featureName : finalizedFeatureLevels.keySet()) {
            removeFinalizedFeatureLevelMetric(featureName);
        }
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.server", type, name);
    }

    private static MetricName getFeatureNameTagMetricName(String type, String name, String featureName) {
        LinkedHashMap<String, String> featureNameTag = new LinkedHashMap<>();
        featureNameTag.put(FEATURE_NAME_TAG, sanitizeFeatureName(featureName));
        return KafkaYammerMetrics.getMetricName("kafka.server", type, name, featureNameTag);
    }

    /**
     * Sanitize the feature name to be used as a tag in metrics by
     * converting from dot notation to camel case.
     * The conversion is done to be consistent with other Yammer metrics.
     *
     * @param featureName The feature name in dot notation.
     * @return The sanitized feature name in camel case.
     */
    private static String sanitizeFeatureName(String featureName) {
        final var words = featureName.split("\\.");
        final var builder = new StringBuilder(words[0]);
        for (int i = 1; i < words.length; i++) {
            final var word = words[i];
            builder.append(Character.toUpperCase(word.charAt(0)))
                   .append(word.substring(1).toLowerCase(Locale.ROOT));
        }
        return builder.toString();
    }
}
