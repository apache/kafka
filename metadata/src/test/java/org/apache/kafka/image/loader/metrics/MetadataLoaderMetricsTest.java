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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.metrics.ControllerMetricsTestUtils;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.TransactionVersion;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataLoaderMetricsTest {
    private static class FakeMetadataLoaderMetrics implements AutoCloseable {
        final AtomicLong batchProcessingTimeNs = new AtomicLong(0L);
        final AtomicInteger batchSize = new AtomicInteger(0);
        final AtomicReference<MetadataProvenance> provenance =
            new AtomicReference<>(MetadataProvenance.EMPTY);
        final MockTime time = new MockTime();
        final MetadataLoaderMetrics metrics;

        FakeMetadataLoaderMetrics(MetricsRegistry registry) {
            this(Optional.of(registry));
        }

        FakeMetadataLoaderMetrics(Optional<MetricsRegistry> registry) {
            metrics = new MetadataLoaderMetrics(
                registry,
                batchProcessingTimeNs::set,
                batchSize::set,
                provenance);
        }

        @Override
        public void close() {
            metrics.close();
        }
    }

    @Test
    public void testMetricNames() {
        MetricsRegistry registry = new MetricsRegistry();
        try {
            try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount",
                        "kafka.server:type=MetadataLoader,name=AvgIdleRatio"
                    )
                );

                // Record some feature levels and verify their metrics are registered
                fakeMetrics.metrics.recordFinalizedFeatureLevel("metadata.version", (short) 3);
                fakeMetrics.metrics.recordFinalizedFeatureLevel("kraft.version", (short) 4);

                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount",
                        "kafka.server:type=MetadataLoader,name=AvgIdleRatio",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=metadataVersion",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=kraftVersion"
                    )
                );
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of());
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testUpdateBatchProcessingTimeNs() {
        MetricsRegistry registry = new MetricsRegistry();
        try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
            fakeMetrics.metrics.updateBatchProcessingTimeNs(123L);
            assertEquals(123L, fakeMetrics.batchProcessingTimeNs.get());
        }
    }

    @Test
    public void testUpdateBatchSize() {
        MetricsRegistry registry = new MetricsRegistry();
        try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
            fakeMetrics.metrics.updateBatchSize(50);
            assertEquals(50, fakeMetrics.batchSize.get());
        }
    }

    @Test
    public void testUpdateLastAppliedImageProvenance() {
        MetricsRegistry registry = new MetricsRegistry();
        try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
            MetadataProvenance provenance = new MetadataProvenance(1L, 2, 3L, true);
            fakeMetrics.metrics.updateLastAppliedImageProvenance(provenance);
            assertEquals(provenance, fakeMetrics.provenance.get());
        }
    }

    @Test
    public void testManagedMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        try {
            try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
                fakeMetrics.metrics.setCurrentMetadataVersion(MetadataVersion.IBP_3_7_IV0);
                fakeMetrics.metrics.incrementHandleLoadSnapshotCount();
                fakeMetrics.metrics.incrementHandleLoadSnapshotCount();

                @SuppressWarnings("unchecked")
                Gauge<Integer> currentMetadataVersion = (Gauge<Integer>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "CurrentMetadataVersion"));
                assertEquals(MetadataVersion.IBP_3_7_IV0.featureLevel(),
                    currentMetadataVersion.value().shortValue());

                @SuppressWarnings("unchecked")
                Gauge<Long> loadSnapshotCount = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "HandleLoadSnapshotCount"));
                assertEquals(2L, loadSnapshotCount.value().longValue());
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                Set.of());
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testInitialValueOfCurrentControllerId() {
        try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(Optional.empty())) {
            assertEquals(-1, fakeMetrics.metrics.currentControllerId());
        }
    }

    @Test
    public void testSetValueOfCurrentControllerId() {
        try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(Optional.empty())) {
            fakeMetrics.metrics.setCurrentControllerId(1001);
            assertEquals(1001, fakeMetrics.metrics.currentControllerId());
        }
    }

    @Test
    public void testFinalizedFeatureLevelMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        try {
            try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
                // Initially no finalized level metrics should be registered
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount",
                        "kafka.server:type=MetadataLoader,name=AvgIdleRatio"
                    )
                );

                // Record metadata version and verify its metric
                fakeMetrics.metrics.recordFinalizedFeatureLevel(MetadataVersion.FEATURE_NAME, (short) 5);
                @SuppressWarnings("unchecked")
                Gauge<Short> finalizedMetadataVersion = (Gauge<Short>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "FinalizedLevel", "featureName=metadataVersion"));
                assertEquals((short) 5, finalizedMetadataVersion.value());

                // Record KRaft version and verify its metric
                fakeMetrics.metrics.recordFinalizedFeatureLevel(KRaftVersion.FEATURE_NAME, (short) 1);
                @SuppressWarnings("unchecked")
                Gauge<Short> finalizedKRaftVersion = (Gauge<Short>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "FinalizedLevel", "featureName=kraftVersion"));
                assertEquals((short) 1, finalizedKRaftVersion.value());

                // Record transaction version and verify its metric
                fakeMetrics.metrics.recordFinalizedFeatureLevel(TransactionVersion.FEATURE_NAME, (short) 1);
                @SuppressWarnings("unchecked")
                Gauge<Short> finalizedTransactionVersion = (Gauge<Short>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "FinalizedLevel", "featureName=transactionVersion"));
                assertEquals((short) 1, finalizedTransactionVersion.value());

                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount",
                        "kafka.server:type=MetadataLoader,name=AvgIdleRatio",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=metadataVersion",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=kraftVersion",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=transactionVersion"
                    )
                );

                // When a feature's finalized level is not present in the new image, its metric should be removed
                // This does not apply to metadataVersion and kraftVersion
                fakeMetrics.metrics.maybeRemoveFinalizedFeatureLevelMetrics(Map.of());
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount",
                        "kafka.server:type=MetadataLoader,name=AvgIdleRatio",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=kraftVersion",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=metadataVersion"
                    )
                );

                // Set the finalized feature level and check the metric is added back with its correct value
                fakeMetrics.metrics.recordFinalizedFeatureLevel(TransactionVersion.FEATURE_NAME, (short) 2);
                @SuppressWarnings("unchecked")
                Gauge<Short> finalizedTransactionVersion2 = (Gauge<Short>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "FinalizedLevel", "featureName=transactionVersion"));
                assertEquals((short) 2, finalizedTransactionVersion2.value());
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Set.of(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount",
                        "kafka.server:type=MetadataLoader,name=AvgIdleRatio",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=metadataVersion",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=kraftVersion",
                        "kafka.server:type=MetadataLoader,name=FinalizedLevel,featureName=transactionVersion"
                    )
                );
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                Set.of());
        } finally {
            registry.shutdown();
        }
    }
    @Test
    public void testAvgIdleRatio() {
        final double delta = 0.001;
        MetricsRegistry registry = new MetricsRegistry();
        try (FakeMetadataLoaderMetrics fakeMetrics = new FakeMetadataLoaderMetrics(registry)) {
            @SuppressWarnings("unchecked")
            Gauge<Double> avgIdleRatio = (Gauge<Double>) registry.allMetrics().get(metricName("MetadataLoader", "AvgIdleRatio"));

            // No idle time recorded yet; returns default ratio of 1.0
            assertEquals(1.0, avgIdleRatio.value(), delta);

            // The first updateIdleTime call is ignored by the TimeRatio sensor.
            // This establishes the baseline timestamp for subsequent measurements.
            fakeMetrics.metrics.updateIdleTime(10, fakeMetrics.time.milliseconds());
            fakeMetrics.time.sleep(40);
            fakeMetrics.metrics.updateIdleTime(20, fakeMetrics.time.milliseconds());
            // avgIdleRatio = (20ms idle) / (40ms interval) = 0.5
            assertEquals(0.5, avgIdleRatio.value(), delta);

            fakeMetrics.time.sleep(20);
            fakeMetrics.metrics.updateIdleTime(1, fakeMetrics.time.milliseconds());
            // avgIdleRatio = (1ms idle) / (20ms interval) = 0.05
            assertEquals(0.05, avgIdleRatio.value(), delta);
        } finally {
            registry.shutdown();
        }
    }

    private static MetricName metricName(String type, String name) {
        String mBeanName = String.format("kafka.server:type=%s,name=%s", type, name);
        return new MetricName("kafka.server", type, name, null, mBeanName);
    }

    private static MetricName metricName(String type, String name, String scope) {
        String mBeanName = String.format("kafka.server:type=%s,name=%s,%s", type, name, scope);
        return new MetricName("kafka.server", type, name, scope, mBeanName);
    }
}
