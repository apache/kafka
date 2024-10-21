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

import org.apache.kafka.controller.metrics.ControllerMetricsTestUtils;
import org.apache.kafka.image.MetadataProvenance;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV2;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class MetadataLoaderMetricsTest {
    private static class FakeMetadataLoaderMetrics implements AutoCloseable {
        final AtomicLong batchProcessingTimeNs = new AtomicLong(0L);
        final AtomicInteger batchSize = new AtomicInteger(0);
        final AtomicReference<MetadataProvenance> provenance =
            new AtomicReference<>(MetadataProvenance.EMPTY);
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
                    new HashSet<>(Arrays.asList(
                        "kafka.server:type=MetadataLoader,name=CurrentControllerId",
                        "kafka.server:type=MetadataLoader,name=CurrentMetadataVersion",
                        "kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount"
                    )));
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                    Collections.emptySet());
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
                fakeMetrics.metrics.setCurrentMetadataVersion(IBP_3_3_IV2);
                fakeMetrics.metrics.incrementHandleLoadSnapshotCount();
                fakeMetrics.metrics.incrementHandleLoadSnapshotCount();

                @SuppressWarnings("unchecked")
                Gauge<Integer> currentMetadataVersion = (Gauge<Integer>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "CurrentMetadataVersion"));
                assertEquals(IBP_3_3_IV2.featureLevel(),
                    currentMetadataVersion.value().shortValue());

                @SuppressWarnings("unchecked")
                Gauge<Long> loadSnapshotCount = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("MetadataLoader", "HandleLoadSnapshotCount"));
                assertEquals(2L, loadSnapshotCount.value().longValue());
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.server",
                Collections.emptySet());
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

    private static MetricName metricName(String type, String name) {
        String mBeanName = String.format("kafka.server:type=%s,name=%s", type, name);
        return new MetricName("kafka.server", type, name, null, mBeanName);
    }
}
