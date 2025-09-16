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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.metadata.BrokerRegistration;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ControllerMetadataMetricsTest {
    @Test
    public void testMetricNames() {
        MetricsRegistry registry = new MetricsRegistry();
        try {
            try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
                metrics.addBrokerRegistrationStateMetric(0);
                metrics.setBrokerRegistrationState(
                    1,
                    brokerRegistration(false, false)
                );
                metrics.addBrokerRegistrationStateMetric(1);
                metrics.setBrokerRegistrationState(
                    2,
                    brokerRegistration(false, false)
                );
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.controller:",
                    Set.of(
                        "kafka.controller:type=KafkaController,name=ActiveBrokerCount",
                        "kafka.controller:type=KafkaController,name=FencedBrokerCount",
                        "kafka.controller:type=KafkaController,name=ControlledShutdownBrokerCount",
                        "kafka.controller:type=KafkaController,name=BrokerRegistrationState,broker=0",
                        "kafka.controller:type=KafkaController,name=BrokerRegistrationState,broker=1",
                        "kafka.controller:type=KafkaController,name=GlobalPartitionCount",
                        "kafka.controller:type=KafkaController,name=GlobalTopicCount",
                        "kafka.controller:type=KafkaController,name=MetadataErrorCount",
                        "kafka.controller:type=KafkaController,name=OfflinePartitionsCount",
                        "kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount",
                        "kafka.controller:type=KafkaController,name=IgnoredStaticVoters",
                        "kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec",
                        "kafka.controller:type=ControllerStats,name=ElectionFromEligibleLeaderReplicasPerSec"
                    ));
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "KafkaController",
                    Set.of());
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testMetadataErrorCount() {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            @SuppressWarnings("unchecked")
            Gauge<Integer> metadataErrorCount = (Gauge<Integer>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "MetadataErrorCount"));
            assertEquals(0, metadataErrorCount.value());
            metrics.incrementMetadataErrorCount();
            assertEquals(1, metadataErrorCount.value());
        } finally {
            registry.shutdown();
        }
    }

    private static MetricName metricName(String type, String name) {
        String mBeanName = String.format("kafka.controller:type=%s,name=%s", type, name);
        return new MetricName("kafka.controller", type, name, null, mBeanName);
    }

    private static MetricName metricName(String type, String name, String scope) {
        String mBeanName = String.format("kafka.controller:type=%s,name=%s,%s", type, name, scope);
        return new MetricName("kafka.controller", type, name, scope, mBeanName);
    }

    private void testIntGaugeMetric(
        Function<ControllerMetadataMetrics, Integer> metricsGetter,
        Function<MetricsRegistry, Integer> registryGetter,
        BiConsumer<ControllerMetadataMetrics, Integer> setter,
        BiConsumer<ControllerMetadataMetrics, Integer> incrementer
    ) {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            assertEquals(0, metricsGetter.apply(metrics));
            assertEquals(0, registryGetter.apply(registry));
            setter.accept(metrics, 123);
            assertEquals(123, metricsGetter.apply(metrics));
            assertEquals(123, registryGetter.apply(registry));
            incrementer.accept(metrics, 123);
            assertEquals(246, metricsGetter.apply(metrics));
            assertEquals(246, registryGetter.apply(registry));
            incrementer.accept(metrics, -246);
            assertEquals(0, metricsGetter.apply(metrics));
            assertEquals(0, registryGetter.apply(registry));
        } finally {
            registry.shutdown();
        }
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testFencedBrokerMetric() {
        testIntGaugeMetric(
            m -> m.fencedBrokerCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "FencedBrokerCount"))).value(),
            (m, v) -> m.setFencedBrokerCount(v),
            (m, v) -> m.addToFencedBrokerCount(v)
        );
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testActiveBrokerCountMetric() {
        testIntGaugeMetric(
            m -> m.activeBrokerCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "ActiveBrokerCount"))).value(),
            (m, v) -> m.setActiveBrokerCount(v),
            (m, v) -> m.addToActiveBrokerCount(v)
        );
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testControlledShutdownCountMetric() {
        testIntGaugeMetric(
            m -> m.controlledShutdownBrokerCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "ControlledShutdownBrokerCount"))).value(),
            (m, v) -> m.setControlledShutdownBrokerCount(v),
            (m, v) -> m.addToControlledShutdownBrokerCount(v)
        );
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testBrokerRegistrationStateMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            int brokerId = 1;
            MetricName name = metricName("KafkaController", "BrokerRegistrationState", "broker=1");

            metrics.addBrokerRegistrationStateMetric(brokerId);
            Gauge<Integer> registrationState = (Gauge<Integer>) registry.allMetrics().get(name);

            metrics.setBrokerRegistrationState(brokerId, brokerRegistration(false, false));
            assertEquals(BrokerRegistrationState.ACTIVE.state(), registrationState.value());

            metrics.setBrokerRegistrationState(brokerId, brokerRegistration(true, false));
            assertEquals(BrokerRegistrationState.FENCED.state(), registrationState.value());

            metrics.setBrokerRegistrationState(brokerId, brokerRegistration(false, true));
            assertEquals(BrokerRegistrationState.CONTROLLED_SHUTDOWN.state(), registrationState.value());

            metrics.setBrokerRegistrationState(brokerId, null);
            assertEquals(BrokerRegistrationState.UNREGISTERED.state(), registrationState.value());
            assertNull(registry.allMetrics().get(name));
        } finally {
            registry.shutdown();
        }
    }

    private BrokerRegistration brokerRegistration(boolean fenced, boolean inControlledShutdown) {
        return new BrokerRegistration.Builder()
            .setFenced(fenced)
            .setInControlledShutdown(inControlledShutdown)
            .build();
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testGlobalTopicCountMetric() {
        testIntGaugeMetric(
            m -> m.globalTopicCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "GlobalTopicCount"))).value(),
            (m, v) -> m.setGlobalTopicCount(v),
            (m, v) -> m.addToGlobalTopicCount(v)
        );
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testGlobalPartitionCountMetric() {
        testIntGaugeMetric(
            m -> m.globalPartitionCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "GlobalPartitionCount"))).value(),
            (m, v) -> m.setGlobalPartitionCount(v),
            (m, v) -> m.addToGlobalPartitionCount(v)
        );
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testOfflinePartitionCountMetric() {
        testIntGaugeMetric(
            m -> m.offlinePartitionCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "OfflinePartitionsCount"))).value(),
            (m, v) -> m.setOfflinePartitionCount(v),
            (m, v) -> m.addToOfflinePartitionCount(v)
        );
    }

    @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
    @Test
    public void testPreferredReplicaImbalanceCountMetric() {
        testIntGaugeMetric(
            m -> m.preferredReplicaImbalanceCount(),
            registry -> ((Gauge<Integer>) registry.allMetrics().
                    get(metricName("KafkaController", "PreferredReplicaImbalanceCount"))).value(),
            (m, v) -> m.setPreferredReplicaImbalanceCount(v),
            (m, v) -> m.addToPreferredReplicaImbalanceCount(v)
        );
    }

    @SuppressWarnings("LocalVariableName")
    @Test
    public void testUpdateUncleanLeaderElection() {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            Meter UncleanLeaderElectionsPerSec = (Meter) registry
                    .allMetrics()
                    .get(metricName("ControllerStats", "UncleanLeaderElectionsPerSec"));
            assertEquals(0, UncleanLeaderElectionsPerSec.count());
            metrics.updateUncleanLeaderElection(2);
            assertEquals(2, UncleanLeaderElectionsPerSec.count());
        } finally {
            registry.shutdown();
        }
    }

    @SuppressWarnings("LocalVariableName")
    @Test
    public void testUpdateElectionFromEligibleLeaderReplicasCount() {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            Meter ElectionFromEligibleLeaderReplicasPerSec = (Meter) registry
                .allMetrics()
                .get(metricName("ControllerStats", "ElectionFromEligibleLeaderReplicasPerSec"));
            assertEquals(0, ElectionFromEligibleLeaderReplicasPerSec.count());
            metrics.updateElectionFromEligibleLeaderReplicasCount(2);
            assertEquals(2, ElectionFromEligibleLeaderReplicasPerSec.count());
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testIgnoredStaticVoters() {
        MetricsRegistry registry = new MetricsRegistry();
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            @SuppressWarnings("unchecked")
            Gauge<Integer> ignoredStaticVoters = (Gauge<Integer>) registry
                .allMetrics()
                .get(metricName("KafkaController", "IgnoredStaticVoters"));
            assertEquals(0, ignoredStaticVoters.value());
            metrics.setIgnoredStaticVoters(true);
            assertEquals(1, ignoredStaticVoters.value());
            metrics.setIgnoredStaticVoters(false);
            assertEquals(0, ignoredStaticVoters.value());
        } finally {
            registry.shutdown();
        }
    }
}
