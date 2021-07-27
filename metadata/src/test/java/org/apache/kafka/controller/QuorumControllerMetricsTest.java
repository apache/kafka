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

package org.apache.kafka.controller;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumControllerMetricsTest {
    @Test
    public void testKafkaControllerMetricNames() {
        String expectedType = "KafkaController";
        Set<String> expectedMetricNames = Utils.mkSet(
            "ActiveControllerCount",
            "GlobalTopicCount",
            "GlobalPartitionCount",
            "OfflinePartitionsCount",
            "PreferredReplicaImbalanceCount");
        assertExpectedMetrics(expectedMetricNames, expectedType);
    }

    @Test
    public void testControllerEventManagerMetricNames() {
        String expectedType = "ControllerEventManager";
        Set<String> expectedMetricNames = Utils.mkSet(
            "EventQueueTimeMs",
            "EventQueueProcessingTimeMs");
        assertExpectedMetrics(expectedMetricNames, expectedType);
    }

    private static void assertExpectedMetrics(Set<String> expectedMetricNames, String expectedType) {
        String expectedGroup = "kafka.controller";
        MetricsRegistry registry = new MetricsRegistry();
        new QuorumControllerMetrics(registry); // populates the registry
        expectedMetricNames.stream().forEach(expectedMetricName -> assertTrue(
            registry.allMetrics().keySet().stream().anyMatch(metricName -> {
                if (metricName.getGroup().equals(expectedGroup) && metricName.getType().equals(expectedType)
                    && metricName.getScope() == null && metricName.getName().equals(expectedMetricName)) {
                    // It has to exist AND the MBean name has to be correct;
                    // fail right here if the MBean name doesn't match
                    String expectedMBeanPrefix = expectedGroup + ":type=" + expectedType + ",name=";
                    assertEquals(expectedMBeanPrefix + expectedMetricName, metricName.getMBeanName(),
                        "Incorrect MBean name");
                    return true; // the expected metric name exists and the associated MBean name matches
                } else {
                    return false; // this one didn't match
                }
            }), "Missing metric: " + expectedMetricName));
    }
}
