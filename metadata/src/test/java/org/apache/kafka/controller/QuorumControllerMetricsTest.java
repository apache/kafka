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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuorumControllerMetricsTest {
    @Test
    public void testKafkaControllerMetricNames() {
        String expectedGroup = "kafka.controller";
        String expectedType = "KafkaController";
        Set<String> expectedMetricNames = Utils.mkSet(
            "ActiveControllerCount",
            "GlobalTopicCount",
            "GlobalPartitionCount",
            "OfflinePartitionCount",
            "PreferredReplicaImbalanceCount");
        Set<String> missingMetrics = getMissingMetricNames(expectedMetricNames, expectedGroup, expectedType);
        assertEquals(Collections.emptySet(), missingMetrics, "Expected metrics did not exist");
    }

    @Test
    public void testControllerEventManagerMetricNames() {
        String expectedGroup = "kafka.controller";
        String expectedType = "ControllerEventManager";
        Set<String> expectedMetricNames = new HashSet<>(Arrays.asList(
            "EventQueueTimeMs",
            "EventQueueProcessingTimeMs"));
        Set<String> missingMetrics = getMissingMetricNames(expectedMetricNames, expectedGroup, expectedType);
        assertEquals(Collections.emptySet(), missingMetrics, "Expected metrics did not exist");
    }

    private static Set<String> getMissingMetricNames(
        Set<String> expectedMetricNames, String expectedGroup, String expectedType) {
        MetricsRegistry registry = new MetricsRegistry();
        new QuorumControllerMetrics(registry); // populates the registry
        Set<String> foundMetricNames = expectedMetricNames.stream().filter(expectedMetricName ->
            registry.allMetrics().keySet().stream().anyMatch(metricName -> {
                if (metricName.getGroup().equals(expectedGroup) && metricName.getType().equals(expectedType)
                    && metricName.getScope() == null && metricName.getName().equals(expectedMetricName)) {
                    // It has to exist AND the MBean name has to be correct;
                    // fail right here if the MBean name doesn't match
                    String expectedMBeanPrefix = expectedGroup + ":type=" + expectedType + ",name=";
                    assertEquals(expectedMBeanPrefix + expectedMetricName, metricName.getMBeanName());
                    return true; // the expected metric name exists and the associated MBean name matches
                } else {
                    return false; // this one didn't match
                }
            })).collect(Collectors.toSet());
        Set<String> missingMetricNames = new HashSet<>(expectedMetricNames);
        missingMetricNames.removeAll(foundMetricNames);
        return missingMetricNames;
    }
}
