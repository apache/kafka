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
package org.apache.kafka.clients.telemetry;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


public class HostProcessTest extends BaseClientTelemetryTest {

    private DefaultClientTelemetry clientTelemetry;

    private DefaultHostProcessMetricRecorder hostProcessMetricRecorder;

    private HostProcessInfo hostProcessInfo;

    @BeforeEach
    public void setup() {
        clientTelemetry = newClientTelemetry();
        hostProcessMetricRecorder = clientTelemetry.hostProcessMetricRecorder();
        hostProcessInfo = clientTelemetry.hostProcessInfo();
    }

    @Test
    public void testMemoryBytes() {
        long bytes = testHostProcessMetric(hostProcessMetricRecorder.memoryBytes());
        assertTrue(bytes >= 1 << 10);
    }

    @Test
    @Disabled
    public void testCpuSystemTime() {
        long seconds = testHostProcessMetric(hostProcessMetricRecorder.cpuSystemTime());
        assertTrue(seconds >= 0);
    }

    @Test
    public void testCpuUserTime() {
        long seconds = testHostProcessMetric(hostProcessMetricRecorder.cpuUserTime());
        assertTrue(seconds >= 0);
    }

    @Test
    @Disabled
    public void testPid() {
        long pid = testHostProcessMetric(hostProcessMetricRecorder.pid());
        assertTrue(pid > 0);
    }

    private long testHostProcessMetric(MetricName metricName) {
        hostProcessInfo.recordHostMetrics(hostProcessMetricRecorder);
        Map<MetricName, TelemetryMetric> metrics = currentTelemetryMetrics(clientTelemetry, newTelemetrySubscription());
        assertNotNull(metrics);
        assertNotEquals(0, metrics.size());
        assertTrue(metrics.containsKey(metricName));
        return metrics.get(metricName).value();
    }

}
