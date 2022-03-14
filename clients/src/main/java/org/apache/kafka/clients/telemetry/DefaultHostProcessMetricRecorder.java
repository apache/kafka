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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the client host process metrics.
 */

public class DefaultHostProcessMetricRecorder extends MetricRecorder implements HostProcessMetricRecorder {

    private final static String GROUP_NAME = "host-process-telemetry";

    private final MetricName memoryBytes;

    private final MetricName cpuUserTime;

    private final MetricName cpuSystemTime;

    private final MetricName pid;

    public DefaultHostProcessMetricRecorder(Metrics metrics) {
        super(metrics);

        this.memoryBytes = createMetricName(MEMORY_BYTES_NAME, GROUP_NAME, MEMORY_BYTES_DESCRIPTION);
        this.cpuUserTime = createMetricName(CPU_USER_TIME_NAME, GROUP_NAME, CPU_USER_TIME_DESCRIPTION);
        this.cpuSystemTime = createMetricName(CPU_SYSTEM_TIME_NAME, GROUP_NAME, CPU_SYSTEM_TIME_DESCRIPTION);
        this.pid = createMetricName(PID_NAME, GROUP_NAME, PID_DESCRIPTION);
    }

    // For testing
    MetricName memoryBytes() {
        return memoryBytes;
    }

    // For testing
    MetricName cpuUserTime() {
        return cpuUserTime;
    }

    // For testing
    MetricName cpuSystemTime() {
        return cpuSystemTime;
    }

    // For testing
    MetricName pid() {
        return pid;
    }

    @Override
    public void setMemoryBytes(long amount) {
        gaugeUpdateSensor(memoryBytes).record(amount);
    }

    @Override
    public void setCpuUserTime(long seconds) {
        gaugeUpdateSensor(cpuUserTime).record(seconds);
    }

    @Override
    public void setCpuSystemTime(long seconds) {
        gaugeUpdateSensor(cpuSystemTime).record(seconds);
    }

    @Override
    public void setPid(long pid) {
        gaugeUpdateSensor(this.pid).record(pid);
    }
}
