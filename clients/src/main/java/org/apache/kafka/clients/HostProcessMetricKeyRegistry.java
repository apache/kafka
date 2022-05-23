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
package org.apache.kafka.clients;

import org.apache.kafka.common.telemetry.metrics.MetricKey;

/**
 * Metrics corresponding to the host-level per
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714:+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Hostprocessmetrics(optional)">KIP-714</a>.
 */
public class HostProcessMetricKeyRegistry {

    private final static String PREFIX = "org.apache.kafka.client.process.";

    private final static String MEMORY_BYTES_NAME = PREFIX + "memory.bytes";

    private final static String CPU_USER_TIME_NAME = PREFIX + "cpu.user.time";

    private final static String CPU_SYSTEM_TIME_NAME = PREFIX + "cpu.system.time";

    private final static String PID_NAME = PREFIX + "process.pid";

    public final MetricKey memoryBytes;

    public final MetricKey cpuUserTime;

    public final MetricKey cpuSystemTime;

    public final MetricKey pid;

    public HostProcessMetricKeyRegistry() {
        this.memoryBytes = new MetricKey(MEMORY_BYTES_NAME);
        this.cpuUserTime = new MetricKey(CPU_USER_TIME_NAME);
        this.cpuSystemTime = new MetricKey(CPU_SYSTEM_TIME_NAME);
        this.pid = new MetricKey(PID_NAME);
    }

}
