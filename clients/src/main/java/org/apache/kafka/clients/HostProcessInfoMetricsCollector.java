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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import org.apache.kafka.common.telemetry.collector.AbstractMetricsCollector;
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.apache.kafka.common.telemetry.metrics.MetricType;
import org.apache.kafka.common.utils.Time;

/**
 * This metrics collector is in charge of collecting various JVM level values.
 */
public class HostProcessInfoMetricsCollector extends AbstractMetricsCollector {

    private final HostProcessInfo hostProcessInfo;

    private final HostProcessMetricKeyRegistry registry;

    public HostProcessInfoMetricsCollector(Time time) {
        super(time);
        this.hostProcessInfo = new HostProcessInfo();
        this.registry = new HostProcessMetricKeyRegistry();
    }

    @Override
    public void collect(Emitter emitter) {
        maybeEmitLong(emitter, registry.cpuSystemTime, MetricType.sum, hostProcessInfo::cpuSystemTimeSec);
        maybeEmitLong(emitter, registry.cpuUserTime, MetricType.sum, hostProcessInfo::cpuUserTimeSec);
        maybeEmitLong(emitter, registry.memoryBytes, MetricType.gauge, hostProcessInfo::memoryBytes);
        maybeEmitLong(emitter, registry.pid, MetricType.gauge, hostProcessInfo::pid);
    }
    public static class HostProcessInfo {

        private final com.sun.management.OperatingSystemMXBean osMxBean;

        public HostProcessInfo() {
            OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

            if (bean instanceof com.sun.management.OperatingSystemMXBean) {
                osMxBean = (com.sun.management.OperatingSystemMXBean) bean;
            } else {
                osMxBean = null;
            }
        }

        public Long cpuUserTimeSec() {
            if (osMxBean != null) {
                long nanos = osMxBean.getProcessCpuTime();

                // If not supported, value returned is -1.
                if (nanos != -1)
                    return Duration.ofNanos(nanos).getSeconds();
            }

            return null;
        }

        public Long cpuSystemTimeSec() {
            return null;
        }

        public Long memoryBytes() {
            MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = bean.getHeapMemoryUsage();
            MemoryUsage nonHeap = bean.getNonHeapMemoryUsage();
            return heap.getUsed() + nonHeap.getUsed();
        }

        public Long pid() {
            return -1L;
        }

    }

}
