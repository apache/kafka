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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import java.util.Optional;

public class HostProcessInfo {

    private final com.sun.management.OperatingSystemMXBean osMxBean;

    public HostProcessInfo() {
        OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

        if (bean instanceof com.sun.management.OperatingSystemMXBean) {
            osMxBean = (com.sun.management.OperatingSystemMXBean) bean;
        } else {
            osMxBean = null;
        }
    }

    public Optional<Long> cpuUserTimeSec() {
        if (osMxBean != null) {
            long nanos = osMxBean.getProcessCpuTime();

            // If not supported, value returned is -1.
            if (nanos != -1)
                return Optional.of(Duration.ofNanos(nanos).getSeconds());
        }

        return Optional.empty();
    }

    public Optional<Long> cpuSystemTimeSec() {
        return Optional.empty();
    }

    public Optional<Long> processMemoryBytes() {
        // TODO: TELEMETRY_TODO: validate.
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = bean.getHeapMemoryUsage();
        MemoryUsage nonHeap = bean.getNonHeapMemoryUsage();
        return Optional.of(heap.getUsed() + nonHeap.getUsed());
    }

    public Optional<Long> pid() {
        // TODO: TELEMETRY_TODO: to implement.
        return Optional.empty();
    }

    public void recordHostMetrics(HostProcessMetricRecorder recorder) {
        cpuSystemTimeSec().ifPresent(recorder::setCpuSystemTime);
        cpuUserTimeSec().ifPresent(recorder::setCpuUserTime);
        pid().ifPresent(recorder::setPid);
        processMemoryBytes().ifPresent(recorder::setMemoryBytes);
    }

}
