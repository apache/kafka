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

import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the client host process metrics.
 */

public interface HostProcessMetricRecorder extends ClientMetricRecorder {

    String PREFIX = ClientMetricRecorder.PREFIX + "process.";

    String MEMORY_BYTES_NAME = PREFIX + "memory.bytes";

    String MEMORY_BYTES_DESCRIPTION = "Current process/runtime memory usage (RSS, not virtual).";

    String CPU_USER_TIME_NAME = PREFIX + "cpu.user.time";

    String CPU_USER_TIME_DESCRIPTION = "User CPU time used (seconds).";

    String CPU_SYSTEM_TIME_NAME = PREFIX + "cpu.system.time";

    String CPU_SYSTEM_TIME_DESCRIPTION = "System CPU time used (seconds).";

    String PID_NAME = PREFIX + "process.pid";

    String PID_DESCRIPTION = "The process id. Can be used, in conjunction with the client host name to map multiple client instances to the same process.";

    void setMemoryBytes(long amount);

    void setCpuUserTime(long seconds);

    void setCpuSystemTime(long seconds);

    void setPid(long pid);

}
