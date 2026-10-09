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
package org.apache.kafka.coordinator.group.streams;

/**
 * Whether the coordinator may hand a member a warm-up task, as far as it knows from the member's heartbeats.
 *
 * <p>A warm-up task is only useful if the coordinator can observe how far the member has restored it, which a member
 * reports through the task offsets of a version 1 (or newer) streams group heartbeat. A member that speaks version 0
 * never reports them, so it could never be promoted from warm-up to active.
 */
public enum WarmupSupport {

    /**
     * The member's last heartbeat was version 1 or newer, so it reports its restore progress.
     */
    SUPPORTED,

    /**
     * The member's last heartbeat was version 0, so its restore progress cannot be observed.
     */
    NOT_SUPPORTED,

    /**
     * No heartbeat of the member has been seen since the coordinator loaded the group, for example right after a
     * coordinator failover, so it is not known which version the member speaks.
     */
    UNKNOWN;

    /**
     * The first streams group heartbeat version in which members report their task offsets.
     */
    static final int FIRST_VERSION_WITH_TASK_OFFSETS = 1;

    /**
     * @param heartbeatVersion The version of the member's last streams group heartbeat.
     */
    static WarmupSupport ofHeartbeatVersion(final int heartbeatVersion) {
        return heartbeatVersion >= FIRST_VERSION_WITH_TASK_OFFSETS ? SUPPORTED : NOT_SUPPORTED;
    }
}
