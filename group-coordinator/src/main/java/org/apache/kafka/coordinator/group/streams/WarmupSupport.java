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
 * Whether a member may be handed a warm-up task, as far as the version of its latest streams group heartbeat tells.
 *
 * <p>A warm-up task is only useful if the coordinator can observe how far the member has restored it, which a member
 * reports through the task offsets of a version 1 (or newer) heartbeat. A member that heartbeats with version 0 never
 * reports them, so it could never be promoted from warm-up to active.
 */
enum WarmupSupport {

    SUPPORTED,

    NOT_SUPPORTED,

    /**
     * The member has not heartbeated since the coordinator loaded the group.
     */
    UNKNOWN;

    private static final int FIRST_VERSION_WITH_TASK_OFFSETS = 1;

    /**
     * @param heartbeatVersion The version of the member's latest heartbeat, or {@code null} if it is not known.
     */
    static WarmupSupport of(final Integer heartbeatVersion) {
        if (heartbeatVersion == null) {
            return UNKNOWN;
        }
        return heartbeatVersion >= FIRST_VERSION_WITH_TASK_OFFSETS ? SUPPORTED : NOT_SUPPORTED;
    }
}
