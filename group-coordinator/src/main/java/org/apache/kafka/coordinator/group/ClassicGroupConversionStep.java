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
package org.apache.kafka.coordinator.group;

/**
 * A step that must run before a classic JoinGroup request may convert a group of another type
 * to a classic group, for example to release state held outside the coordinator that the
 * conversion would otherwise orphan.
 *
 * <p>A group reports its step through {@link Group#stepBeforeClassicConversion()}. The shard
 * then changes nothing and returns the step, and the caller runs it and calls the join again.
 * To add a step for a group type, add a value here, return it from that group's
 * {@link Group#stepBeforeClassicConversion()}, and handle it in the caller. The classic join
 * logic itself does not change.
 */
public enum ClassicGroupConversionStep {
    /**
     * The group is an empty streams group whose topology may still be held by the
     * topology description plugin (KIP-1331). The caller must delete the topology from the
     * plugin before the group is converted.
     */
    DELETE_STREAMS_TOPOLOGY
}
