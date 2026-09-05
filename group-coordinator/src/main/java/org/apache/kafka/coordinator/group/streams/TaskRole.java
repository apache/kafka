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
 * The role a member holds a task in. The three roles of a task are the three task sets of a {@link TasksTuple}, and a
 * process holds a given task in at most one of them, on at most one of its members.
 */
public enum TaskRole {

    /**
     * The member runs the task: it reads the task's input topics and produces its output.
     */
    ACTIVE,

    /**
     * The member keeps the task's state up to date from the changelog without running the task, so that it can take
     * the task over quickly. How many of these a task has is bounded by {@code num.standby.replicas}.
     */
    STANDBY,

    /**
     * The member is restoring the task's state because the task is moving to it, and will run the task once the state
     * has caught up. Like a standby task in everything but its purpose and its accounting: how many of these the group
     * runs at a time is bounded by {@code num.warmup.replicas} rather than by {@code num.standby.replicas}.
     */
    WARMUP
}
