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
package org.apache.kafka.migration;

/**
 * UNINITIALIZED ─────────────► WAIR_FOR_CONTROLLER_QUORUM ─────────────► BECOME_LEADER ─────────────► WAIT_FOR_BROKERS
 *
 *      │                                 │                                    │                             │
 *      │                                 │                                    │                             │
 *      │                                 │                                    │                             │
 *      │                                 ◄────────────────────────────────────┘◄────────────────────────────┤
 *      │                                 │                                                                  │
 *      │                                 │                                                                  │
 *      │                                 ▼                                                                  ▼
 *      │
 *      └─────────────────────────────► INACTIVE ◄───────────────────────── DUAL_WRITE  ◄───────────── ZK_MIGRATION
 **/
public enum MigrationState {
    UNINITIALIZED(false),                 // Initial state.
    INACTIVE(false),                      // State when not the active controller.
    WAIT_FOR_CONTROLLER_QUORUM(false),    // Ensure all the quorum nodes are ready for migration.
    BECOME_CONTROLLER(false),             // Become controller for the Zk Brokers.
    WAIT_FOR_BROKERS(true),               // Wait for Zk brokers to be ready for migration.
    ZK_MIGRATION(true),                   // The cluster has satisfied the migration criteria
    DUAL_WRITE(true);                     // The data has been migrated

    private final boolean isActiveController;

    MigrationState(boolean isActiveController) {
        this.isActiveController = isActiveController;
    }

    boolean isActiveController() {
        return isActiveController;
    }
}
