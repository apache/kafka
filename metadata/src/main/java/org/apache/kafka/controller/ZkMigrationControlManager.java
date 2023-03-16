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
package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;


public class ZkMigrationControlManager {
    private final TimelineObject<ZkMigrationState> zkMigrationState;

    private final Logger log;

    ZkMigrationControlManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext
    ) {
        zkMigrationState = new TimelineObject<>(snapshotRegistry, ZkMigrationState.UNINITIALIZED);
        log = logContext.logger(ZkMigrationControlManager.class);
    }

    ZkMigrationState zkMigrationState() {
        return zkMigrationState.get();
    }

    boolean inPreMigrationMode() {
        return zkMigrationState.get() == ZkMigrationState.PRE_MIGRATION;
    }

    void replay(ZkMigrationStateRecord record) {
        ZkMigrationState recordState = ZkMigrationState.of(record.zkMigrationState());
        // From uninitialized, only allow PRE_MIGRATION
        if (zkMigrationState.get().equals(ZkMigrationState.UNINITIALIZED)) {
            if (recordState.equals(ZkMigrationState.PRE_MIGRATION) || recordState.equals(ZkMigrationState.NONE)) {
                log.info("Initializing ZK migration state as {}", recordState);
                zkMigrationState.set(recordState);
            } else {
                throw new IllegalStateException("The first migration state seen can only be PRE_MIGRATION or NONE, not " + recordState.name());
            }
        } else {
            switch (zkMigrationState.get()) {
                case NONE:
                    throw new IllegalStateException("Cannot ever change migration state from NONE");
                case PRE_MIGRATION:
                    if (recordState.equals(ZkMigrationState.MIGRATION)) {
                        log.info("Transitioning ZK migration state to {}", recordState);
                        zkMigrationState.set(recordState);
                    } else {
                        throw new IllegalStateException("Cannot change migration state from PRE_MIGRATION to " + recordState);
                    }
                    break;
                case MIGRATION:
                    if (recordState.equals(ZkMigrationState.POST_MIGRATION)) {
                        log.info("Transitioning ZK migration state to {}", recordState);
                        zkMigrationState.set(recordState);
                    } else {
                        throw new IllegalStateException("Cannot change migration state from MIGRATION to " + recordState);
                    }
                    break;
                case POST_MIGRATION:
                    throw new IllegalStateException("Cannot ever change migration state from POST_MIGRATION");
            }
        }

    }
}
