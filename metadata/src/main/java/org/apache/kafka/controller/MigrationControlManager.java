package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineObject;

public class MigrationControlManager {
    private final TimelineObject<ZkMigrationState> zkMigrationState;

    MigrationControlManager(SnapshotRegistry snapshotRegistry) {
        zkMigrationState = new TimelineObject<>(snapshotRegistry, ZkMigrationState.NONE);
    }

    public ZkMigrationState zkMigrationState() {
        return zkMigrationState.get();
    }

    void replay(ZkMigrationStateRecord record) {
        zkMigrationState.set(ZkMigrationState.of(record.zkMigrationState()));
    }
}
