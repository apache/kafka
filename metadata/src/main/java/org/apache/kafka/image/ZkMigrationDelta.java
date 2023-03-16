package org.apache.kafka.image;

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.metadata.migration.ZkMigrationState;

public class ZkMigrationDelta {
    private final ZkMigrationImage image;

    private ZkMigrationState updatedState;

    public ZkMigrationDelta(ZkMigrationImage image) {
        this.image = image;
    }

    public void replay(ZkMigrationStateRecord record) {
        this.updatedState = ZkMigrationState.of(record.zkMigrationState());
    }

    public void finishSnapshot() {
        // no-op
    }

    public ZkMigrationImage apply() {
        return new ZkMigrationImage(updatedState);
    }
}
