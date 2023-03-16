package org.apache.kafka.image;

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.migration.ZkMigrationState;

import java.util.Objects;

public class ZkMigrationImage {
    public static final ZkMigrationImage EMPTY = new ZkMigrationImage(ZkMigrationState.UNINITIALIZED);

    private final ZkMigrationState state;

    ZkMigrationImage(ZkMigrationState state) {
        this.state = state;
    }

    public ZkMigrationState migrationState() {
        return state;
    }

    public boolean isEmpty() {
        return this.equals(ZkMigrationImage.EMPTY);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (options.metadataVersion().isMigrationSupported()) {
            writer.write(0, new ZkMigrationStateRecord().setZkMigrationState(state.value()));
        } else {
            options.handleLoss("the ZK Migration state");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZkMigrationImage that = (ZkMigrationImage) o;
        return state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state);
    }

    @Override
    public String toString() {
        return "ZkMigrationImage{" +
                "state=" + state +
                '}';
    }
}
