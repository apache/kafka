package org.apache.kafka.metadata.migration;

@FunctionalInterface
public interface KRaftMigrationOperation {
    ZkMigrationLeadershipState apply(ZkMigrationLeadershipState migrationState);
}
