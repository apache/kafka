package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

public interface AclMigrationClient {
    ZkMigrationLeadershipState removeDeletedAcls(
        ResourcePattern resourcePattern,
        List<AccessControlEntry> deletedAcls,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState writeAddedAcls(
        ResourcePattern resourcePattern,
        List<AccessControlEntry> addedAcls,
        ZkMigrationLeadershipState state
    );

    void iterateAcls(BiConsumer<ResourcePattern, Set<AccessControlEntry>> aclConsumer);
}
