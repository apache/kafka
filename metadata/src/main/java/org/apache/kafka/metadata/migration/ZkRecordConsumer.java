package org.apache.kafka.metadata.migration;

import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ZkRecordConsumer {
    void beginMigration();
    CompletableFuture<?> acceptBatch(List<ApiMessageAndVersion> recordBatch);
    OffsetAndEpoch completeMigration();
    void abortMigration();
}
