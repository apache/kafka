package org.apache.kafka.storage.internals.log;

public interface AlterPartitionListener {
    void markIsrExpand();
    void markIsrShrink();
    void markFailed();
}
