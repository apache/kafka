package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;

/**
 * Listener receives notification from an Online Partition.
 *
 * A listener can be (re-)registered to an Online partition only. The listener
 * is notified as long as the partition remains Online. When the partition fails
 * or is deleted, respectively `onFailed` or `onDeleted` are called once. No further
 * notifications are sent after this point on.
 *
 * Note that the callbacks are executed in the thread that triggers the change
 * AND that locks may be held during their execution. They are meant to be used
 * as notification mechanism only.
 */
public interface PartitionListener {
    /**
     * Called when the Log increments its high watermark.
     *
     * @param partition The topic partition for which the high watermark was updated.
     * @param offset    The new high watermark offset.
     */
    default void onHighWatermarkUpdated(TopicPartition partition, long offset) {}

    /**
     * Called when the Partition (or replica) on this broker has a failure (e.g. goes offline).
     *
     * @param partition The topic partition that failed.
     */
    default void onFailed(TopicPartition partition) {}

    /**
     * Called when the Partition (or replica) on this broker is deleted. Note that it does not mean
     * that the partition was deleted but only that this broker does not host a replica of it any more.
     *
     * @param partition The topic partition that was deleted from this broker.
     */
    default void onDeleted(TopicPartition partition) {}

    /**
     * Called when the Partition on this broker is transitioned to follower.
     *
     * @param partition The topic partition that transitioned to follower role.
     */
    default void onBecomingFollower(TopicPartition partition) {}
}
