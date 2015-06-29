package io.confluent.streaming.internal;

import io.confluent.streaming.SyncGroup;

/**
 * Created by yasuhiro on 6/19/15.
 */
class PartitioningInfo {

  public static PartitioningInfo missing = new PartitioningInfo(null, -1);

  public final SyncGroup syncGroup;
  public final int numPartitions;

  PartitioningInfo(SyncGroup syncGroup, int numPartitions) {
    if (syncGroup == null) throw new NullPointerException();

    this.syncGroup = syncGroup;
    this.numPartitions = numPartitions;
  }

  boolean isJoinCompatibleWith(PartitioningInfo other) {
    return syncGroup == other.syncGroup && numPartitions >= 0 && numPartitions == other.numPartitions;
  }

}
