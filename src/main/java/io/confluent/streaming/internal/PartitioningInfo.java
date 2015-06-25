package io.confluent.streaming.internal;

import io.confluent.streaming.CopartitioningGroup;

/**
 * Created by yasuhiro on 6/19/15.
 */
class PartitioningInfo {

  public static PartitioningInfo missing = new PartitioningInfo(null, 0);

  private final CopartitioningGroup copartitioningGroup;
  private final int numPartitions;

  PartitioningInfo(int numPartitions) {
    this(null, numPartitions);
  }

  PartitioningInfo(CopartitioningGroup copartitioningGroup) {
    this(copartitioningGroup, copartitioningGroup.numPartitions);
  }

  private PartitioningInfo(CopartitioningGroup copartitioningGroup, int numPartitions) {
    this.copartitioningGroup = copartitioningGroup;
    this.numPartitions = numPartitions;
  }

  boolean isJoinCompatibleWith(PartitioningInfo other) {
    if (copartitioningGroup != null) {
      return copartitioningGroup == other.copartitioningGroup;
    }
    else {
      return
        (other.copartitioningGroup == null &
          (numPartitions >= 0 || numPartitions == other.numPartitions));
    }
  }

}
