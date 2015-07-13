package io.confluent.streaming.internal;

/**
 * Created by yasuhiro on 6/19/15.
 */
class PartitioningInfo {

  public final int numPartitions;

  PartitioningInfo(int numPartitions) {
    this.numPartitions = numPartitions;
  }
}
