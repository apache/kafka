package org.apache.kafka.stream.internal;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class PartitioningInfo {

  public final int numPartitions;

  public PartitioningInfo(int numPartitions) {
    this.numPartitions = numPartitions;
  }
}
