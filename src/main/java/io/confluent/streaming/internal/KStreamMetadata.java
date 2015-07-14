package io.confluent.streaming.internal;

import io.confluent.streaming.SyncGroup;

import java.util.Collections;
import java.util.Map;

/**
 * Created by guozhang on 7/13/15.
 */
public class KStreamMetadata {

  public static String UNKNOWN_TOPICNAME = "__UNKNOWN_TOPIC__";

  public static KStreamMetadata unjoinable(SyncGroup syncGroup) {
    return new KStreamMetadata(syncGroup, Collections.singletonMap(UNKNOWN_TOPICNAME, new PartitioningInfo(-1)));
  }

  public final SyncGroup syncGroup;
  public final Map<String, PartitioningInfo> topicPartitionInfos;

  KStreamMetadata(SyncGroup syncGroup, Map<String, PartitioningInfo> topicPartitionInfos) {
    this.syncGroup = syncGroup;
    this.topicPartitionInfos = topicPartitionInfos;
  }

  boolean isJoinCompatibleWith(KStreamMetadata other) {
    // the two streams should only be joinable if they are inside the same sync group
    // and their contained streams all have the same number of partitions
    if (this.syncGroup != other.syncGroup)
      return false;

    int numPartitions = -1;
    for (PartitioningInfo partitionInfo : this.topicPartitionInfos.values()) {
      if (partitionInfo.numPartitions < 0) {
        return false;
      } else if (numPartitions >= 0) {
        if (partitionInfo.numPartitions != numPartitions)
          return false;
      } else {
        numPartitions = partitionInfo.numPartitions;
      }
    }

    for (PartitioningInfo partitionInfo : other.topicPartitionInfos.values()) {
      if (partitionInfo.numPartitions != numPartitions)
        return false;
    }

    return true;
  }
}
