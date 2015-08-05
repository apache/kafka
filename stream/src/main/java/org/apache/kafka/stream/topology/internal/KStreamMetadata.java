package org.apache.kafka.stream.topology.internal;

import org.apache.kafka.stream.internal.PartitioningInfo;
import org.apache.kafka.stream.internal.StreamGroup;

import java.util.Collections;
import java.util.Map;

/**
 * Created by guozhang on 7/13/15.
 */
public class KStreamMetadata {

  public static String UNKNOWN_TOPICNAME = "__UNKNOWN_TOPIC__";
  public static int UNKNOWN_PARTITION = -1;

  public static KStreamMetadata unjoinable() {
    return new KStreamMetadata(Collections.singletonMap(UNKNOWN_TOPICNAME, new PartitioningInfo(UNKNOWN_PARTITION)));
  }

  public StreamGroup streamGroup;
  public final Map<String, PartitioningInfo> topicPartitionInfos;

  public KStreamMetadata(Map<String, PartitioningInfo> topicPartitionInfos) {
    this.topicPartitionInfos = topicPartitionInfos;
  }

  boolean isJoinCompatibleWith(KStreamMetadata other) {
    // the two streams should only be joinable if they are inside the same sync group
    // and their contained streams all have the same number of partitions
    if (this.streamGroup != other.streamGroup)
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
