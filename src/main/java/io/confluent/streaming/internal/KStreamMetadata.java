package io.confluent.streaming.internal;

import io.confluent.streaming.SyncGroup;

import java.util.Collections;
import java.util.Map;

/**
 * Created by guozhang on 7/13/15.
 */
public class KStreamMetadata {

  public static KStreamMetadata unjoinable(SyncGroup syncGroup) {
    // TODO: how to define the topic name for flat functions?
    return new KStreamMetadata(syncGroup, Collections.singletonMap("FlatTopic", new PartitioningInfo(-1)));
  }

  public final SyncGroup syncGroup;
  public final Map<String, PartitioningInfo> topicPartitionInfos;

  KStreamMetadata(SyncGroup syncGroup, Map<String, PartitioningInfo> topicPartitionInfos) {
    this.syncGroup = syncGroup;
    this.topicPartitionInfos = topicPartitionInfos;
  }

  boolean isJoinCompatibleWith(KStreamMetadata other) {
    // the two streams should only be joinable if they only contain one topic-partition each
    if (this.topicPartitionInfos.size() != 1 || other.topicPartitionInfos.size() != 1)
      return false;
    else {
      return syncGroup == other.syncGroup
          && this.topicPartitionInfos.values().iterator().next().numPartitions >= 0
          && this.topicPartitionInfos.values().iterator().next().numPartitions == other.topicPartitionInfos.values().iterator().next().numPartitions;
    }
  }
}
