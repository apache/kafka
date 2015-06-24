package io.confluent.streaming.internal;

import io.confluent.streaming.CopartitioningGroup;
import io.confluent.streaming.CopartitioningGroupFactory;
import io.confluent.streaming.KStreamConfig;
import io.confluent.streaming.StreamSynchronizer;

import java.util.HashMap;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamConfigImpl implements KStreamConfig {

  private StreamSynchronizer streamSynchronizer = null;
  private CopartitioningGroupFactory copartitioningGroupFactory = null;

  private final HashMap<String, CopartitioningGroup> copartitioningGroups = new HashMap<String, CopartitioningGroup>();
  private final HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<String, PartitioningInfo>();

  KStreamConfigImpl(/* TODO: pass in a consumer */) {

  }

  public void setDefaultStreamSynchronizer(StreamSynchronizer streamSynchronizer) {
    synchronized (this) {
      if (streamSynchronizer == null)
        throw new IllegalArgumentException("null StreamSynchronizer");
      if (this.streamSynchronizer != null)
        throw new IllegalStateException("default StreamSynchronizer was already set");

      this.streamSynchronizer = streamSynchronizer;
    }
  }

  public void setCopartitioningGroupFactory(CopartitioningGroupFactory copartitioningGroupFactory) {
    synchronized (this) {
      if (copartitioningGroupFactory == null)
        throw new IllegalArgumentException("null CoPartitioningGroupFactory");
      if (this.copartitioningGroupFactory != null)
        throw new IllegalStateException("CoPartitioningGroupFactory was already set");

      this.copartitioningGroupFactory = copartitioningGroupFactory;
    }
  }

  public void addTopicToCopartitioningGroup(String topic, String groupName) {
    synchronized (this) {
      PartitioningInfo info = partitioningInfos.get(topic);
      if (info != null)
        throw new IllegalStateException("the topic was already added to the CopartitioningGroup: topic="+ topic);

      // TODO: use a consumer to get partition info from Kafka and set them in PartitioningInfo below
      int numPartitions = 1;

      CopartitioningGroup group = copartitioningGroups.get(groupName);

      if (group == null) {
        group = copartitioningGroupFactory.create(groupName, numPartitions);
        copartitioningGroups.put(groupName, group);
      }
      else {
        if (group.numPartitions != numPartitions)
          throw new IllegalStateException("incompatible number of partitions");
      }

      info = new PartitioningInfo(group, numPartitions);
      partitioningInfos.put(topic, info);
    }
  }

  HashMap<String, PartitioningInfo> getPartitioningInfos() {
    return partitioningInfos;
  }

}
