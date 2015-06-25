package io.confluent.streaming.internal;

import io.confluent.streaming.CopartitioningGroup;
import io.confluent.streaming.KStreamConfig;
import io.confluent.streaming.StreamSynchronizer;
import io.confluent.streaming.StreamSynchronizerFactory;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamConfigImpl implements KStreamConfig {

  private StreamSynchronizerFactory streamSynchronizerFactory = null;

  private final HashMap<String, String> topicCopartitioningGroupMap = new HashMap<String, String>();
  private final HashMap<String, CopartitioningGroup> copartitioningGroups = new HashMap<String, CopartitioningGroup>();

  KStreamConfigImpl(/* TODO: pass in a consumer */) {

  }

  public void setStreamSynchronizerFactory(StreamSynchronizerFactory streamSynchronizerFactory) {
    synchronized (this) {
      if (streamSynchronizerFactory == null)
        throw new IllegalArgumentException("null StreamSynchronizerFactory");
      if (this.streamSynchronizerFactory != null)
        throw new IllegalStateException("default StreamSynchronizerFactory was already set");

      this.streamSynchronizerFactory = streamSynchronizerFactory;
    }
  }

  public void addTopicToCopartitioningGroup(String topic, String groupName) {
    synchronized (this) {
      if (topicCopartitioningGroupMap.get(topic) != null)
        throw new IllegalStateException("the topic was already added to the CopartitioningGroup: topic="+ topic);

      // TODO: use a consumer to get partition info from Kafka and set them in PartitioningInfo below
      int numPartitions = 1;

      CopartitioningGroup group = copartitioningGroups.get(groupName);

      if (group == null) {
        group = new CopartitioningGroup(groupName, numPartitions);
        copartitioningGroups.put(groupName, group);
      }
      else {
        if (group.numPartitions != numPartitions)
          throw new IllegalStateException("incompatible number of partitions");
      }
    }
  }

  Map<String, PartitioningInfo> getPartitioningInfos() {
    HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<String, PartitioningInfo>();

    for (Map.Entry<String, CopartitioningGroup> entry : copartitioningGroups.entrySet()) {
      String topic = entry.getKey();
      CopartitioningGroup group = entry.getValue();
      PartitioningInfo info = new PartitioningInfo(group);
      partitioningInfos.put(topic, info);
    }

    return partitioningInfos;
  }

  <K, V> Map<TopicPartition, StreamSynchronizer<K, V>> getStreamSynchronizers(RegulatedConsumer<K, V> consumer,
                                                                              Set<TopicPartition> partitions) {
    HashMap<TopicPartition, StreamSynchronizer<K, V>> streamSynchronizers = new HashMap();
    for (TopicPartition partition : partitions) {
      streamSynchronizers.put(partition, streamSynchronizerFactory.create(topicCopartitioningGroupMap.get(partition.topic()), consumer));
    }
    return streamSynchronizers;
  }

}
