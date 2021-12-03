package org.apache.kafka.streams.processor.internals.namedtopology;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.internals.StreamsMetadataImpl;

public class NamedTopologyStreamsMetadataImpl extends StreamsMetadataImpl {
  private final Map<String, Collection<String>> namedTopologyToStoreNames;

  public NamedTopologyStreamsMetadataImpl(final HostInfo hostInfo,
      final Set<String> stateStoreNames,
      final Set<TopicPartition> topicPartitions,
      final Set<String> standbyStateStoreNames,
      final Set<TopicPartition> standbyTopicPartitions,
      final Map<String, Collection<String>> namedTopologyToStoreNames) {
    super(hostInfo, stateStoreNames, topicPartitions, standbyStateStoreNames,
        standbyTopicPartitions);
    this.namedTopologyToStoreNames = namedTopologyToStoreNames;
  }

  public Map<String, Collection<String>> getNamedTopologyToStoreNames() {
    return namedTopologyToStoreNames;
  }
}
