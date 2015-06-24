package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public interface KStreamConfig {

  void setDefaultStreamSynchronizer(StreamSynchronizer streamSynchronizer);

  void setCopartitioningGroupFactory(CopartitioningGroupFactory copartitioningGroupFactory);

  void addTopicToCopartitioningGroup(String topic, String groupName);

}
