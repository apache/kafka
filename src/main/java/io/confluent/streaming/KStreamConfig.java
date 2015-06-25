package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public interface KStreamConfig {

  void setStreamSynchronizerFactory(StreamSynchronizerFactory streamSynchronizerFactory);

  void addTopicToCopartitioningGroup(String topic, String groupName);

}
