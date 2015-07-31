package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Receiver {

  void bind(KStreamContext context, KStreamMetadata metadata);

  void receive(Object key, Object value, long timestamp, long streamTime);

  void close();

}
