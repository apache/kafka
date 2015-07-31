package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Receiver {

  void bind(KStreamContext context, KStreamMetadata metadata);

<<<<<<< HEAD
  void receive(Object key, Object value, long timestamp);

  void close();
=======
  void receive(Object key, Object value, long timestamp, long streamTime);
>>>>>>> new api model

}
