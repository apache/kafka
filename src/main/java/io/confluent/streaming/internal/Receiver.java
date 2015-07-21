package io.confluent.streaming.internal;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Receiver {

  void receive(Object key, Object value, long timestamp, long streamTime);

}
