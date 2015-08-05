package org.apache.kafka.stream.topology;

import org.apache.kafka.clients.processor.Processor;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Transformer<K1, V1, K, V> extends Processor<K, V> {

  interface Forwarder<K, V> {
    public void send(K key, V value, long timestamp);
  }

  public void forwarder(Forwarder<K1, V1> forwarder);
}
