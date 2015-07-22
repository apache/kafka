package io.confluent.streaming;

import org.apache.kafka.common.serialization.Serializer;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K, V>  {

  public interface ProcessorContext {

    void send(String topic, Object key, Object value);

    void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

    void schedule(long timestamp);

    void commit();

    String topic();

    int partition();

    long offset();

    long timestamp();
  }

  void init(ProcessorContext context);

  void process(K key, V value);

  void punctuate(long streamTime);
}
