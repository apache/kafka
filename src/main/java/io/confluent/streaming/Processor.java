package io.confluent.streaming;

import org.apache.kafka.common.serialization.Serializer;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K, V>  {

  void init(KStreamContext context);

  void process(K key, V value);

  void punctuate(long streamTime);

  void close();
}
