package io.confluent.streaming.internal;

<<<<<<< HEAD
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yasuhiro on 6/17/15.
 */
public class KStreamSource<K, V> extends KStreamImpl<K, V> {

  private Deserializer<K> keyDeserializer;
  private Deserializer<V> valueDeserializer;

<<<<<<< HEAD
  String[] topics;

  public KStreamSource(String[] topics, KStreamTopology topology) {
    this(topics, null, null, topology);
  }

  public KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamTopology topology) {
    super(topology);
=======
  final String[] topics;

  @SuppressWarnings("unchecked")
  KStreamSource(String[] topics, KStreamInitializer initializer) {
    this(topics, (Deserializer<K>) initializer.keyDeserializer(), (Deserializer<V>) initializer.valueDeserializer(), initializer);
  }

  KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamInitializer initializer) {
    super(initializer);
>>>>>>> new api model
    this.topics = topics;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    if (keyDeserializer == null) keyDeserializer = (Deserializer<K>) context.keyDeserializer();
    if (valueDeserializer == null) valueDeserializer = (Deserializer<V>) context.valueDeserializer();

    super.bind(context, metadata);
  }

  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized(this) {
      // KStream needs to forward the topic name since it is directly from the Kafka source
      forward(key, value, timestamp);
    }
  }

  public Deserializer<K> keyDeserializer() {
    return keyDeserializer;
  }

  public Deserializer<V> valueDeserializer() {
    return valueDeserializer;
  }

  public Set<String> topics() {
    return new HashSet<>(Arrays.asList(topics));
  }

}
