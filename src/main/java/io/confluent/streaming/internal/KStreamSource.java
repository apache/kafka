package io.confluent.streaming.internal;

<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamTopology;
<<<<<<< HEAD
=======
=======
import io.confluent.streaming.KStreamContext;
>>>>>>> wip
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
=======
>>>>>>> wip
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
<<<<<<< HEAD
<<<<<<< HEAD
  String[] topics;

  public KStreamSource(String[] topics, KStreamTopology topology) {
    this(topics, null, null, topology);
  }
=======
  private Deserializer<K> keyDeserializer;
  private Deserializer<V> valueDeserializer;
>>>>>>> wip

  public KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamTopology topology) {
    super(topology);
=======
  final String[] topics;
=======
  String[] topics;
>>>>>>> wip

  public KStreamSource(String[] topics, KStreamTopology initializer) {
    this(topics, null, null, initializer);
  }

  public KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamTopology initializer) {
    super(initializer);
>>>>>>> new api model
    this.topics = topics;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @SuppressWarnings("unchecked")
<<<<<<< HEAD
=======
  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    if (keyDeserializer == null) keyDeserializer = (Deserializer<K>) context.keyDeserializer();
    if (valueDeserializer == null) valueDeserializer = (Deserializer<V>) context.valueDeserializer();

    super.bind(context, metadata);
  }

>>>>>>> wip
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

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> wip
  public Set<String> topics() {
    return new HashSet<>(Arrays.asList(topics));
  }

<<<<<<< HEAD
=======
>>>>>>> wip
=======
>>>>>>> wip
}
