package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamInitializer;
import io.confluent.streaming.StreamingConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamInitializerImpl implements KStreamInitializer {

  private final Serializer<?> keySerializer;
  private final Serializer<?> valueSerializer;
  private final Deserializer<?> keyDeserializer;
  private final Deserializer<?> valueDeserializer;
  private final ArrayList<KStreamSource<?, ?>> streams = new ArrayList<>();

  KStreamInitializerImpl(StreamingConfig streamingConfig) {
    this(
      streamingConfig.keySerializer(),
      streamingConfig.valueSerializer(),
      streamingConfig.keyDeserializer(),
      streamingConfig.valueDeserializer()
    );
  }

  KStreamInitializerImpl(Serializer<?> keySerializer,Serializer<?> valueSerializer, Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  public Serializer<?> keySerializer() {
    return keySerializer;
  }

  @Override
  public Serializer<?> valueSerializer() {
    return valueSerializer;
  }

  @Override
  public Deserializer<?> keyDeserializer() {
    return keyDeserializer;
  }

  @Override
  public Deserializer<?> valueDeserializer() {
    return valueDeserializer;
  }

  @Override
  public KStream<?, ?> from(String... topics) {
    return from(this.keyDeserializer(), this.valueDeserializer(), topics);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {

    KStreamSource<K, V> stream = new KStreamSource<>(topics, keyDeserializer, valDeserializer, this);
    streams.add(stream);
    return stream;
  }

  Collection<KStreamSource<?, ?>> sourceStreams() {
    return Collections.unmodifiableCollection(streams);
  }

}
