package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamInitializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamInitializerImpl implements KStreamInitializer {

  private final ArrayList<KStreamSource<?, ?>> streams = new ArrayList<>();

  @Override
  public KStream<?, ?> from(String... topics) {
    return from(null, null, topics);
  }

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
