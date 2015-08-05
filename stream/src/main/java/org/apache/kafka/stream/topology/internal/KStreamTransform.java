package org.apache.kafka.stream.topology.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamTopology;
import io.confluent.streaming.Transformer;
import org.apache.kafka.stream.topology.Transformer;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamTransform<K, V, K1, V1> extends KStreamImpl<K, V> implements Transformer.Forwarder<K, V> {

  private final Transformer<K, V, K1, V1> transformer;

  KStreamTransform(Transformer<K, V, K1, V1> transformer, KStreamTopology topology) {
    super(topology);
    this.transformer = transformer;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    transformer.init(context);
    transformer.forwarder(this);

    super.bind(context, KStreamMetadata.unjoinable());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized (this) {
      transformer.process((K1) key, (V1) value);
    }
  }

  @Override
  public void send(K key, V value, long timestamp) {
    forward(key, value, timestamp);
  }
}
