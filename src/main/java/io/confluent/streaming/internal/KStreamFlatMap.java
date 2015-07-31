package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
<<<<<<< HEAD
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.KeyValue;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMap<K, V, K1, V1> extends KStreamImpl<K, V> {

  private final KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper;

<<<<<<< HEAD
  KStreamFlatMap(KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper, KStreamTopology topology) {
    super(topology);
=======
  KStreamFlatMap(KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper, KStreamInitializer initializer) {
    super(initializer);
>>>>>>> new api model
    this.mapper = mapper;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    super.bind(context, KStreamMetadata.unjoinable());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized(this) {
      KeyValue<K, ? extends Iterable<V>> newPair = mapper.apply((K1)key, (V1)value);
      for (V v : newPair.value) {
        forward(newPair.key, v, timestamp);
      }
    }
  }

}
