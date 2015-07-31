package io.confluent.streaming.internal;

<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
=======
import io.confluent.streaming.KStreamTopology;
>>>>>>> wip
import io.confluent.streaming.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMapValues<K, V, V1> extends KStreamImpl<K, V> {

  private final ValueMapper<V, V1> mapper;

<<<<<<< HEAD
<<<<<<< HEAD
  KStreamMapValues(ValueMapper<V, V1> mapper, KStreamTopology topology) {
    super(topology);
=======
  KStreamMapValues(ValueMapper<V, V1> mapper, KStreamInitializer initializer) {
=======
  KStreamMapValues(ValueMapper<V, V1> mapper, KStreamTopology initializer) {
>>>>>>> wip
    super(initializer);
>>>>>>> new api model
    this.mapper = mapper;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized (this) {
      V newValue = mapper.apply((V1)value);
      forward(key, newValue, timestamp);
    }
  }

}
