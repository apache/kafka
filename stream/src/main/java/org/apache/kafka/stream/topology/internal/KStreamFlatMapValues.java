package org.apache.kafka.stream.topology.internal;

<<<<<<< HEAD
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
=======
import org.apache.kafka.stream.topology.KStreamTopology;
>>>>>>> removing io.confluent imports: wip
import org.apache.kafka.stream.topology.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMapValues<K, V, V1> extends KStreamImpl<K, V> {

  private final ValueMapper<? extends Iterable<V>, V1> mapper;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  KStreamFlatMapValues(ValueMapper<? extends Iterable<V>, V1> mapper, KStreamTopology topology) {
    super(topology);
=======
  KStreamFlatMapValues(ValueMapper<? extends Iterable<V>, V1> mapper, KStreamInitializer initializer) {
=======
  KStreamFlatMapValues(ValueMapper<? extends Iterable<V>, V1> mapper, KStreamTopology initializer) {
>>>>>>> wip
    super(initializer);
>>>>>>> new api model
=======
  KStreamFlatMapValues(ValueMapper<? extends Iterable<V>, V1> mapper, KStreamTopology topology) {
    super(topology);
>>>>>>> fix parameter name
    this.mapper = mapper;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized(this) {
      Iterable<V> newValues = mapper.apply((V1)value);
      for (V v : newValues) {
        forward(key, v, timestamp);
      }
    }
  }

}
