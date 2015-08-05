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
import io.confluent.streaming.Predicate;
=======
import org.apache.kafka.stream.topology.KStreamTopology;
>>>>>>> removing io.confluent imports: wip
import org.apache.kafka.stream.topology.Predicate;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFilter<K, V> extends KStreamImpl<K, V> {

  private final Predicate<K, V> predicate;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  KStreamFilter(Predicate<K, V> predicate, KStreamTopology topology) {
    super(topology);
=======
  KStreamFilter(Predicate<K, V> predicate, KStreamInitializer initializer) {
=======
  KStreamFilter(Predicate<K, V> predicate, KStreamTopology initializer) {
>>>>>>> wip
    super(initializer);
>>>>>>> new api model
=======
  KStreamFilter(Predicate<K, V> predicate, KStreamTopology topology) {
    super(topology);
>>>>>>> fix parameter name
    this.predicate = predicate;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized(this) {
      if (predicate.apply((K)key, (V)value)) {
        forward(key, value, timestamp);
      }
    }
  }

}
