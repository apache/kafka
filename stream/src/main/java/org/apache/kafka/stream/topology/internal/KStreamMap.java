package org.apache.kafka.stream.topology.internal;

<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamContext;
<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamInitializer;
>>>>>>> new api model
=======
import io.confluent.streaming.KStreamTopology;
>>>>>>> wip
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.KeyValue;
=======
import org.apache.kafka.clients.processor.ProcessorContext;
=======
import org.apache.kafka.stream.KStreamContext;
>>>>>>> removing io.confluent imports: wip
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.KeyValue;
>>>>>>> removing io.confluent imports: wip
import org.apache.kafka.stream.topology.KeyValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMap<K, V, K1, V1> extends KStreamImpl<K, V> {

  private final KeyValueMapper<K, V, K1, V1> mapper;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, KStreamTopology topology) {
    super(topology);
=======
  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, KStreamInitializer initializer) {
=======
  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, KStreamTopology initializer) {
>>>>>>> wip
    super(initializer);
>>>>>>> new api model
=======
  KStreamMap(KeyValueMapper<K, V, K1, V1> mapper, KStreamTopology topology) {
    super(topology);
>>>>>>> fix parameter name
    this.mapper = mapper;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    super.bind(context, KStreamMetadata.unjoinable());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized (this) {
      KeyValue<K, V> newPair = mapper.apply((K1)key, (V1)value);
      forward(newPair.key, newPair.value, timestamp);
    }
  }

}
