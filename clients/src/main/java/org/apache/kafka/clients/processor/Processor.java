package org.apache.kafka.clients.processor;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K, V>  {

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
  void init(KStreamContext context);
=======
  public interface ProcessorContext {

    void send(String topic, Object key, Object value);

    void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

    void schedule(long timestamp);

    void commit();

    String topic();

    int partition();

    long offset();

    long timestamp();

    KStreamContext kstreamContext();

  }

  void init(ProcessorContext context);
>>>>>>> new api model
=======
  void init(KStreamContext context);
>>>>>>> removed ProcessorContext
=======
  void init(ProcessorContext context);
>>>>>>> removing io.confluent imports: wip

  void process(K key, V value);

  void punctuate(long streamTime);

  void close();
}
