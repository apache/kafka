package org.apache.kafka.stream.internal;

import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.stream.topology.internal.KStreamMetadata;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Receiver {

  void bind(ProcessorContext context, KStreamMetadata metadata);

<<<<<<< HEAD
<<<<<<< HEAD
  void receive(Object key, Object value, long timestamp);

  void close();
=======
  void receive(Object key, Object value, long timestamp, long streamTime);
>>>>>>> new api model
=======
  void receive(Object key, Object value, long timestamp);
>>>>>>> remove streamTime from Receiver

  void close();

}
