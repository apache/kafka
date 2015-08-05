package org.apache.kafka.stream.internal;

import org.apache.kafka.clients.processor.Processor;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.stream.topology.internal.KStreamMetadata;

/**
 * Created by yasuhiro on 7/31/15.
 */
public class ProcessorNode<K, V> implements Receiver {

  private final Processor<K, V> processor;
  private ProcessorContext context;

  public ProcessorNode(Processor<K, V> processor) {
    this.processor = processor;
  }

  @Override
  public void bind(ProcessorContext context, KStreamMetadata metadata) {
    if (this.context != null) throw new IllegalStateException("kstream topology is already bound");

    this.context = context;
<<<<<<< HEAD
<<<<<<< HEAD
    processor.init(context);
  }
  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
<<<<<<< HEAD
=======
    processor.init(new ProcessorContextImpl(context, context.getPunctuationScheduler(processor)));
=======
    processor.init(context);
>>>>>>> removed ProcessorContext
  }
  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
>>>>>>> wip
=======
>>>>>>> remove streamTime from Receiver
    processor.process((K) key, (V) value);
  }
  @Override
  public void close() {
    processor.close();
  }

}
