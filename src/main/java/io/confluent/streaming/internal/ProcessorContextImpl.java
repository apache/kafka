package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Created by guozhang on 7/21/15.
 */
public class ProcessorContextImpl implements Processor.ProcessorContext {

  private final KStreamContext context;
  private final StreamGroup streamGroup;
  private final PunctuationScheduler scheduler;

  public ProcessorContextImpl(KStreamContext context,
                              StreamGroup streamGroup,
                              PunctuationScheduler scheduler) {

    this.context = context;
    this.scheduler = scheduler;
    this.streamGroup = streamGroup;
  }

  @Override
  public String topic() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as topic() should only be called while a record is processed");

    return this.streamGroup.record().topic();
  }

  @Override
  public int partition() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as partition() should only be called while a record is processed");

    return this.streamGroup.record().partition();
  }

  @Override
  public long offset() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as offset() should only be called while a record is processed");

    return this.streamGroup.record().offset();
  }

  @Override
  public long timestamp() {
    if (this.streamGroup.record() == null)
      throw new IllegalStateException("this should not happen as timestamp() should only be called while a record is processed");

    return this.streamGroup.record().timestamp;
  }

  @Override
  public void send(String topic, Object key, Object value) {
    this.context.recordCollector().send(new ProducerRecord<>(topic, key, value));
  }

  @Override
  public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
    if (keySerializer == null || valSerializer == null)
      throw new IllegalStateException("key and value serializers must be specified");

    context.recordCollector().send(new ProducerRecord<>(topic, key, value), keySerializer, valSerializer);
  }

  @Override
  public void commit() {
    this.streamGroup.commitOffset();
  }

  @Override
  public void schedule(long timestamp) {
    scheduler.schedule(timestamp);
  }
}
