package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.RecordCollector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Created by guozhang on 7/21/15.
 */
public class ProcessorContextImpl implements Processor.ProcessorContext {

  private final KStreamContextImpl context;
  private final StreamGroup streamGroup;
  private final PunctuationScheduler scheduler;

  public ProcessorContextImpl(KStreamContextImpl context,
                              StreamGroup streamGroup,
                              PunctuationScheduler scheduler) {

    this.context = context;
    this.scheduler = scheduler;
    this.streamGroup = streamGroup;
  }

  @Override
  public void send(String topic, Object key, Object value) {
    this.context.recordCollector().send(new ProducerRecord(topic, key, value));
  }

  @Override
  public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
    if (keySerializer == null || valSerializer == null)
      throw new IllegalStateException("key and value serializers must be specified");

    final RecordCollector<Object, Object> collector = new RecordCollectors.SerializingRecordCollector(
        context.simpleRecordCollector(), keySerializer, valSerializer);

    collector.send(new ProducerRecord(topic, key, value));
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
