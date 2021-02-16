package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.RecordHeadersMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;

public class KStreamSetRecordHeaders<K, V> implements ProcessorSupplier<K, V> {

  final RecordHeadersMapper<K, V> action;

  public KStreamSetRecordHeaders(RecordHeadersMapper<K, V> action) {
    this.action = action;
  }

  @Override
  public Processor<K, V> get() {
    return new KStreamSetRecordHeadersProcessor();
  }

  private class KStreamSetRecordHeadersProcessor extends AbstractProcessor<K, V> {

    @Override
    public void process(K key, V value) {
      Headers headers = action.get(key, value);
      context().forward(key, value, To.all().withHeaders(headers));
    }

    @Override
    public void close() {
    }
  }
}
