package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;

public class KeyValueTableSource<K, V> extends KTableSource<K, V> {

  public KeyValueTableSource(final String storeName, final String queryableName) {
    super(storeName, queryableName);
  }

  @Override
  public Processor<K, V> get() {
      return new KTableSourceProcessor();
  }

  private class KTableSourceProcessor extends AbstractProcessor<K, V> {

    private KeyValueStore<K, V> store;
    private TupleForwarder<K, V> tupleForwarder;
    private StreamsMetricsImpl metrics;

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
      super.init(context);
      metrics = (StreamsMetricsImpl) context.metrics();
      if (queryableName != null) {
        store = (KeyValueStore<K, V>) context.getStateStore(queryableName);
        tupleForwarder = new TupleForwarder<>(store, context, new ForwardingCacheFlushListener<K, V>(context), sendOldValues);
      }
    }

    @Override
    public void process(final K key, final V value) {
      // if the key is null, then ignore the record
      if (key == null) {
        LOG.warn(
            "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
            context().topic(), context().partition(), context().offset()
        );
        metrics.skippedRecordsSensor().record();
        return;
      }

      if (queryableName != null) {
        final V oldValue = sendOldValues ? store.get(key) : null;
        store.put(key, value);
        tupleForwarder.maybeForward(key, value, oldValue);
      } else {
        context().forward(key, new Change<>(value, null));
      }
    }
  }
}
