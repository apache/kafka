package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class RocksDBTimeOrderedKeyValueBuffer<K, V>  extends WrappedStateStore<RocksDBTimeOrderedKeyValueSegmentedBytesStore, Object, Object> implements TimeOrderedKeyValueBuffer<K, V>{

  private final Duration gracePeriod;
  private long bufferSize;
  private long minTimestamp;
  private int numRec;
  private Serde<K> keySerde;
  private FullChangeSerde<V> valueSerde;
  private String topic;

  public RocksDBTimeOrderedKeyValueBuffer(final RocksDBTimeOrderedKeyValueSegmentedBytesStore store,
                                   final Duration gracePeriod,
                                   final String topic) {
    super(store);
    this.gracePeriod = gracePeriod;
    minTimestamp = 0;
    numRec = 0;
    bufferSize = 0;
    this.topic = topic;
  }
  @SuppressWarnings("unchecked")
  @Override
  public void setSerdesIfNull(final SerdeGetter getter) {
    keySerde = keySerde == null ? (Serde<K>) getter.keySerde() : keySerde;
    valueSerde = valueSerde == null ? FullChangeSerde.wrap((Serde<V>) getter.valueSerde()) : valueSerde;
  }

  @Override
  public void evictWhile(Supplier<Boolean> predicate, Consumer<Eviction<K, V>> callback) {
    KeyValue<Bytes, byte[]> keyValue = null;

    if (predicate.get()) {
      final KeyValueIterator<Bytes, byte[]> iterator = wrapped()
          .backwardFetchAll(wrapped().observedStreamTime, wrapped().observedStreamTime - gracePeriod.toMillis());
      if (iterator.hasNext()) {
        keyValue = iterator.next();
      }
      while (keyValue != null && predicate.get() && wrapped().observedStreamTime - gracePeriod.toMillis() > minTimestamp()) {
        final K key = keySerde.deserializer().deserialize(topic, keyValue.key.get());
        final BufferValue bufferValue = BufferValue.deserialize(ByteBuffer.wrap(keyValue.value));
        final Change<V> value = valueSerde.deserializeParts(
            topic,
            new Change<>(bufferValue.newValue(), bufferValue.oldValue())
        );
        if (bufferValue.context().timestamp() != minTimestamp) {
          throw new IllegalStateException(
              "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                  bufferValue.context().timestamp() + "]"
          );
        }
        callback.accept(new Eviction<>(key, value, bufferValue.context()));
        iterator.remove();
        numRec--;
        bufferSize =- computeRecordSize(keyValue.key, bufferValue);
        if (iterator.hasNext()) {
          keyValue = iterator.next();
          if (keyValue == null) {
            minTimestamp = Long.MAX_VALUE;
          } else {
            final BufferValue nextBufferValue = BufferValue.deserialize(ByteBuffer.wrap(keyValue.value));
            minTimestamp = nextBufferValue.context().timestamp();
          }
        } else {
          keyValue = null;
          minTimestamp = Long.MAX_VALUE;
        }
      }
    }
  }


  @Override
  public Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key) {
    return null;
  }

  @Override
  public void put(long time, Record<K, Change<V>> record, ProcessorRecordContext recordContext) {
    requireNonNull(record.value(), "value cannot be null");
    requireNonNull(recordContext, "recordContext cannot be null");
    final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(topic, record.key()));
    final Change<byte[]> serialChange = valueSerde.serializeParts(topic, record.value());
    final BufferValue buffered = new BufferValue(serialChange.oldValue, serialChange.oldValue, serialChange.newValue, recordContext);
    wrapped().put(serializedKey, buffered.serialize(0).array());
    bufferSize += computeRecordSize(serializedKey, buffered);
    numRec++;
  }

  @Override
  public int numRecords() {
    return numRec;
  }

  @Override
  public long bufferSize() {
    return bufferSize;
  }

  @Override
  public long minTimestamp() {
    return minTimestamp;
  }

  private static long computeRecordSize(final Bytes key, final BufferValue value) {
    long size = 0L;
    size += 8; // buffer time
    size += key.get().length;
    if (value != null) {
      size += value.residentMemorySizeEstimate();
    }
    return size;
  }
}
