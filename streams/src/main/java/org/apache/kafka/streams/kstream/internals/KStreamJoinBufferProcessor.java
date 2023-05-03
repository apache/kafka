/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

public class KStreamJoinBufferProcessor<K, V> extends ContextualProcessor<K, V, K, V> {
  final Logger LOG = LoggerFactory.getLogger(KStreamKTableJoin.class);

  private final TimeOrderedKeyValueBuffer<K, V> buffer;
  private Sensor droppedRecordsSensor;
  protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
  private InternalProcessorContext<K, Change<V>> internalProcessorContext;
  private final Duration gracePeriod;


  public KStreamJoinBufferProcessor(final TimeOrderedKeyValueBuffer<K, V> buffer,
                                    final Duration gracePeriod) {
    this.buffer = buffer;
    this.gracePeriod = gracePeriod;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(final ProcessorContext<K, V> context) {
    super.init(context);
    internalProcessorContext = (InternalProcessorContext<K, Change<V>>) context();
    final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
    droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
    buffer.setSerdesIfNull(new SerdeGetter(context));
    buffer.init((org.apache.kafka.streams.processor.StateStoreContext) context(), (StateStore) null);
  }
  /**
   * Process the record. Note that record metadata is undefined in cases such as a forward call from a punctuator.
   *
   * @param record the record to process
   */
  @Override
  public void process(Record<K, V> record) {
    if(record.key() == null) {
      LOG.warn(
          "Skipping record due to null join key or value. "
              + "topic=[{}] partition=[{}] offset=[{}]",
          internalProcessorContext.recordContext().topic(), internalProcessorContext.recordContext().partition(), internalProcessorContext.recordContext().offset());

      return;
    }
    updateObservedStreamTime(record.timestamp());
    final long deadline = observedStreamTime - gracePeriod.toMillis();
    if(record.timestamp() < deadline) {
      droppedRecordsSensor.record();
    } else {
      final Change<V> change = new Change<>(record.value(), record.value());
      final Record<K, Change<V>> r = new Record<>(record.key(), change, record.timestamp());
      buffer.put(observedStreamTime, r, internalProcessorContext.recordContext());
      buffer.evictWhile(() -> buffer.minTimestamp() <= deadline, this::emit);
    }
  }

  protected void updateObservedStreamTime(final long timestamp) {
    observedStreamTime = Math.max(observedStreamTime, timestamp);
  }

  private void emit(final TimeOrderedKeyValueBuffer.Eviction<K, V> toEmit) {
    final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
    internalProcessorContext.setRecordContext(toEmit.recordContext());
    final Record<K, V> r = new Record<>(toEmit.key(), toEmit.value().newValue, toEmit.recordContext().timestamp())
        .withHeaders(toEmit.recordContext().headers());
    try {
      context().forward(r);
    } finally {
      internalProcessorContext.setRecordContext(prevRecordContext);
    }
  }
}
