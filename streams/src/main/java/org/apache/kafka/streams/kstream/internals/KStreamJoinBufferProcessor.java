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
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

public class KStreamJoinBufferProcessor<K, V> extends ContextualProcessor<K, Change<V>, K, Change<V>> {

  private final TimeOrderedKeyValueBuffer<K, V> buffer;
  private Sensor droppedRecordsSensor;
  protected long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
  private InternalProcessorContext<K, Change<V>> internalProcessorContext;
  private final Duration gracePeriod;


  public KStreamJoinBufferProcessor(final TimeOrderedKeyValueBuffer<K, V> buffer,
                                    final Duration gracePeriod) {
    this.buffer = buffer;
    this.gracePeriod = gracePeriod;
    internalProcessorContext = (InternalProcessorContext<K, Change<V>>) context();
  }

  @Override
  public void init(final ProcessorContext<K, Change<V>> context) {
    super.init(context);
    final StreamsMetricsImpl metrics = (StreamsMetricsImpl) context.metrics();
    droppedRecordsSensor = droppedRecordsSensor(Thread.currentThread().getName(), context.taskId().toString(), metrics);
  }
  /**
   * Process the record. Note that record metadata is undefined in cases such as a forward call from a punctuator.
   *
   * @param record the record to process
   */
  @Override
  public void process(Record<K, Change<V>> record) {
    updateObservedStreamTime(record.timestamp());
    final long deadline = observedStreamTime + gracePeriod.toMillis();
    if(buffer.minTimestamp() <= deadline) {
      droppedRecordsSensor.record();
    } else {
      buffer.put(observedStreamTime, record, internalProcessorContext.recordContext());
      buffer.evictWhile(() -> buffer.minTimestamp() <= deadline, this::emit);
    }
  }

  protected void updateObservedStreamTime(final long timestamp) {
    observedStreamTime = Math.max(observedStreamTime, timestamp);
  }

  private void emit(final TimeOrderedKeyValueBuffer.Eviction<K, V> toEmit) {
    final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
    internalProcessorContext.setRecordContext(toEmit.recordContext());
    try {
      context().forward(toEmit.record()
          .withTimestamp(toEmit.recordContext().timestamp())
          .withHeaders(toEmit.recordContext().headers()));
    } finally {
      internalProcessorContext.setRecordContext(prevRecordContext);
    }
  }
}
