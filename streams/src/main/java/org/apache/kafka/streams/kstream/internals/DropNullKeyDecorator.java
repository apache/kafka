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

import static org.apache.kafka.streams.processor.internals.metrics.TaskMetrics.droppedRecordsSensor;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropNullKeyDecorator<KIn, VIn, KOut, VOut> implements
    ProcessorSupplier<KIn, VIn, KOut, VOut> {

    private static final Logger LOG = LoggerFactory.getLogger(DropNullKeyDecorator.class);
    private final ProcessorSupplier<KIn, VIn, KOut, VOut> wrappedSupplier;

    public DropNullKeyDecorator(final ProcessorSupplier<KIn, VIn, KOut, VOut> wrappedSupplier) {
        this.wrappedSupplier = wrappedSupplier;
    }

    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        final Processor<KIn, VIn, KOut, VOut> wrapped = wrappedSupplier.get();
        return new DropNullKeyValueDecoratorProcessor(wrapped);
    }

    private class DropNullKeyValueDecoratorProcessor extends
        ContextualProcessor<KIn, VIn, KOut, VOut> {

        private final Processor<KIn, VIn, KOut, VOut> wrapped;
        private Sensor droppedRecordsSensor;

        DropNullKeyValueDecoratorProcessor(final Processor<KIn, VIn, KOut, VOut> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void init(final ProcessorContext<KOut, VOut> context) {
            super.init(context);
            droppedRecordsSensor = droppedRecordsSensor(
                Thread.currentThread().getName(),
                context.taskId().toString(),
                (StreamsMetricsImpl) context.metrics());

            if (wrapped instanceof ContextualProcessor) {
                wrapped.init(context);
            }
        }

        @Override
        public void process(final Record<KIn, VIn> record) {
            if (!dropNullKey(record)) {
                wrapped.process(record);
            }
        }

        private boolean dropNullKey(final Record<KIn, VIn> record) {
            if (record.key() == null) {
                if (context().recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context().recordMetadata().get();
                    LOG.warn(
                        "Skipping record due to null key or value. "
                            + "topic=[{}] partition=[{}] offset=[{}]",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()
                    );
                } else {
                    LOG.warn(
                        "Skipping record due to null key or value. Topic, partition, and offset not known."
                    );
                }
                droppedRecordsSensor.record();
                return true;
            }
            return false;
        }
    }
}
