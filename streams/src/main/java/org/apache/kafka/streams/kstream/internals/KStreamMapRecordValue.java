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

import org.apache.kafka.streams.kstream.RecordValue;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

class KStreamMapRecordValue<K, V> implements ProcessorSupplier<K, V, K, RecordValue<V>> {

    public KStreamMapRecordValue() {
    }

    @Override
    public Processor<K, V, K, RecordValue<V>> get() {
        return new KStreamMapProcessor();
    }

    private class KStreamMapProcessor extends ContextualProcessor<K, V, K, RecordValue<V>> {

        @Override
        public void process(final Record<K, V> record) {
            final RecordValue<V> newValue = context().recordMetadata()
                .map(meta -> new RecordValue<>(
                    meta.topic(),
                    meta.partition(),
                    meta.offset(),
                    record.value(),
                    record.timestamp(),
                    record.headers()))
                .orElse(new RecordValue<>(record.value(), record.timestamp(), record.headers()));
            context().forward(record.withValue(newValue));
        }
    }
}
