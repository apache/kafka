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
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

class KStreamMapToRecordValue<K, V> implements ProcessorSupplier<K, V> {

    public KStreamMapToRecordValue() {
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamMapProcessor();
    }

    private class KStreamMapProcessor implements Processor<K, V> {
        ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(final K readOnlyKey, final V value) {
            final RecordValue<V> newValue = new RecordValue<>(
                value,
                context.headers(),
                context.timestamp());
            context.forward(readOnlyKey, newValue);
        }

        @Override
        public void close() {

        }
    }
}
