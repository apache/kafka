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

import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.PrintForeachAction;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

public class KStreamPrint<K, V> implements ProcessorSupplier<K, V> {

    private final Serde<?> keySerde;
    private final Serde<?> valueSerde;
    private final ForeachAction<K, V> action;
    
    public KStreamPrint(final ForeachAction<K, V> action, final Serde<?> keySerde, final Serde<?> valueSerde) {
        this.action = action;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamPrintProcessor(keySerde, valueSerde);
    }

    private class KStreamPrintProcessor extends AbstractProcessor<K, V> {
        
        private Serde<?> keySerde;
        private Serde<?> valueSerde;
        private ProcessorContext context;
        
        public KStreamPrintProcessor(final Serde<?> keySerde, final Serde<?> valueSerde) {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            if (keySerde == null) {
                this.keySerde = context.keySerde();
            }
            if (valueSerde == null) {
                this.valueSerde = context.valueSerde();
            }
        }

        @Override
        public void process(final K key, final V value) {
            final K deKey = (K) maybeDeserialize(key, keySerde.deserializer());
            final V deValue = (V) maybeDeserialize(value, valueSerde.deserializer());
            action.apply(deKey, deValue);
        }

        private Object maybeDeserialize(final Object keyOrValue, final Deserializer<?> deserializer) {
            if (keyOrValue instanceof byte[]) {
                return deserializer.deserialize(this.context.topic(), (byte[]) keyOrValue);
            }
            return keyOrValue;
        }
        
        @Override
        public void close() {
            if (action instanceof PrintForeachAction) {
                ((PrintForeachAction) action).close();
            }
        }
    }

}
