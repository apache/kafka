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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

import java.io.PrintWriter;

class KStreamPeek<K, V> implements ProcessorSupplier<K, V> {

    private boolean print;
    private boolean downStream;
    private ForeachAction<K, V> action;
    
    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    private String streamName;
    private PrintWriter printWriter;

    public KStreamPeek(final ForeachAction<K, V> action, final boolean downStream) {
        this.action = action;
        this.downStream = downStream;
        this.print = false;
    }

    public KStreamPeek(Serde<?> keySerde, Serde<?> valueSerde, String streamName) {
        this(null, keySerde, valueSerde, streamName);
    }

    public KStreamPeek(PrintWriter printWriter, String streamName) {
        this(printWriter, null, null, streamName);
    }

    public KStreamPeek(PrintWriter printWriter, Serde<?> keySerde, Serde<?> valueSerde, String streamName) {
        this.printWriter = printWriter;
        this.keySerde    = keySerde;
        this.valueSerde  = valueSerde;
        this.streamName  = streamName;
        this.print = true;
    }

    @Override
    public Processor<K, V> get() {
        if (!print) {
            return new KStreamPeekProcessor();
        } else {
            return new KStreamPeekPrinterProcessor();
        }
    }

    private class KStreamPeekProcessor extends AbstractProcessor<K, V> {
        @Override
        public void process(final K key, final V value) {
            action.apply(key, value);
            if (downStream) {
                context().forward(key, value);
            }
        }
    }

    private class KStreamPeekPrinterProcessor extends AbstractProcessor<K, V> {
        private ProcessorContext context;
        private Deserializer keyDeserializer;
        private Deserializer valueDeserializer;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            if (keySerde == null) {
                keySerde = context.keySerde();
            }
            if (valueSerde == null) {
                valueSerde = context.valueSerde();
            }
            keyDeserializer = keySerde.deserializer();
            valueDeserializer = valueSerde.deserializer();
        }
        
        @Override
        public void process(final K key, final V value) {
            K deKey   = (K) deserialize(key, keyDeserializer);
            V deValue = (V) deserialize(value, valueDeserializer);
            String data = String.format("[%s]: %s, %s", streamName, deKey, deValue);
            if (printWriter == null) {
                System.out.println(data);
            } else {
                printWriter.println(data);
            }
        }

        private Object deserialize(Object value, Deserializer<?> deserializer) {
            if (value instanceof byte[]) {
                return deserializer.deserialize(this.context.topic(), (byte[]) value);
            }
            return value;
        }

        @Override
        public void close() {
            if (printWriter == null) {
                System.out.flush();
            } else {
                printWriter.close();
            }
        }
    }
}
