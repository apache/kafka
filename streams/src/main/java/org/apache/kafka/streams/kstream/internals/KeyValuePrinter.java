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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.io.PrintWriter;


class KeyValuePrinter<K, V> implements ProcessorSupplier<K, V> {
    private final PrintWriter printWriter;
    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    private String streamName;

    KeyValuePrinter(PrintWriter printWriter, Serde<?> keySerde, Serde<?> valueSerde, String streamName) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.streamName = streamName;
        this.printWriter = printWriter;
    }

    KeyValuePrinter(PrintWriter printWriter, String streamName) {
        this(printWriter, null, null, streamName);
    }

    KeyValuePrinter(Serde<?> keySerde, Serde<?> valueSerde, String streamName) {
        this(null, keySerde, valueSerde, streamName);
    }

    @Override
    public Processor<K, V> get() {
        return new KeyValuePrinterProcessor(this.printWriter, this.keySerde, this.valueSerde, this.streamName);
    }

    private class KeyValuePrinterProcessor extends AbstractProcessor<K, V> {
        private final PrintWriter printWriter;
        private Serde<?> keySerde;
        private Serde<?> valueSerde;
        private ProcessorContext processorContext;
        private String streamName;

        private KeyValuePrinterProcessor(PrintWriter printWriter, Serde<?> keySerde, Serde<?> valueSerde, String streamName) {
            this.printWriter = printWriter;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.streamName = streamName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.processorContext = context;

            if (this.keySerde == null) {
                keySerde = this.processorContext.keySerde();
            }

            if (this.valueSerde == null) {
                valueSerde = this.processorContext.valueSerde();
            }
        }

        @Override
        public void process(K key, V value) {
            K keyToPrint = (K) maybeDeserialize(key, keySerde.deserializer());
            V valueToPrint = (V) maybeDeserialize(value, valueSerde.deserializer());

            println("[" + this.streamName + "]: " + keyToPrint + " , " + valueToPrint);

            this.processorContext.forward(key, value);
        }

        private void println(String str) {
            if (printWriter == null)
                System.out.println(str);
            else
                printWriter.println(str);
        }

        private Object maybeDeserialize(Object receivedElement, Deserializer<?> deserializer) {
            if (receivedElement == null) {
                return null;
            }

            if (receivedElement instanceof byte[]) {
                return deserializer.deserialize(this.processorContext.topic(), (byte[]) receivedElement);
            }

            return receivedElement;
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
