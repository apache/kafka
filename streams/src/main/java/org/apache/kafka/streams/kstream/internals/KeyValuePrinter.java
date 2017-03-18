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
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.PrintStream;

class KeyValuePrinter<K, V> extends KStreamPeek {

    private PrintStream printStream;
    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    private String streamName;

    public KeyValuePrinter(final PrintStream printStream, final Serde<?> keySerde, final Serde<?> valueSerde, final String streamName) {
        super(null);
        this.printStream = printStream;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.streamName = streamName;
    }

    @Override
    public Processor<K, V> get() {
        return new KStreamPeekProcessor();
    }

    private class KStreamPeekProcessor extends AbstractProcessor<K, V> {
        ForeachAction<K, V> action = printAction(context(), printStream, keySerde, valueSerde, streamName);

        @Override
        public void process(final K key, final V value) {
            action.apply(key, value);
            context().forward(key, value);
        }
    }

    private static <K, V> ForeachAction<K, V> printAction(final ProcessorContext context, final PrintStream printStream, final Serde<?> keySerde, final Serde<?> valueSerde, final String streamName) {
        return new ForeachAction<K, V>() {
            @Override
            public void apply(final K key, final V value) {
                K keyToPrint = (K) maybeDeserialize(key, keySerde.deserializer());
                V valueToPrint = (V) maybeDeserialize(value, valueSerde.deserializer());
                printStream.println("[" + streamName + "]: " + keyToPrint + " , " + valueToPrint);
            }

            private Object maybeDeserialize(Object receivedElement, Deserializer<?> deserializer) {
                if (receivedElement == null) {
                    return null;
                }

                if (receivedElement instanceof byte[]) {
                    return deserializer.deserialize(context.topic(), (byte[]) receivedElement);
                }

                return receivedElement;
            }
        };
    }
}
