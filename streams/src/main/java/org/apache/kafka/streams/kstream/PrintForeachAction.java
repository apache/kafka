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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

import java.io.PrintWriter;

public class PrintForeachAction<K, V> implements ForeachAction<K, V> {

    private final String streamName;

    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    
    private final PrintWriter printWriter;
    private ProcessorContext context;

    public PrintForeachAction(final PrintWriter printWriter, final Serde<?> keySerde, final Serde<?> valueSerde, final String streamName) {
        this.printWriter = printWriter;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.streamName = streamName;
    }

    public void setContext(final ProcessorContext context) {
        this.context = context;
    }
    
    public void useDefaultKeySerde() {
        this.keySerde = context.keySerde();
    }

    public void useDefaultValueSerde() { 
        this.valueSerde = context.valueSerde(); 
    }

    public Serde<?> keySerde() { 
        return keySerde; 
    }

    public Serde<?> valueSerde() { 
        return valueSerde; 
    }

    @Override
    public void apply(final K key, final V value) {
        final K deKey = (K) maybeDeserialize(key, keySerde.deserializer());
        final V deValue = (V) maybeDeserialize(value, valueSerde.deserializer());
        final String data = String.format("[%s]: %s, %s", streamName, deKey, deValue);
        if (printWriter == null) {
            System.out.println(data);
        } else {
            printWriter.println(data);
        }
    }

    private Object maybeDeserialize(final Object keyOrValue, final Deserializer<?> deserializer) {
        if (keyOrValue instanceof byte[]) {
            return deserializer.deserialize(this.context.topic(), (byte[]) keyOrValue);
        }
        return keyOrValue;
    }

    public void close() {
        if (printWriter == null) {
            System.out.flush();
        } else {
            printWriter.close();
        }
    }

}
