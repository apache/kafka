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

import java.io.PrintStream;

final class PrintAction<K, V> implements ForeachAction<K, V> {

    private PrintStream printStream;
    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    private String streamName;

    PrintAction(final PrintStream printStream, final Serde<?> keySerde, final Serde<?> valueSerde, final String streamName){
        this.printStream = printStream;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.streamName = streamName;
    }

    @Override
    public void apply(final K key, final V value) {
        @SuppressWarnings("unchecked")
        K keyToPrint = (K) maybeDeserialize(key, keySerde.deserializer());
        @SuppressWarnings("unchecked")
        V valueToPrint = (V) maybeDeserialize(value, valueSerde.deserializer());
        printStream.println("[" + streamName + "]: " + keyToPrint + " , " + valueToPrint);
    }

    private Object maybeDeserialize(Object receivedElement, Deserializer<?> deserializer) {
        if (receivedElement == null) {
            return null;
        }

        if (receivedElement instanceof byte[]) {
            return deserializer.deserialize(null, (byte[]) receivedElement);
        }

        return receivedElement;
    }

}
