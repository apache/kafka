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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueBytesStoreWrapper;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KTableReduceTest {

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Set<String>> setSerde = new StringSetSerde();

    @Test
    public void shouldAddAndSubtract() {
        final AbstractProcessorContext context = new InternalMockProcessorContext();

        final Processor<String, Change<Set<String>>> reduceProcessor =
            new KTableReduce<String, Set<String>>(
                "myStore",
                this::unionNotNullArgs,
                this::differenceNotNullArgs
            ).get();

        final InMemoryKeyValueStore underlyingStore = new InMemoryKeyValueStore("myStore");
        final KeyValueStore<String, Set<String>> myStore = new KeyValueBytesStoreWrapper<>(underlyingStore, stringSerde, setSerde);

        context.register(myStore, null);
        reduceProcessor.init(context);
        context.setCurrentNode(new ProcessorNode<>("reduce", reduceProcessor, singleton("myStore")));

        reduceProcessor.process("A", new Change<>(singleton("a"), null));
        assertEquals(singleton("a"), myStore.get("A"));
        reduceProcessor.process("A", new Change<>(singleton("b"), singleton("a")));
        assertEquals(singleton("b"), myStore.get("A"));
        reduceProcessor.process("A", new Change<>(null, singleton("b")));
        assertEquals(emptySet(), myStore.get("A"));

    }

    private Set<String> differenceNotNullArgs(final Set<String> left, final Set<String> right) {
        assertNotNull(left);
        assertNotNull(right);

        final HashSet<String> strings = new HashSet<>(left);
        strings.removeAll(right);
        return strings;
    }

    private Set<String> unionNotNullArgs(final Set<String> left, final Set<String> right) {
        assertNotNull(left);
        assertNotNull(right);

        final HashSet<String> strings = new HashSet<>();
        strings.addAll(left);
        strings.addAll(right);
        return strings;
    }

    @Test
    public void stringSetSerdeTest() {
        final Set<String> originalSet = new HashSet<>();
        final String topicName = "serdes";

        final String string1 = "One";
        final String string2 = "Two";
        final String string3 = "Three";
        originalSet.add(string1);
        originalSet.add(string2);
        originalSet.add(string3);

        final byte[] bytes = setSerde.serializer().serialize(topicName, originalSet);
        final Set<String> newSet = setSerde.deserializer().deserialize(topicName, bytes);

        assertTrue(newSet.containsAll(originalSet));
        assertEquals(originalSet.size(), newSet.size());
    }

    private static final class StringSetSerde extends WrapperSerde<Set<String>> {
        public StringSetSerde() {
            super(new StringSetSerializer(), new StringSetDeserializer());
        }

        private static class StringSetSerializer implements Serializer<Set<String>> {
            Serde<String> stringSerde = Serdes.String();
            Serde<Integer> intSerde = Serdes.Integer();

            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
            }

            @Override
            public byte[] serialize(final String topic, final Set<String> data) {
                if (data == null)
                    return null;

                final List<byte[]> bytesList = new LinkedList<>();
                int totalBytes = 0;

                // Data is encoded as 4 bytes containing N, the length in bytes of the next string, followed by the N bytes containing that string
                for (final String string : data) {
                    final byte[] stringBytes = stringSerde.serializer().serialize(topic, string);
                    final int numStringBytes = stringBytes.length;
                    final byte[] lengthBytes = intSerde.serializer().serialize(topic, numStringBytes);

                    totalBytes += numStringBytes + 4;
                    bytesList.add(lengthBytes);
                    bytesList.add(stringBytes);
                }

                // now that we know the total number of bytes needed we can allocate an array of that size and copy the individual byte[] over
                final byte[] serializedBytes = new byte[totalBytes];

                int i = 0;
                for (final byte[] bytes : bytesList) {
                    for (final byte b : bytes) {
                        serializedBytes[i++] = b;
                    }
                }
                return serializedBytes;
            }

            @Override
            public void close() {
                // nothing to do
            }
        }

        private static class StringSetDeserializer implements Deserializer<Set<String>> {
            final Serde<String> stringSerde = Serdes.String();
            final Serde<Integer> intSerde = Serdes.Integer();

            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
            }

            @Override
            public Set<String> deserialize(final String topic, final byte[] data) {
                if (data == null)
                    return null;

                final Set<String> strings = new HashSet<>();

                int i = 0;
                while (i < data.length) {
                    final byte[] lengthBytes = getNBytes(data, i, 4);
                    final int stringLength = intSerde.deserializer().deserialize(topic, lengthBytes);
                    i += 4;

                    final byte[] stringBytes = getNBytes(data, i, stringLength);
                    final String string = stringSerde.deserializer().deserialize(topic, stringBytes);
                    i += stringLength;

                    strings.add(string);
                }
                return strings;
            }

            @Override
            public void close() {
                // nothing to do
            }

            private byte[] getNBytes(final byte[] data, int i, final int numBytes) {
                final byte[] bytes = new byte[numBytes];
                for (int b = 0; b < numBytes; ++b, ++i) {
                    bytes[b] = data[i];
                }
                return bytes;
            }
        }
    }
}
