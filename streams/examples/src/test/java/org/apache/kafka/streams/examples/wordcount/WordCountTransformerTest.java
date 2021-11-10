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
package org.apache.kafka.streams.examples.wordcount;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demonstrate the use of {@link MockProcessorContext} for testing the {@link Transformer} in the {@link WordCountTransformerDemo}.
 */
public class WordCountTransformerTest {
    @Test
    public void test() {
        final MockProcessorContext<String, String> context = new MockProcessorContext<>();

        // Create and initialize the transformer under test; including its provided store
        final WordCountTransformerDemo.MyTransformerSupplier supplier = new WordCountTransformerDemo.MyTransformerSupplier();
        for (final StoreBuilder<?> storeBuilder : supplier.stores()) {
            final StateStore store = storeBuilder
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                // Caching is disabled by default, but FYI: caching is also not supported by MockProcessorContext.
                .build();
            store.init(context.getStateStoreContext(), store);
            context.getStateStoreContext().register(store, null);
        }
        final Transformer<String, String, KeyValue<String, String>> transformer = supplier.get();
        transformer.init(new org.apache.kafka.streams.processor.MockProcessorContext() {
            @Override
            public <S extends StateStore> S getStateStore(final String name) {
                return context.getStateStore(name);
            }

            @Override
            public <K, V> void forward(final K key, final V value) {
                context.forward(new Record<>((String) key, (String) value, 0L));
            }

            @Override
            public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
                return context.schedule(interval, type, callback);
            }
        });

        // send a record to the transformer
        transformer.transform("key", "alpha beta\tgamma\n\talpha");

        // note that the transformer does not forward during transform()
        assertTrue(context.forwarded().isEmpty());

        // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

        // finally, we can verify the output.
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> capturedForwards = context.forwarded();
        final List<MockProcessorContext.CapturedForward<? extends String, ? extends String>> expected = asList(
                new MockProcessorContext.CapturedForward<>(new Record<>("alpha", "2", 0L)),
                new MockProcessorContext.CapturedForward<>(new Record<>("beta", "1", 0L)),
                new MockProcessorContext.CapturedForward<>(new Record<>("gamma", "1", 0L))
        );
        assertThat(capturedForwards, is(expected));
    }
}
