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
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Demonstrate the use of {@link MockProcessorContext} for testing the {@link Transformer} in the {@link WordCountTransformerDemo}.
 */
public class WordCountTransformerTest {
    @SuppressWarnings("deprecation") // TODO will be fixed in KAFKA-10437
    @Test
    public void test() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create and initialize the transformer under test; including its provided store
        final WordCountTransformerDemo.MyTransformerSupplier supplier = new WordCountTransformerDemo.MyTransformerSupplier();
        for (final StoreBuilder<?> storeBuilder : supplier.stores()) {
            final StateStore store = storeBuilder
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                // Caching is disabled by default, but FYI: caching is also not supported by MockProcessorContext.
                .build();
            store.init(context, store);
            context.register(store, null);
        }
        final Transformer<String, String, KeyValue<String, String>> transformer = supplier.get();
        transformer.init(context);

        // send a record to the transformer
        transformer.transform("key", "alpha beta gamma alpha");

        // note that the transformer does not forward during transform()
        assertTrue(context.forwarded().isEmpty());

        // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

        // finally, we can verify the output.
        final Iterator<MockProcessorContext.CapturedForward> capturedForwards = context.forwarded().iterator();
        assertEquals(new KeyValue<>("alpha", "2"), capturedForwards.next().keyValue());
        assertEquals(new KeyValue<>("beta", "1"), capturedForwards.next().keyValue());
        assertEquals(new KeyValue<>("gamma", "1"), capturedForwards.next().keyValue());
        assertFalse(capturedForwards.hasNext());
    }
}
