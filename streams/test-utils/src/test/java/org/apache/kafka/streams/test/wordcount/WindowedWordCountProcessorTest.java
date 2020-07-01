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
package org.apache.kafka.streams.test.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

public class WindowedWordCountProcessorTest {
    @Test
    public void shouldWorkWithInMemoryStore() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final WindowStore<String, Integer> store =
            Stores.windowStoreBuilder(Stores.inMemoryWindowStore("WindowedCounts",
                                                                 Duration.ofDays(24),
                                                                 Duration.ofMillis(100),
                                                                 false),
                                      Serdes.String(),
                                      Serdes.Integer())
                  .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                  .withCachingDisabled() // Caching is not supported by MockProcessorContext.
                  .build();
        store.init(context, store);
        context.register(store, null);

        // Create and initialize the processor under test
        final Processor<String, String> processor = new WindowedWordCountProcessorSupplier().get();
        processor.init(context);

        // send a record to the processor
        context.setTimestamp(101);
        processor.process("key", "alpha beta gamma alpha");

        // send a record to the processor in a new window
        context.setTimestamp(221);
        processor.process("key", "gamma delta");

        // note that the processor does not forward during process()
        assertThat(context.forwarded().isEmpty(), is(true));

        // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(1_000L);

        // finally, we can verify the output.
        final Iterator<MockProcessorContext.CapturedForward> capturedForwards = context.forwarded().iterator();
        assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[alpha@100/200]", "2")));
        assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[beta@100/200]", "1")));
        assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[gamma@100/200]", "1")));
        assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[delta@200/300]", "1")));
        assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[gamma@200/300]", "1")));
        assertThat(capturedForwards.hasNext(), is(false));
    }

    @Test
    public void shouldWorkWithPersistentStore() throws IOException {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final File stateDir = TestUtils.tempDirectory();

        try {
            final MockProcessorContext context = new MockProcessorContext(
                properties,
                new TaskId(0, 0),
                stateDir
            );

            // Create, initialize, and register the state store.
            final WindowStore<String, Integer> store =
                Stores.windowStoreBuilder(Stores.persistentWindowStore("WindowedCounts",
                                                                       Duration.ofDays(24),
                                                                       Duration.ofMillis(100),
                                                                       false),
                                          Serdes.String(),
                                          Serdes.Integer())
                      .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                      .withCachingDisabled() // Caching is not supported by MockProcessorContext.
                      .build();
            store.init(context, store);
            context.register(store, null);

            // Create and initialize the processor under test
            final Processor<String, String> processor = new WindowedWordCountProcessorSupplier().get();
            processor.init(context);

            // send a record to the processor
            context.setTimestamp(101);
            processor.process("key", "alpha beta gamma alpha");

            // send a record to the processor in a new window
            context.setTimestamp(221);
            processor.process("key", "gamma delta");

            // note that the processor does not forward during process()
            assertThat(context.forwarded().isEmpty(), is(true));

            // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
            context.scheduledPunctuators().get(0).getPunctuator().punctuate(1_000L);

            // finally, we can verify the output.
            final Iterator<MockProcessorContext.CapturedForward> capturedForwards = context.forwarded().iterator();
            assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[alpha@100/200]", "2")));
            assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[beta@100/200]", "1")));
            assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[delta@200/300]", "1")));
            assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[gamma@100/200]", "1")));
            assertThat(capturedForwards.next().keyValue(), is(new KeyValue<>("[gamma@200/300]", "1")));
            assertThat(capturedForwards.hasNext(), is(false));
        } finally {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldFailWithLogging() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final WindowStore<String, Integer> store =
            Stores.windowStoreBuilder(Stores.inMemoryWindowStore("WindowedCounts",
                                                                 Duration.ofDays(24),
                                                                 Duration.ofMillis(100),
                                                                 false),
                                      Serdes.String(),
                                      Serdes.Integer())
                  .withLoggingEnabled(new HashMap<>()) // Changelog is not supported by MockProcessorContext.
                  .withCachingDisabled() // Caching is not supported by MockProcessorContext.
                  .build();
        assertThrows(IllegalArgumentException.class, () -> store.init(context, store));
    }

    @Test
    public void shouldFailWithCaching() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final WindowStore<String, Integer> store =
            Stores.windowStoreBuilder(Stores.inMemoryWindowStore("WindowedCounts",
                                                                 Duration.ofDays(24),
                                                                 Duration.ofMillis(100),
                                                                 false),
                                      Serdes.String(),
                                      Serdes.Integer())
                  .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                  .withCachingEnabled() // Caching is not supported by MockProcessorContext.
                  .build();

        assertThrows(IllegalArgumentException.class, () -> store.init(context, store));
    }
}
