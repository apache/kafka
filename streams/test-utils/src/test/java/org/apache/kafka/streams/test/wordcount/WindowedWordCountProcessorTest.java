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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WindowedWordCountProcessorTest {
    @Test
    public void shouldWorkWithInMemoryStore() {
        final MockProcessorContext<String, String> context = new MockProcessorContext<>();

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
        store.init(context.getStateStoreContext(), store);
        context.getStateStoreContext().register(store, null);

        // Create and initialize the processor under test
        final Processor<String, String, String, String> processor = new WindowedWordCountProcessorSupplier().get();
        processor.init(context);

        // send a record to the processor
        processor.process(new Record<>("key", "alpha beta gamma alpha", 101L));

        // send a record to the processor in a new window
        processor.process(new Record<>("key", "gamma delta", 221L));

        // note that the processor does not forward during process()
        assertThat(context.forwarded().isEmpty(), is(true));

        // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(1_000L);

        // finally, we can verify the output.
        final List<CapturedForward<? extends String, ? extends String>> capturedForwards = context.forwarded();
        final List<CapturedForward<? extends String, ? extends String>> expected = asList(
            new CapturedForward<>(new Record<>("[alpha@100/200]", "2", 1_000L)),
            new CapturedForward<>(new Record<>("[beta@100/200]", "1", 1_000L)),
            new CapturedForward<>(new Record<>("[gamma@100/200]", "1", 1_000L)),
            new CapturedForward<>(new Record<>("[delta@200/300]", "1", 1_000L)),
            new CapturedForward<>(new Record<>("[gamma@200/300]", "1", 1_000L))
        );

        assertThat(capturedForwards, is(expected));

        store.close();
    }

    @Test
    public void shouldWorkWithPersistentStore() throws IOException {
        final File stateDir = TestUtils.tempDirectory();

        try {
            final MockProcessorContext<String, String> context = new MockProcessorContext<>(
                new Properties(),
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
            store.init(context.getStateStoreContext(), store);
            context.getStateStoreContext().register(store, null);

            // Create and initialize the processor under test
            final Processor<String, String, String, String> processor = new WindowedWordCountProcessorSupplier().get();
            processor.init(context);

            // send a record to the processor
            processor.process(new Record<>("key", "alpha beta gamma alpha", 101L));

            // send a record to the processor in a new window
            processor.process(new Record<>("key", "gamma delta", 221L));

            // note that the processor does not forward during process()
            assertThat(context.forwarded().isEmpty(), is(true));

            // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
            context.scheduledPunctuators().get(0).getPunctuator().punctuate(1_000L);

            // finally, we can verify the output.
            final List<CapturedForward<? extends String, ? extends String>> capturedForwards = context.forwarded();
            final List<CapturedForward<? extends String, ? extends String>> expected = asList(
                new CapturedForward<>(new Record<>("[alpha@100/200]", "2", 1_000L)),
                new CapturedForward<>(new Record<>("[beta@100/200]", "1", 1_000L)),
                new CapturedForward<>(new Record<>("[delta@200/300]", "1", 1_000L)),
                new CapturedForward<>(new Record<>("[gamma@100/200]", "1", 1_000L)),
                new CapturedForward<>(new Record<>("[gamma@200/300]", "1", 1_000L))
            );

            assertThat(capturedForwards, is(expected));

            store.close();
        } finally {
            Utils.delete(stateDir);
        }
    }
}
