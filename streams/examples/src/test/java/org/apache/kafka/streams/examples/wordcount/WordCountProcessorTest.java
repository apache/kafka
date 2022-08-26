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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demonstrate the use of {@link MockProcessorContext} for testing the {@link Processor} in the {@link WordCountProcessorDemo}.
 */
public class WordCountProcessorTest {
    @Test
    public void test() {
        final MockProcessorContext<String, String> context = new MockProcessorContext<String, String>();

        // Create, initialize, and register the state store.
        final KeyValueStore<String, Integer> store =
            Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("Counts"), Serdes.String(), Serdes.Integer())
                .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                // Caching is disabled by default, but FYI: caching is also not supported by MockProcessorContext.
                .build();
        store.init(context.getStateStoreContext(), store);

        // Create and initialize the processor under test
        final Processor<String, String, String, String> processor = new WordCountProcessorDemo.WordCountProcessor();
        processor.init(context);

        // send a record to the processor
        processor.process(new Record<>("key", "alpha beta\tgamma\n\talpha", 0L));

        // note that the processor does not forward during process()
        assertTrue(context.forwarded().isEmpty());

        // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

        // finally, we can verify the output.
        final List<MockProcessorContext.CapturedForward<String, String>> expected = Arrays.asList(
            new MockProcessorContext.CapturedForward<>(new Record<>("alpha", "2", 0L)),
            new MockProcessorContext.CapturedForward<>(new Record<>("beta", "1", 0L)),
            new MockProcessorContext.CapturedForward<>(new Record<>("gamma", "1", 0L))
        );
        assertThat(context.forwarded(), is(expected));
    }
}
