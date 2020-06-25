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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Locale;

public final class WindowedWordCountProcessorSupplier implements ProcessorSupplier<String, String> {

    @Override
    public Processor<String, String> get() {
        return new Processor<String, String>() {
            private ProcessorContext context;
            private WindowStore<String, Integer> windowStore;

            @Override
            @SuppressWarnings("unchecked")
            public void init(final ProcessorContext context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                    try (final KeyValueIterator<Windowed<String>, Integer> iter = windowStore.all()) {
                        while (iter.hasNext()) {
                            final KeyValue<Windowed<String>, Integer> entry = iter.next();
                            context.forward(entry.key.toString(), entry.value.toString());
                        }
                    }
                });
                windowStore = (WindowStore<String, Integer>) context.getStateStore("WindowedCounts");
            }

            @Override
            public void process(final String key, final String value) {
                final String[] words = value.toLowerCase(Locale.getDefault()).split(" ");
                final long timestamp = context.timestamp();

                // calculate the window as every 100 ms
                // Note this has to be aligned with the configuration for the window store you register separately
                final long windowStart = timestamp / 100 * 100;

                for (final String word : words) {
                    final Integer oldValue = windowStore.fetch(word, windowStart);

                    if (oldValue == null) {
                        windowStore.put(word, 1, windowStart);
                    } else {
                        windowStore.put(word, oldValue + 1, windowStart);
                    }
                }
            }

            @Override
            public void close() {}
        };
    }
}
