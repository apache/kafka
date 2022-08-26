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
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Locale;

public final class WindowedWordCountProcessorSupplier implements ProcessorSupplier<String, String, String, String> {

    @Override
    public Processor<String, String, String, String> get() {
        return new Processor<String, String, String, String>() {
            private WindowStore<String, Integer> windowStore;

            @Override
            public void init(final ProcessorContext<String, String> context) {
                context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                    try (final KeyValueIterator<Windowed<String>, Integer> iter = windowStore.all()) {
                        while (iter.hasNext()) {
                            final KeyValue<Windowed<String>, Integer> entry = iter.next();
                            context.forward(new Record<>(entry.key.toString(), entry.value.toString(), timestamp));
                        }
                    }
                });
                windowStore = context.getStateStore("WindowedCounts");
            }

            @Override
            public void process(final Record<String, String> record) {
                final String[] words = record.value().toLowerCase(Locale.getDefault()).split(" ");
                final long timestamp = record.timestamp();

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
        };
    }
}
