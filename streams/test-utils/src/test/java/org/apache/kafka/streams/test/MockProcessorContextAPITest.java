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
package org.apache.kafka.streams.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class MockProcessorContextAPITest {
    @Test
    public void shouldCaptureOutputRecords() {
        final Processor<String, Long, String, Long> processor = new Processor<String, Long, String, Long>() {
            private ProcessorContext<String, Long> context;

            @Override
            public void init(final ProcessorContext<String, Long> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<String, Long> record) {
                final String key = record.key();
                final Long value = record.value();
                context.forward(record.withKey(key + value).withValue(key.length() + value));
            }
        };

        final MockProcessorContext<String, Long> context = new MockProcessorContext<>();
        processor.init(context);

        processor.process(new Record<>("foo", 5L, 0L));
        processor.process(new Record<>("barbaz", 50L, 0L));

        final List<CapturedForward<? extends String, ? extends Long>> actual = context.forwarded();
        final List<CapturedForward<String, Long>> expected = asList(
            new CapturedForward<>(new Record<>("foo5", 8L, 0L)),
            new CapturedForward<>(new Record<>("barbaz50", 56L, 0L))
        );
        assertThat(actual, is(expected));

        context.resetForwards();

        assertThat(context.forwarded(), empty());
    }

    @Test
    public void shouldCaptureRecordsOutputToChildByName() {
        final Processor<String, Long, String, Long> processor = new Processor<String, Long, String, Long>() {
            private ProcessorContext<String, Long> context;

            @Override
            public void process(final Record<String, Long> record) {
                final String key = record.key();
                final Long value = record.value();
                if (count == 0) {
                    context.forward(new Record<>("start", -1L, 0L)); // broadcast
                }
                final String toChild = count % 2 == 0 ? "george" : "pete";
                context.forward(new Record<>(key + value, key.length() + value, 0L), toChild);
                count++;
            }

            @Override
            public void init(final ProcessorContext<String, Long> context) {
                this.context = context;
            }

            private int count = 0;

        };

        final MockProcessorContext<String, Long> context = new MockProcessorContext<>();

        processor.init(context);

        processor.process(new Record<>("foo", 5L, 0L));
        processor.process(new Record<>("barbaz", 50L, 0L));

        {
            final List<CapturedForward<? extends String, ? extends Long>> forwarded = context.forwarded();
            final List<CapturedForward<? extends String, ? extends Long>> expected = asList(
                new CapturedForward<>(new Record<>("start", -1L, 0L), Optional.empty()),
                new CapturedForward<>(new Record<>("foo5", 8L, 0L), Optional.of("george")),
                new CapturedForward<>(new Record<>("barbaz50", 56L, 0L), Optional.of("pete"))
            );

            assertThat(forwarded, is(expected));
        }
        {
            final List<CapturedForward<? extends String, ? extends Long>> forwarded = context.forwarded("george");
            final List<CapturedForward<? extends String, ? extends Long>> expected = asList(
                new CapturedForward<>(new Record<>("start", -1L, 0L), Optional.empty()),
                new CapturedForward<>(new Record<>("foo5", 8L, 0L), Optional.of("george"))
            );

            assertThat(forwarded, is(expected));
        }
        {
            final List<CapturedForward<? extends String, ? extends Long>> forwarded = context.forwarded("pete");
            final List<CapturedForward<? extends String, ? extends Long>> expected = asList(
                new CapturedForward<>(new Record<>("start", -1L, 0L), Optional.empty()),
                new CapturedForward<>(new Record<>("barbaz50", 56L, 0L), Optional.of("pete"))
            );

            assertThat(forwarded, is(expected));
        }
        {
            final List<CapturedForward<? extends String, ? extends Long>> forwarded = context.forwarded("steve");
            final List<CapturedForward<? extends String, ? extends Long>> expected = singletonList(
                new CapturedForward<>(new Record<>("start", -1L, 0L))
            );

            assertThat(forwarded, is(expected));
        }
    }

    @Test
    public void shouldCaptureCommitsAndAllowReset() {
        final Processor<String, Long, Void, Void> processor = new Processor<String, Long, Void, Void>() {
            private ProcessorContext<Void, Void> context;
            private int count = 0;

            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<String, Long> record) {
                if (++count > 2) {
                    context.commit();
                }
            }
        };

        final MockProcessorContext<Void, Void> context = new MockProcessorContext<>();

        processor.init(context);

        processor.process(new Record<>("foo", 5L, 0L));
        processor.process(new Record<>("barbaz", 50L, 0L));

        assertThat(context.committed(), is(false));

        processor.process(new Record<>("foobar", 500L, 0L));

        assertThat(context.committed(), is(true));

        context.resetCommit();

        assertThat(context.committed(), is(false));
    }

    @Test
    public void shouldStoreAndReturnStateStores() {
        final Processor<String, Long, Void, Void> processor = new Processor<String, Long, Void, Void>() {
            private ProcessorContext<Void, Void> context;

            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<String, Long> record) {
                final String key = record.key();
                final Long value = record.value();
                final KeyValueStore<String, Long> stateStore = context.getStateStore("my-state");

                stateStore.put(key, (stateStore.get(key) == null ? 0 : stateStore.get(key)) + value);
                stateStore.put("all", (stateStore.get("all") == null ? 0 : stateStore.get("all")) + value);
            }

        };

        final MockProcessorContext<Void, Void> context = new MockProcessorContext<>();

        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("my-state"),
            Serdes.String(),
            Serdes.Long()).withLoggingDisabled();

        final KeyValueStore<String, Long> store = storeBuilder.build();

        store.init(context.getStateStoreContext(), store);

        processor.init(context);

        processor.process(new Record<>("foo", 5L, 0L));
        processor.process(new Record<>("bar", 50L, 0L));

        assertThat(store.get("foo"), is(5L));
        assertThat(store.get("bar"), is(50L));
        assertThat(store.get("all"), is(55L));
    }


    @Test
    public void shouldCaptureApplicationAndRecordMetadata() {
        final Properties config = mkProperties(
            mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "testMetadata"),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
            )
        );

        final Processor<String, Object, String, Object> processor = new Processor<String, Object, String, Object>() {
            private ProcessorContext<String, Object> context;

            @Override
            public void init(final ProcessorContext<String, Object> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<String, Object> record) {
                context.forward(new Record<String, Object>("appId", context.applicationId(), 0L));
                context.forward(new Record<String, Object>("taskId", context.taskId(), 0L));

                if (context.recordMetadata().isPresent()) {
                    final RecordMetadata recordMetadata = context.recordMetadata().get();
                    context.forward(new Record<String, Object>("topic", recordMetadata.topic(), 0L));
                    context.forward(new Record<String, Object>("partition", recordMetadata.partition(), 0L));
                    context.forward(new Record<String, Object>("offset", recordMetadata.offset(), 0L));
                }

                context.forward(new Record<String, Object>("record", record, 0L));
            }
        };

        final MockProcessorContext<String, Object> context = new MockProcessorContext<>(config);
        processor.init(context);

        processor.process(new Record<>("foo", 5L, 0L));
        {
            final List<CapturedForward<? extends String, ?>> forwarded = context.forwarded();
            final List<CapturedForward<? extends String, ?>> expected = asList(
                new CapturedForward<>(new Record<>("appId", "testMetadata", 0L)),
                new CapturedForward<>(new Record<>("taskId", new TaskId(0, 0), 0L)),
                new CapturedForward<>(new Record<>("record", new Record<>("foo", 5L, 0L), 0L))
            );
            assertThat(forwarded, is(expected));
        }
        context.resetForwards();
        context.setRecordMetadata("t1", 0, 0L);
        processor.process(new Record<>("foo", 5L, 0L));
        {
            final List<CapturedForward<? extends String, ?>> forwarded = context.forwarded();
            final List<CapturedForward<? extends String, ?>> expected = asList(
                new CapturedForward<>(new Record<>("appId", "testMetadata", 0L)),
                new CapturedForward<>(new Record<>("taskId", new TaskId(0, 0), 0L)),
                new CapturedForward<>(new Record<>("topic", "t1", 0L)),
                new CapturedForward<>(new Record<>("partition", 0, 0L)),
                new CapturedForward<>(new Record<>("offset", 0L, 0L)),
                new CapturedForward<>(new Record<>("record", new Record<>("foo", 5L, 0L), 0L))
            );
            assertThat(forwarded, is(expected));
        }
    }

    @Test
    public void shouldCapturePunctuator() {
        final Processor<String, Long, Void, Void> processor = new Processor<String, Long, Void, Void>() {
            @Override
            public void init(final ProcessorContext<Void, Void> context) {
                context.schedule(
                    Duration.ofSeconds(1L),
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> context.commit()
                );
            }

            @Override
            public void process(final Record<String, Long> record) {}
        };

        final MockProcessorContext<Void, Void> context = new MockProcessorContext<>();

        processor.init(context);

        final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        assertThat(capturedPunctuator.getInterval(), is(Duration.ofMillis(1000L)));
        assertThat(capturedPunctuator.getType(), is(PunctuationType.WALL_CLOCK_TIME));
        assertThat(capturedPunctuator.cancelled(), is(false));

        final Punctuator punctuator = capturedPunctuator.getPunctuator();
        assertThat(context.committed(), is(false));
        punctuator.punctuate(1234L);
        assertThat(context.committed(), is(true));
    }

    @Test
    public void fullConstructorShouldSetAllExpectedAttributes() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFullConstructor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        final File dummyFile = new File("");
        final MockProcessorContext<Void, Void> context =
            new MockProcessorContext<>(config, new TaskId(1, 1), dummyFile);

        assertThat(context.applicationId(), is("testFullConstructor"));
        assertThat(context.taskId(), is(new TaskId(1, 1)));
        assertThat(context.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG), is("testFullConstructor"));
        assertThat(context.appConfigsWithPrefix("application.").get("id"), is("testFullConstructor"));
        assertThat(context.keySerde().getClass(), is(Serdes.String().getClass()));
        assertThat(context.valueSerde().getClass(), is(Serdes.Long().getClass()));
        assertThat(context.stateDir(), is(dummyFile));
    }
}
