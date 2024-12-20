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
package org.apache.kafka.streams;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Deprecated
public class MockProcessorContextTest {

    @Test
    public void shouldCaptureOutputRecords() {
        final Transformer<String, Long, KeyValue<String, Long>> transformer = new Transformer<>() {
            private ProcessorContext context;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, Long> transform(final String key, final Long value) {
                context.forward(key + value, key.length() + value);
                return null;
            }

            @Override
            public void close() { }
        };

        final MockProcessorContext context = new MockProcessorContext();
        transformer.init(context);

        transformer.transform("foo", 5L);
        transformer.transform("barbaz", 50L);

        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
        assertEquals(new KeyValue<>("foo5", 8L), forwarded.next().keyValue());
        assertEquals(new KeyValue<>("barbaz50", 56L), forwarded.next().keyValue());
        assertFalse(forwarded.hasNext());

        context.resetForwards();

        assertEquals(0, context.forwarded().size());
    }

    @Test
    public void shouldCaptureOutputRecordsUsingTo() {
        final Transformer<String, Long, KeyValue<String, Long>> transformer = new Transformer<>() {
            private ProcessorContext context;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, Long> transform(final String key, final Long value) {
                context.forward(key + value, key.length() + value);
                return null;
            }

            @Override
            public void close() { }
        };

        final MockProcessorContext context = new MockProcessorContext();

        transformer.init(context);

        transformer.transform("foo", 5L);
        transformer.transform("barbaz", 50L);

        final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
        assertEquals(new KeyValue<>("foo5", 8L), forwarded.next().keyValue());
        assertEquals(new KeyValue<>("barbaz50", 56L), forwarded.next().keyValue());
        assertFalse(forwarded.hasNext());

        context.resetForwards();

        assertEquals(0, context.forwarded().size());
    }

    @Test
    public void shouldCaptureRecordsOutputToChildByName() {
        final Transformer<String, Long, KeyValue<String, Long>> transformer = new Transformer<>() {
            private ProcessorContext context;
            private int count = 0;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, Long> transform(final String key, final Long value) {
                if (count == 0) {
                    context.forward("start", -1L, To.all()); // broadcast
                }
                final To toChild = count % 2 == 0 ? To.child("george") : To.child("pete");
                context.forward(key + value, key.length() + value, toChild);
                count++;

                return null;
            }

            @Override
            public void close() { }
        };

        final MockProcessorContext context = new MockProcessorContext();

        transformer.init(context);

        transformer.transform("foo", 5L);
        transformer.transform("barbaz", 50L);

        {
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();

            final MockProcessorContext.CapturedForward forward1 = forwarded.next();
            assertEquals(new KeyValue<>("start", -1L), forward1.keyValue());
            assertNull(forward1.childName());

            final MockProcessorContext.CapturedForward forward2 = forwarded.next();
            assertEquals(new KeyValue<>("foo5", 8L), forward2.keyValue());
            assertEquals("george", forward2.childName());

            final MockProcessorContext.CapturedForward forward3 = forwarded.next();
            assertEquals(new KeyValue<>("barbaz50", 56L), forward3.keyValue());
            assertEquals("pete", forward3.childName());

            assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded("george").iterator();
            assertEquals(new KeyValue<>("start", -1L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("foo5", 8L), forwarded.next().keyValue());
            assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded("pete").iterator();
            assertEquals(new KeyValue<>("start", -1L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("barbaz50", 56L), forwarded.next().keyValue());
            assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded("steve").iterator();
            assertEquals(new KeyValue<>("start", -1L), forwarded.next().keyValue());
            assertFalse(forwarded.hasNext());
        }
    }

    @Test
    public void shouldCaptureCommitsAndAllowReset() {
        final Transformer<String, Long, ?> transformer = new Transformer<>() {
            private ProcessorContext context;
            private int count = 0;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Object transform(final String key, final Long value) {
                if (++count > 2) {
                    context.commit();
                }
                return null;
            }

            @Override
            public void close() { }
        };

        final MockProcessorContext context = new MockProcessorContext();

        transformer.init(context);

        transformer.transform("foo", 5L);
        transformer.transform("barbaz", 50L);

        assertFalse(context.committed());

        transformer.transform("foobar", 500L);

        assertTrue(context.committed());

        context.resetCommit();

        assertFalse(context.committed());
    }

    @Test
    public void shouldStoreAndReturnStateStores() {
        final Transformer<String, Long, ?> transformer = new Transformer<>() {
            private ProcessorContext context;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Object transform(final String key, final Long value) {
                final KeyValueStore<String, Long> stateStore = context.getStateStore("my-state");
                stateStore.put(key, (stateStore.get(key) == null ? 0 : stateStore.get(key)) + value);
                stateStore.put("all", (stateStore.get("all") == null ? 0 : stateStore.get("all")) + value);

                return null;
            }

            @Override
            public void close() { }
        };

        final StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("my-state"),
            Serdes.String(),
            Serdes.Long()).withLoggingDisabled();

        final KeyValueStore<String, Long> store = storeBuilder.build();

        final InternalProcessorContext<?, ?> mockInternalProcessorContext = mock(InternalProcessorContext.class);
        final Map<String, StateStore> stateStores = new HashMap<>();
        doAnswer(invocation -> {
            final StateStore stateStore = invocation.getArgument(0);
            stateStores.put(stateStore.name(), stateStore);
            return null;
        }).when(mockInternalProcessorContext).register(any(), any());
        when(mockInternalProcessorContext.getStateStore(anyString())).thenAnswer(invocation -> {
                final String name = invocation.getArgument(0);
                return stateStores.get(name);
            }
        );
        when(mockInternalProcessorContext.metrics()).thenReturn(new StreamsMetricsImpl(
            new Metrics(new MetricConfig()),
            Thread.currentThread().getName(),
            "processId",
            Time.SYSTEM
        ));
        when(mockInternalProcessorContext.taskId()).thenReturn(new TaskId(1, 1));

        store.init(mockInternalProcessorContext, store);

        transformer.init(mockInternalProcessorContext);

        transformer.transform("foo", 5L);
        transformer.transform("bar", 50L);

        assertEquals(5L, (long) store.get("foo"));
        assertEquals(50L, (long) store.get("bar"));
        assertEquals(55L, (long) store.get("all"));
    }

    @Test
    public void shouldCaptureApplicationAndRecordMetadata() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testMetadata");

        final Transformer<String, Long, KeyValue<String, Object>> transformer = new Transformer<>() {
            private ProcessorContext context;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, Object> transform(final String key, final Long value) {
                context.forward("appId", context.applicationId());
                context.forward("taskId", context.taskId());

                context.forward("topic", context.topic());
                context.forward("partition", context.partition());
                context.forward("offset", context.offset());
                context.forward("timestamp", context.timestamp());

                context.forward("key", key);
                context.forward("value", value);

                return null;
            }

            @Override
            public void close() { }
        };

        final MockProcessorContext context = new MockProcessorContext(config);
        transformer.init(context);

        try {
            transformer.transform("foo", 5L);
            fail("Should have thrown an exception.");
        } catch (final IllegalStateException expected) {
            // expected, since the record metadata isn't initialized
        }

        context.resetForwards();
        context.setRecordMetadata("t1", 0, 0L, new RecordHeaders(), 0L);

        {
            transformer.transform("foo", 5L);
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(new KeyValue<>("appId", "testMetadata"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("taskId", new TaskId(0, 0)), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("topic", "t1"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("partition", 0), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("offset", 0L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("timestamp", 0L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("key", "foo"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("value", 5L), forwarded.next().keyValue());
        }

        context.resetForwards();

        // record metadata should be "sticky"
        context.setOffset(1L);
        context.setRecordTimestamp(10L);
        context.setCurrentSystemTimeMs(20L);
        context.setCurrentStreamTimeMs(30L);

        {
            transformer.transform("bar", 50L);
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(new KeyValue<>("appId", "testMetadata"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("taskId", new TaskId(0, 0)), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("topic", "t1"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("partition", 0), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("offset", 1L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("timestamp", 10L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("key", "bar"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("value", 50L), forwarded.next().keyValue());
            assertEquals(20L, context.currentSystemTimeMs());
            assertEquals(30L, context.currentStreamTimeMs());
        }

        context.resetForwards();
        // record metadata should be "sticky"
        context.setTopic("t2");
        context.setPartition(30);

        {
            transformer.transform("baz", 500L);
            final Iterator<MockProcessorContext.CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(new KeyValue<>("appId", "testMetadata"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("taskId", new TaskId(0, 0)), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("topic", "t2"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("partition", 30), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("offset", 1L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("timestamp", 10L), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("key", "baz"), forwarded.next().keyValue());
            assertEquals(new KeyValue<>("value", 500L), forwarded.next().keyValue());
        }
    }

    @Test
    public void shouldCapturePunctuator() {
        final Transformer<String, Long, KeyValue<String, Long>> transformer = new Transformer<>() {
            @Override
            public void init(final ProcessorContext context) {
                context.schedule(
                    Duration.ofSeconds(1L),
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> context.commit()
                );
            }

            @Override
            public KeyValue<String, Long> transform(final String key, final Long value) {
                return null;
            }

            @Override
            public void close() { }
        };

        final MockProcessorContext context = new MockProcessorContext();

        transformer.init(context);

        final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        assertEquals(1000L, capturedPunctuator.getIntervalMs());
        assertEquals(PunctuationType.WALL_CLOCK_TIME, capturedPunctuator.getType());
        assertFalse(capturedPunctuator.cancelled());

        final Punctuator punctuator = capturedPunctuator.getPunctuator();
        assertFalse(context.committed());
        punctuator.punctuate(1234L);
        assertTrue(context.committed());
    }

    @SuppressWarnings("resource")
    @Test
    public void fullConstructorShouldSetAllExpectedAttributes() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFullConstructor");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);

        final File dummyFile = new File("");
        final MockProcessorContext context = new MockProcessorContext(config, new TaskId(1, 1), dummyFile);

        assertEquals("testFullConstructor", context.applicationId());
        assertEquals(new TaskId(1, 1), context.taskId());
        assertEquals("testFullConstructor", context.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("testFullConstructor", context.appConfigsWithPrefix("application.").get("id"));
        assertEquals(Serdes.StringSerde.class, context.keySerde().getClass());
        assertEquals(Serdes.LongSerde.class, context.valueSerde().getClass());
        assertEquals(dummyFile, context.stateDir());
    }
}