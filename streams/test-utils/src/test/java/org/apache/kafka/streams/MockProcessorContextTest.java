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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MockProcessorContextTest {
    /**
     * Behavioral test demonstrating the use of the context for capturing forwarded values
     */
    @Test
    public void shouldCaptureOutputRecords() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override
            public void process(final String key, final Long value) {
                context().forward(key + value, key.length() + value);
            }
        };

        final MockProcessorContext context = new MockProcessorContext();
        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
        assertEquals(forwarded.next().keyValue(), new KeyValue<>("foo5", 8L));
        assertEquals(forwarded.next().keyValue(), new KeyValue<>("barbaz50", 56L));
        assertFalse(forwarded.hasNext());

        context.resetForwards();

        assertEquals(context.forwarded().size(), 0);
    }

    /**
     * Behavioral test demonstrating the use of the context for capturing forwarded values using the To API
     */
    @Test
    public void shouldCaptureOutputRecordsUsingTo() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override
            public void process(final String key, final Long value) {
                context().forward(key + value, key.length() + value, To.all());
            }
        };

        final MockProcessorContext context = new MockProcessorContext();

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
        assertEquals(forwarded.next().keyValue(), new KeyValue<>("foo5", 8L));
        assertEquals(forwarded.next().keyValue(), new KeyValue<>("barbaz50", 56L));
        assertFalse(forwarded.hasNext());

        context.resetForwards();

        assertEquals(context.forwarded().size(), 0);
    }

    /**
     * Behavioral test demonstrating the use of the context for capturing forwarded values to specific children
     */
    @Test
    public void shouldCaptureRecordsOutputToChildByName() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            private int count = 0;

            @Override
            public void process(final String key, final Long value) {
                if (count == 0) {
                    context().forward("start", -1L, To.all()); // broadcast
                }
                final To toChild = count % 2 == 0 ? To.child("george") : To.child("pete");
                context().forward(key + value, key.length() + value, toChild);
                count++;
            }
        };

        final MockProcessorContext context = new MockProcessorContext();

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        {
            final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("start", -1L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("foo5", 8L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("barbaz50", 56L));
            assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<CapturedForward> forwarded = context.forwarded("george").iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("start", -1L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("foo5", 8L));
            assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<CapturedForward> forwarded = context.forwarded("pete").iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("start", -1L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("barbaz50", 56L));
            assertFalse(forwarded.hasNext());
        }

        {
            final Iterator<CapturedForward> forwarded = context.forwarded("steve").iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("start", -1L));
            assertFalse(forwarded.hasNext());
        }
    }

    /**
     * Unit test verifying that forwarding to child by index is not supported. This usage is deprecated.
     */
    @Test
    public void shouldThrowIfForwardedWithDeprecatedChildIndex() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override
            public void process(final String key, final Long value) {
                //noinspection deprecation
                context().forward(key, value, 0);
            }
        };

        final MockProcessorContext context = new MockProcessorContext();

        processor.init(context);

        try {
            processor.process("foo", 5L);
            fail("Should have thrown an UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) {
            // expected
        }
    }

    /**
     * Unit test verifying that forwarding to child by name is not supported. This usage is deprecated.
     */
    @Test
    public void shouldThrowIfForwardedWithDeprecatedChildName() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override
            public void process(final String key, final Long value) {
                //noinspection deprecation
                context().forward(key, value, "child1");
            }
        };

        final MockProcessorContext context = new MockProcessorContext();

        processor.init(context);

        try {
            processor.process("foo", 5L);
            fail("Should have thrown an UnsupportedOperationException.");
        } catch (final UnsupportedOperationException expected) {
            // expected
        }
    }

    /**
     * Behavioral test demonstrating the use of the context for capturing commits
     */
    @Test
    public void shouldCaptureCommitsAndAllowReset() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            private int count = 0;

            @Override
            public void process(final String key, final Long value) {
                if (++count > 2) context().commit();
            }
        };

        final MockProcessorContext context = new MockProcessorContext();

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("barbaz", 50L);

        assertFalse(context.committed());

        processor.process("foobar", 500L);

        assertTrue(context.committed());

        context.resetCommit();

        assertFalse(context.committed());
    }

    /**
     * Behavioral test demonstrating the use of state stores
     */
    @Test
    public void shouldStoreAndReturnStateStores() {
        final AbstractProcessor<String, Long> processor = new AbstractProcessor<String, Long>() {
            @Override
            public void process(final String key, final Long value) {
                //noinspection unchecked
                final KeyValueStore<String, Long> stateStore = (KeyValueStore<String, Long>) context().getStateStore("my-state");
                stateStore.put(key, (stateStore.get(key) == null ? 0 : stateStore.get(key)) + value);
                stateStore.put("all", (stateStore.get("all") == null ? 0 : stateStore.get("all")) + value);
            }
        };

        final MockProcessorContext context = new MockProcessorContext();
        final KeyValueStore<String, Long> store = new InMemoryKeyValueStore<>("my-state", Serdes.String(), Serdes.Long());
        context.register(store, false, null);

        processor.init(context);

        processor.process("foo", 5L);
        processor.process("bar", 50L);

        assertEquals(5L, (long) store.get("foo"));
        assertEquals(50L, (long) store.get("bar"));
        assertEquals(55L, (long) store.get("all"));
    }

    /**
     * Behavioral test demonstrating the use of the context with context-aware processors
     */
    @Test
    public void shouldCaptureApplicationAndRecordMetadata() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testMetadata");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final AbstractProcessor<String, Object> processor = new AbstractProcessor<String, Object>() {
            @Override
            public void process(final String key, final Object value) {
                context().forward("appId", context().applicationId());
                context().forward("taskId", context().taskId());

                context().forward("topic", context().topic());
                context().forward("partition", context().partition());
                context().forward("offset", context().offset());
                context().forward("timestamp", context().timestamp());

                context().forward("key", key);
                context().forward("value", value);
            }
        };

        final MockProcessorContext context = new MockProcessorContext(config);
        processor.init(context);

        try {
            processor.process("foo", 5L);
            fail("Should have thrown an NPE.");
        } catch (final NullPointerException expected) {
            // expected, since the record metadata isn't initialized
        }

        context.resetForwards();
        context.setRecordMetadata("t1", 0, 0L, 0L);

        {
            processor.process("foo", 5L);
            final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("appId", "testMetadata"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("taskId", new TaskId(0, 0)));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("topic", "t1"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("partition", 0));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("offset", 0L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("timestamp", 0L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("key", "foo"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("value", 5L));
        }

        context.resetForwards();

        // record metadata should be "sticky"
        context.setOffset(1L);
        context.setTimestamp(10L);

        {
            processor.process("bar", 50L);
            final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("appId", "testMetadata"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("taskId", new TaskId(0, 0)));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("topic", "t1"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("partition", 0));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("offset", 1L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("timestamp", 10L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("key", "bar"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("value", 50L));
        }

        context.resetForwards();
        // record metadata should be "sticky"
        context.setTopic("t2");
        context.setPartition(30);

        {
            processor.process("baz", 500L);
            final Iterator<CapturedForward> forwarded = context.forwarded().iterator();
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("appId", "testMetadata"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("taskId", new TaskId(0, 0)));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("topic", "t2"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("partition", 30));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("offset", 1L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("timestamp", 10L));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("key", "baz"));
            assertEquals(forwarded.next().keyValue(), new KeyValue<>("value", 500L));
        }
    }

    /**
     * Behavioral test demonstrating testing captured punctuator behavior
     */
    @Test
    public void shouldCapturePunctuator() {
        final Processor<String, Long> processor = new Processor<String, Long>() {
            @Override
            public void init(final ProcessorContext context) {
                context.schedule(
                    1000L,
                    PunctuationType.WALL_CLOCK_TIME,
                    new Punctuator() {
                        @Override
                        public void punctuate(final long timestamp) {
                            context.commit();
                        }
                    }
                );
            }

            @Override
            public void process(final String key, final Long value) {
            }

            @Override
            public void punctuate(final long timestamp) {
            }

            @Override
            public void close() {
            }
        };

        final MockProcessorContext context = new MockProcessorContext();

        processor.init(context);

        final MockProcessorContext.CapturedPunctuator capturedPunctuator = context.scheduledPunctuators().get(0);
        assertEquals(1000L, capturedPunctuator.getIntervalMs());
        assertEquals(PunctuationType.WALL_CLOCK_TIME, capturedPunctuator.getType());
        assertFalse(capturedPunctuator.cancelled());

        final Punctuator punctuator = capturedPunctuator.getPunctuator();
        assertFalse(context.committed());
        punctuator.punctuate(1234L);
        assertTrue(context.committed());
    }

    /**
     * Unit test verifying the full MockProcessorContext constructor
     */
    @Test
    public void fullConstructorShouldSetAllExpectedAttributes() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testFullConstructor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        final File dummyFile = new File("");
        final MockProcessorContext context = new MockProcessorContext(config, new TaskId(1, 1), dummyFile);

        assertEquals(context.applicationId(), "testFullConstructor");
        assertEquals(context.taskId(), new TaskId(1, 1));
        assertEquals("testFullConstructor", context.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals("testFullConstructor", context.appConfigsWithPrefix("application.").get("id"));
        assertEquals(Serdes.String().getClass(), context.keySerde().getClass());
        assertEquals(Serdes.Long().getClass(), context.valueSerde().getClass());
        assertEquals(dummyFile, context.stateDir());
    }
}
