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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;
import org.apache.kafka.test.MockKeyValueStore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getStreamsConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AbstractProcessorContextTest {

    private final MockStreamsMetrics metrics = new MockStreamsMetrics(new Metrics());
    private final AbstractProcessorContext<?, ?> context = new TestProcessorContext(metrics);
    private final MockKeyValueStore stateStore = new MockKeyValueStore("store", false);
    private final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    private final ProcessorRecordContext recordContext = new ProcessorRecordContext(10, System.currentTimeMillis(), 1, "foo", headers);

    @BeforeEach
    public void before() {
        context.setRecordContext(recordContext);
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnRegisterWhenContextIsInitialized() {
        context.initialize();
        assertThrows(IllegalStateException.class, () -> context.register(stateStore, null),
            "should throw illegal state exception when context already initialized");
    }

    @Test
    public void shouldNotThrowIllegalStateExceptionOnRegisterWhenContextIsNotInitialized() {
        context.register(stateStore, null);
    }

    @Test
    public void shouldThrowNullPointerOnRegisterIfStateStoreIsNull() {
        assertThrows(NullPointerException.class, () -> context.register(null, null));
    }

    @Test
    public void shouldReturnNullTopicIfNoRecordContext() {
        context.setRecordContext(null);
        assertNull(context.topic());
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnTopicIfRecordContextTopicIsNull() {
        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, null, new RecordHeaders()));
        assertNull(context.topic());
    }

    @Test
    public void shouldReturnTopicFromRecordContext() {
        assertEquals(recordContext.topic(), context.topic());
    }

    @Test
    public void shouldReturnNullIfTopicEqualsNonExistTopic() {
        context.setRecordContext(null);
        assertNull(context.topic());
    }

    @Test
    public void shouldReturnDummyPartitionIfNoRecordContext() {
        context.setRecordContext(null);
        assertEquals(-1, context.partition());
    }

    @Test
    public void shouldReturnPartitionFromRecordContext() {
        assertEquals(recordContext.partition(), context.partition());
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnOffsetIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.offset();
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnOffsetFromRecordContext() {
        assertEquals(recordContext.offset(), context.offset());
    }

    @Test
    public void shouldReturnDummyTimestampIfNoRecordContext() {
        context.setRecordContext(null);
        assertEquals(0L, context.timestamp());
    }

    @Test
    public void shouldReturnTimestampFromRecordContext() {
        assertEquals(recordContext.timestamp(), context.timestamp());
    }

    @Test
    public void shouldReturnHeadersFromRecordContext() {
        assertEquals(recordContext.headers(), context.headers());
    }

    @Test
    public void shouldReturnEmptyHeadersIfHeadersAreNotSet() {
        context.setRecordContext(null);
        assertFalse(context.headers().iterator().hasNext());
    }

    @Test
    public void appConfigsShouldReturnParsedValues() {
        assertEquals(RocksDBConfigSetter.class, context.appConfigs().get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG));
    }

    @Test
    public void appConfigsShouldReturnUnrecognizedValues() {
        assertEquals("user-supplied-value", context.appConfigs().get("user.supplied.config"));
    }
    @Test
    public void shouldThrowErrorIfSerdeDefaultNotSet() {
        final Properties config = getStreamsConfig();
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.class.getName());
        config.put("user.supplied.config", "user-supplied-value");
        final TestProcessorContext pc = new TestProcessorContext(metrics, config);
        assertThrows(ConfigException.class, pc::keySerde);
        assertThrows(ConfigException.class, pc::valueSerde);
    }

    private static class TestProcessorContext extends AbstractProcessorContext<Object, Object> {
        static Properties config;
        static {
            config = getStreamsConfig();
            // Value must be a string to test className -> class conversion
            config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.class.getName());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
            config.put("user.supplied.config", "user-supplied-value");
        }

        TestProcessorContext(final MockStreamsMetrics metrics) {
            super(new TaskId(0, 0), new StreamsConfig(config), metrics, new ThreadCache(new LogContext("name "), 0, metrics));
        }

        TestProcessorContext(final MockStreamsMetrics metrics, final Properties config) {
            super(new TaskId(0, 0), new StreamsConfig(config), metrics, new ThreadCache(new LogContext("name "), 0, metrics));
        }

        @Override
        protected StateManager stateManager() {
            return new StateManagerStub();
        }

        @Override
        public <S extends StateStore> S getStateStore(final String name) {
            return null;
        }

        @Override
        public Cancellable schedule(final Duration interval,
                                    final PunctuationType type,
                                    final Punctuator callback) throws IllegalArgumentException {
            return null;
        }

        @Override
        public Cancellable schedule(final Instant startTime,
                                    final Duration interval,
                                    final PunctuationType type,
                                    final Punctuator callback) {
            return null;
        }

        @Override
        public <K, V> void forward(final Record<K, V> record) {}

        @Override
        public <K, V> void forward(final Record<K, V> record, final String childName) {}

        @Override
        public <K, V> void forward(final K key, final V value) {}

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {}

        @Override
        public void commit() {}

        @Override
        public long currentStreamTimeMs() {
            throw new UnsupportedOperationException("this method is not supported in TestProcessorContext");
        }

        @Override
        public void logChange(final String storeName,
                              final Bytes key,
                              final byte[] value,
                              final long timestamp,
                              final Headers headers,
                              final Position position) {
        }

        @Override
        public void transitionToActive(final StreamTask streamTask, final RecordCollector recordCollector, final ThreadCache newCache) {
        }

        @Override
        public void transitionToStandby(final ThreadCache newCache) {
        }

        @Override
        public void registerCacheFlushListener(final String namespace, final DirtyEntryFlushListener listener) {
        }

        @Override
        public String changelogFor(final String storeName) {
            return ProcessorStateManager.storeChangelogTopic(applicationId(), storeName, taskId().topologyName());
        }

        @Override
        public <K, V> void forward(final FixedKeyRecord<K, V> record) {
            forward(new Record<>(record.key(), record.value(), record.timestamp(), record.headers()));
        }

        @Override
        public <K, V> void forward(final FixedKeyRecord<K, V> record, final String childName) {
            forward(
                new Record<>(record.key(), record.value(), record.timestamp(), record.headers()),
                childName
            );
        }
    }
}
