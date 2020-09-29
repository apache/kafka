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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.state.internals.ThreadCache.DirtyEntryFlushListener;
import org.apache.kafka.test.MockKeyValueStore;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.getStreamsConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class AbstractProcessorContextTest {

    private final MockStreamsMetrics metrics = new MockStreamsMetrics(new Metrics());
    private final AbstractProcessorContext context = new TestProcessorContext(metrics);
    private final MockKeyValueStore stateStore = new MockKeyValueStore("store", false);
    private final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    private final ProcessorRecordContext recordContext = new ProcessorRecordContext(10, System.currentTimeMillis(), 1, "foo", headers);

    @Before
    public void before() {
        context.setRecordContext(recordContext);
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnRegisterWhenContextIsInitialized() {
        context.initialize();
        try {
            context.register(stateStore, null);
            fail("should throw illegal state exception when context already initialized");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldNotThrowIllegalStateExceptionOnRegisterWhenContextIsNotInitialized() {
        context.register(stateStore, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnRegisterIfStateStoreIsNull() {
        context.register(null, null);
    }

    @Test
    public void shouldReturnNullTopicIfNoRecordContext() {
        context.setRecordContext(null);
        assertThat(context.topic(), is(nullValue()));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionOnTopicIfRecordContextTopicIsNull() {
        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, null, null));
        assertThat(context.topic(), nullValue());
    }

    @Test
    public void shouldReturnTopicFromRecordContext() {
        assertThat(context.topic(), equalTo(recordContext.topic()));
    }

    @Test
    public void shouldReturnNullIfTopicEqualsNonExistTopic() {
        context.setRecordContext(null);
        assertThat(context.topic(), nullValue());
    }

    @Test
    public void shouldReturnDummyPartitionIfNoRecordContext() {
        context.setRecordContext(null);
        assertThat(context.partition(), is(-1));
    }

    @Test
    public void shouldReturnPartitionFromRecordContext() {
        assertThat(context.partition(), equalTo(recordContext.partition()));
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
        assertThat(context.offset(), equalTo(recordContext.offset()));
    }

    @Test
    public void shouldReturnDummyTimestampIfNoRecordContext() {
        context.setRecordContext(null);
        assertThat(context.timestamp(), is(0L));
    }

    @Test
    public void shouldReturnTimestampFromRecordContext() {
        assertThat(context.timestamp(), equalTo(recordContext.timestamp()));
    }

    @Test
    public void shouldReturnHeadersFromRecordContext() {
        assertThat(context.headers(), equalTo(recordContext.headers()));
    }

    @Test
    public void shouldReturnEmptyHeadersIfHeadersAreNotSet() {
        context.setRecordContext(null);
        assertThat(context.headers(), is(emptyIterable()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnHeadersIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.headers();
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void appConfigsShouldReturnParsedValues() {
        assertThat(
            context.appConfigs().get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG),
            equalTo(RocksDBConfigSetter.class));
    }

    @Test
    public void appConfigsShouldReturnUnrecognizedValues() {
        assertThat(
            context.appConfigs().get("user.supplied.config"),
            equalTo("user-supplied-value"));
    }


    private static class TestProcessorContext extends AbstractProcessorContext {
        static Properties config;
        static {
            config = getStreamsConfig();
            // Value must be a string to test className -> class conversion
            config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.class.getName());
            config.put("user.supplied.config", "user-supplied-value");
        }

        TestProcessorContext(final MockStreamsMetrics metrics) {
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
        @Deprecated
        public Cancellable schedule(final long interval,
                                    final PunctuationType type,
                                    final Punctuator callback) {
            return null;
        }

        @Override
        public Cancellable schedule(final Duration interval,
                                    final PunctuationType type,
                                    final Punctuator callback) throws IllegalArgumentException {
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
        @Deprecated
        public <K, V> void forward(final K key, final V value, final int childIndex) {}

        @Override
        @Deprecated
        public <K, V> void forward(final K key, final V value, final String childName) {}

        @Override
        public void commit() {}

        @Override
        public void logChange(final String storeName,
                              final Bytes key,
                              final byte[] value,
                              final long timestamp) {
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
            return ProcessorStateManager.storeChangelogTopic(applicationId(), storeName);
        }
    }
}
