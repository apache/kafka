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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockStateStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.test.StreamsTestUtils.minimalStreamsConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class AbstractProcessorContextTest {

    private final MockStreamsMetrics metrics = new MockStreamsMetrics(new Metrics());
    private final AbstractProcessorContext context = new TestProcessorContext(metrics);
    private final MockStateStore stateStore = new MockStateStore("store", false);
    private final RecordContext recordContext = new RecordContextStub(10, System.currentTimeMillis(), 1, "foo");

    @Before
    public void before() {
        context.setRecordContext(recordContext);
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnRegisterWhenContextIsInitialized() {
        context.initialized();
        try {
            context.register(stateStore, null);
            fail("should throw illegal state exception when context already initialized");
        } catch (IllegalStateException e) {
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
    public void shouldThrowIllegalStateExceptionOnTopicIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.topic();
            fail("should throw illegal state exception when record context is null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnTopicFromRecordContext() {
        assertThat(context.topic(), equalTo(recordContext.topic()));
    }

    @Test
    public void shouldReturnNullIfTopicEqualsNonExistTopic() {
        context.setRecordContext(new RecordContextStub(0, 0, 0, AbstractProcessorContext.NONEXIST_TOPIC));
        assertThat(context.topic(), nullValue());
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnPartitionIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.partition();
            fail("should throw illegal state exception when record context is null");
        } catch (final IllegalStateException e) {
            // pass
        }
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
    public void shouldThrowIllegalStateExceptionOnTimestampIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.timestamp();
            fail("should throw illegal state exception when record context is null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnTimestampFromRecordContext() {
        assertThat(context.timestamp(), equalTo(recordContext.timestamp()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void appConfigsShouldReturnParsedValues() {
        assertThat((Class<RocksDBConfigSetter>) context.appConfigs().get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG), equalTo(RocksDBConfigSetter.class));
    }

    @Test
    public void appConfigsShouldReturnUnrecognizedValues() {
        assertThat((String) context.appConfigs().get("user.supplied.config"), equalTo("user-suppplied-value"));
    }


    private static class TestProcessorContext extends AbstractProcessorContext {
        static Properties config;
        static {
            config = minimalStreamsConfig();
            // Value must be a string to test className -> class conversion
            config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.class.getName());
            config.put("user.supplied.config", "user-suppplied-value");
        }

        TestProcessorContext(final MockStreamsMetrics metrics) {
            super(new TaskId(0, 0), new StreamsConfig(config), metrics, new StateManagerStub(), new ThreadCache(new LogContext("name "), 0, metrics));
        }

        @Override
        public StateStore getStateStore(final String name) {
            return null;
        }

        @Override
        public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
            return null;
        }

        @Override
        public <K, V> void forward(final K key, final V value) {}

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {}

        @Override
        public <K, V> void forward(final K key, final V value, final int childIndex) {}

        @Override
        public <K, V> void forward(final K key, final V value, final String childName) {}

        @Override
        public void commit() {}
    }
}
