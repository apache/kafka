/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.junit.Before;
import org.junit.Test;

import static org.apache.kafka.test.StreamsTestUtils.minimalStreamsConfig;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class AbstractProcessorContextTest {

    private final MockStreamsMetrics metrics = new MockStreamsMetrics(new Metrics());
    private final AbstractProcessorContext context = new TestProcessorContext(metrics);
    private final MockStateStoreSupplier.MockStateStore stateStore = new MockStateStoreSupplier.MockStateStore("store", false);
    private final RecordContext recordContext = new RecordContextStub(10, System.currentTimeMillis(), 1, "foo");

    @Before
    public void before() {
        context.setRecordContext(recordContext);
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnRegisterWhenContextIsInitialized() throws Exception {
        context.initialized();
        try {
            context.register(stateStore, false, null);
            fail("should throw illegal state exception when context already initialized");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldNotThrowIllegalStateExceptionOnRegisterWhenContextIsNotInitialized() throws Exception {
        context.register(stateStore, false, null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerOnRegisterIfStateStoreIsNull() {
        context.register(null, false, null);
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnTopicIfNoRecordContext() throws Exception {
        context.setRecordContext(null);
        try {
            context.topic();
            fail("should throw illegal state exception when record context is null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnTopicFromRecordContext() throws Exception {
        assertThat(context.topic(), equalTo(recordContext.topic()));
    }

    @Test
    public void shouldReturnNullIfTopicEqualsNonExistTopic() throws Exception {
        context.setRecordContext(new RecordContextStub(0, 0, 0, AbstractProcessorContext.NONEXIST_TOPIC));
        assertThat(context.topic(), nullValue());
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnPartitionIfNoRecordContext() throws Exception {
        context.setRecordContext(null);
        try {
            context.partition();
            fail("should throw illegal state exception when record context is null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnPartitionFromRecordContext() throws Exception {
        assertThat(context.partition(), equalTo(recordContext.partition()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnOffsetIfNoRecordContext() throws Exception {
        context.setRecordContext(null);
        try {
            context.offset();
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnOffsetFromRecordContext() throws Exception {
        assertThat(context.offset(), equalTo(recordContext.offset()));
    }

    @Test
    public void shouldThrowIllegalStateExceptionOnTimestampIfNoRecordContext() throws Exception {
        context.setRecordContext(null);
        try {
            context.timestamp();
            fail("should throw illegal state exception when record context is null");
        } catch (final IllegalStateException e) {
            // pass
        }
    }

    @Test
    public void shouldReturnTimestampFromRecordContext() throws Exception {
        assertThat(context.timestamp(), equalTo(recordContext.timestamp()));
    }


    private static class TestProcessorContext extends AbstractProcessorContext {
        public TestProcessorContext(final MockStreamsMetrics metrics) {
            super(new TaskId(0, 0), "appId", new StreamsConfig(minimalStreamsConfig()), metrics, new StateManagerStub(), new ThreadCache("name", 0, metrics));
        }

        @Override
        public StateStore getStateStore(final String name) {
            return null;
        }

        @Override
        public void schedule(final long interval) {

        }

        @Override
        public <K, V> void forward(final K key, final V value) {

        }

        @Override
        public <K, V> void forward(final K key, final V value, final int childIndex) {

        }

        @Override
        public <K, V> void forward(final K key, final V value, final String childName) {

        }

        @Override
        public void commit() {

        }
    }
}