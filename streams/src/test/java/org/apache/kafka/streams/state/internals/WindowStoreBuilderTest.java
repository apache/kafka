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

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@RunWith(EasyMockRunner.class)
public class WindowStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private WindowBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private WindowStore<Bytes, byte[]> inner;
    private WindowStoreBuilder<String, String> builder;

    @Before
    public void setUp() throws Exception {
        EasyMock.expect(supplier.get()).andReturn(inner);
        EasyMock.expect(supplier.name()).andReturn("name");
        EasyMock.replay(supplier);

        builder = new WindowStoreBuilder<>(supplier,
                                           Serdes.String(),
                                           Serdes.String(),
                                           new MockTime());

    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        final WindowStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredWindowStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        final WindowStore<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingWindowBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        final WindowStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        final WindowStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredWindowStore.class));
        assertThat(wrapped, instanceOf(CachingWindowStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        final WindowStore<String, String> store = builder
                .withLoggingEnabled(Collections.<String, String>emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredWindowStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingWindowBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        final WindowStore<String, String> store = builder
                .withLoggingEnabled(Collections.<String, String>emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertThat(store, instanceOf(MeteredWindowStore.class));
        assertThat(caching, instanceOf(CachingWindowStore.class));
        assertThat(changeLogging, instanceOf(ChangeLoggingWindowBytesStore.class));
        assertThat(changeLogging.wrapped(), CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfInnerIsNull() {
        new WindowStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        new WindowStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        new WindowStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfTimeIsNull() {
        new WindowStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

}