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
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThrows;

@RunWith(EasyMockRunner.class)
public class SessionStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private SessionBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> inner;
    private SessionStoreBuilder<String, String> builder;

    @Before
    public void setUp() {
        expect(supplier.get()).andReturn(inner);
        expect(supplier.name()).andReturn("name");
        expect(supplier.metricsScope()).andReturn("metricScope");
        replay(supplier);

        builder = new SessionStoreBuilder<>(
            supplier,
            Serdes.String(),
            Serdes.String(),
            new MockTime());
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        final SessionStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredSessionStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        final SessionStore<String, String> store = builder.build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, instanceOf(ChangeLoggingSessionBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        final SessionStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertThat(next, CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        final SessionStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredSessionStore.class));
        assertThat(wrapped, instanceOf(CachingSessionStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        final SessionStore<String, String> store = builder
                .withLoggingEnabled(Collections.<String, String>emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(MeteredSessionStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingSessionBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrapped(), CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        final SessionStore<String, String> store = builder
                .withLoggingEnabled(Collections.<String, String>emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertThat(store, instanceOf(MeteredSessionStore.class));
        assertThat(caching, instanceOf(CachingSessionStore.class));
        assertThat(changeLogging, instanceOf(ChangeLoggingSessionBytesStore.class));
        assertThat(changeLogging.wrapped(), CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime()));
        assertThat(e.getMessage(), equalTo("storeSupplier cannot be null"));
    }

    @Test
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(supplier, null, Serdes.String(), new MockTime()));
        assertThat(e.getMessage(), equalTo("name cannot be null"));
    }

    @Test
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(supplier, Serdes.String(), null, new MockTime()));
        assertThat(e.getMessage(), equalTo("name cannot be null"));
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        reset(supplier);
        expect(supplier.name()).andReturn("name");
        replay(supplier);
        final Exception e = assertThrows(NullPointerException.class, () -> new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null));
        assertThat(e.getMessage(), equalTo("time cannot be null"));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        reset(supplier);
        expect(supplier.get()).andReturn(new RocksDBSessionStore(
            new RocksDBSegmentedBytesStore(
                "name",
                null,
                10L,
                5L,
                new SessionKeySchema())
        ));
        expect(supplier.name()).andReturn("name");
        replay(supplier);

        final Exception e = assertThrows(NullPointerException.class,
            () -> new SessionStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertThat(e.getMessage(), equalTo("storeSupplier's metricsScope can't be null"));
    }

}