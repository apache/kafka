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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TimestampedKeyValueStoreBuilderWithHeadersTest {

    @Mock
    private KeyValueBytesStoreSupplier supplier;
    @Mock
    private RocksDBTimestampedStoreWithHeaders inner;
    private TimestampedKeyValueStoreBuilderWithHeaders<String, String> builder;

    private void setUpWithoutInner() {
        when(supplier.name()).thenReturn("name");
        when(supplier.metricsScope()).thenReturn("metricScope");

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );
    }

    private void setUp() {
        when(supplier.get()).thenReturn(inner);
        setUpWithoutInner();
    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.build();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.build();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders.class, next);
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrapped();
        assertEquals(next, inner);
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        assertInstanceOf(CachingKeyValueStore.class, wrapped);
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        assertInstanceOf(ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders.class, wrapped);
        assertEquals(((WrappedStateStore) wrapped).wrapped(), inner);
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        setUp();
        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingEnabled(Collections.emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrapped();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrapped();
        assertInstanceOf(MeteredTimestampedKeyValueStoreWithHeaders.class, store);
        assertInstanceOf(CachingKeyValueStore.class, caching);
        assertInstanceOf(ChangeLoggingTimestampedKeyValueBytesStoreWithHeaders.class, changeLogging);
        assertEquals(changeLogging.wrapped(), inner);
    }

    @Test
    public void shouldNotWrapTimestampedByteStore() {
        setUp();
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("name", "metrics-scope"));

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
        assertInstanceOf(RocksDBTimestampedStoreWithHeaders.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldWrapTimestampKeyValueStoreAsHeadersStore() {
        setUp();
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("name", "metrics-scope"));

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();
        assertInstanceOf(TimestampedToHeadersStoreAdapter.class, ((WrappedStateStore) store).wrapped());
    }

    @Test
    public void shouldThrowNullPointerIfInnerIsNull() {
        setUpWithoutInner();
        assertThrows(NullPointerException.class, () ->
                new TimestampedKeyValueStoreBuilderWithHeaders<>(null, Serdes.String(), Serdes.String(), new MockTime()));
    }

    @Test
    public void shouldNotThrowNullPointerIfKeySerdeIsNull() {
        setUpWithoutInner();
        // does not throw
        new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test
    public void shouldNotThrowNullPointerIfValueSerdeIsNull() {
        setUpWithoutInner();
        // does not throw
        new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test
    public void shouldThrowNullPointerIfTimeIsNull() {
        setUpWithoutInner();
        assertThrows(NullPointerException.class, () ->
                new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, Serdes.String(), Serdes.String(), null));
    }

    @Test
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        setUpWithoutInner();
        when(supplier.metricsScope()).thenReturn(null);

        final Exception e = assertThrows(NullPointerException.class,
                () -> new TimestampedKeyValueStoreBuilderWithHeaders<>(supplier, Serdes.String(), Serdes.String(), new MockTime()));
        assertTrue(e.getMessage().contains("storeSupplier's metricsScope can't be null"));
    }

    @Test
    public void shouldThrowUsingIQv2ForInMemoryStores() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new InMemoryKeyValueStore("test-store"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final KeyQuery<String, ValueTimestampHeaders<String>> query =
                KeyQuery.withKey("test-key");

        final StateStore wrapped = ((WrappedStateStore) store).wrapped();

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> wrapped.query(query, PositionBound.unbounded(), new QueryConfig(false))
        );

        assertTrue(exception.getMessage().contains(
            "Queries (IQv2) are not supported by timestamped key-value stores with headers yet."));
    }

    @Test
    public void shouldThrowWhenUsingIQv2InHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(TimestampedToHeadersStoreAdapter.class, wrapped);

        final KeyQuery<String, ValueTimestampHeaders<String>> query =
                KeyQuery.withKey("test-key");

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> wrapped.query(query, PositionBound.unbounded(), new QueryConfig(false))
        );

        assertTrue(exception.getMessage().contains("Queries (IQv2) are not supported for timestamped key-value stores with headers yet."));
    }

    @Test
    public void shouldThrowWhenPlainKeyValueStoreIsProvided() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> builder.withLoggingDisabled().withCachingDisabled().build()
        );

        assertTrue(exception.getMessage().contains("Provided store must be a timestamped store"));
    }

    @Test
    public void shouldThrowUsingIQv2ForNativeHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertInstanceOf(RocksDBTimestampedStoreWithHeaders.class, wrapped);

        final KeyQuery<String, ValueTimestampHeaders<String>> query =
                KeyQuery.withKey("test-key");

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                () -> wrapped.query(query, PositionBound.unbounded(), new QueryConfig(false))
        );

        assertTrue(exception.getMessage().contains("Queries (IQv2) are not supported for timestamped key-value stores with headers yet."));
    }

    @Test
    public void shouldThrowOnGetPositionForInMemoryStores() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new InMemoryKeyValueStore("test-store"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                store::getPosition
        );

        assertTrue(exception.getMessage().contains("Position is not supported by timestamped key-value stores with headers yet."));
    }

    @Test
    public void shouldThrowOnGetPositionForHeadersStoreAdapter() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStore("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                store::getPosition
        );

        assertTrue(exception.getMessage().contains("Position is not supported by timestamped key-value stores with headers yet."));
    }

    @Test
    public void shouldThrowOnGetPositionForNativeHeadersStore() {
        when(supplier.name()).thenReturn("test-store");
        when(supplier.metricsScope()).thenReturn("metricScope");
        when(supplier.get()).thenReturn(new RocksDBTimestampedStoreWithHeaders("test-store", "metrics-scope"));

        builder = new TimestampedKeyValueStoreBuilderWithHeaders<>(
                supplier,
                Serdes.String(),
                Serdes.String(),
                new MockTime()
        );

        final TimestampedKeyValueStoreWithHeaders<String, String> store = builder
                .withLoggingDisabled()
                .withCachingDisabled()
                .build();

        final UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class,
                store::getPosition
        );

        assertTrue(exception.getMessage().contains("Position is not supported by timestamped key-value stores with headers yet."));
    }

}