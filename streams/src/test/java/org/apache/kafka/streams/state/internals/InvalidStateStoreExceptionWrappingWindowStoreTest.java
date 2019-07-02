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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StateStoreMigratedException;
import org.apache.kafka.streams.errors.StateStoreNotAvailableException;
import org.apache.kafka.streams.errors.internals.EmptyStateStoreException;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.test.StateStoreProviderStub;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.streams.KafkaStreams.State;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class InvalidStateStoreExceptionWrappingWindowStoreTest {

    private static final long WINDOW_SIZE = 30_000;
    private final String storeName = "window-store";

    @Before
    public void before() {

    }

    @Test
    public void shouldWrapStateStoreClosedException() {
        verifyAllMethodsThrowExceptionFromStateStoreProvider(KafkaStreams.State.REBALANCING,
                StateStoreClosedException.class, StateStoreMigratedException.class);
        verifyAllMethodsThrowExceptionFromStateStoreProvider(KafkaStreams.State.PENDING_SHUTDOWN,
                StateStoreClosedException.class, StateStoreNotAvailableException.class);

        verifyAllMethodsThrowExceptionFromStateStore(State.REBALANCING, StateStoreMigratedException.class);
        verifyAllMethodsThrowExceptionFromStateStore(State.PENDING_SHUTDOWN, StateStoreNotAvailableException.class);
    }

    @Test
    public void shouldWrapEmptyStateStoreException() {
        verifyAllMethodsThrowExceptionFromStateStoreProvider(KafkaStreams.State.REBALANCING,
                EmptyStateStoreException.class, StateStoreMigratedException.class);
        verifyAllMethodsThrowExceptionFromStateStoreProvider(KafkaStreams.State.PENDING_SHUTDOWN,
                EmptyStateStoreException.class, StateStoreNotAvailableException.class);
    }

    private void verifyAllMethodsThrowExceptionFromStateStoreProvider(final KafkaStreams.State streamState,
                                                                      final Class<? extends InvalidStateStoreException> actualExceptionClass,
                                                                      final Class<? extends InvalidStateStoreException> expectExceptionClass) {
        verifyThrowExceptionFromStateStoreProvider(streamState, actualExceptionClass, expectExceptionClass,
            windowStore -> windowStore.fetch("key", 0));

        verifyThrowExceptionFromStateStoreProvider(streamState, actualExceptionClass, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.fetch("key", ofEpochMilli(1), ofEpochMilli(10)))
        );

        verifyThrowExceptionFromStateStoreProvider(streamState, actualExceptionClass, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.fetch("a", "b", ofEpochMilli(1), ofEpochMilli(10)))
        );

        verifyThrowExceptionFromStateStoreProvider(streamState, actualExceptionClass, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.all()));

        verifyThrowExceptionFromStateStoreProvider(streamState, actualExceptionClass, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(1), ofEpochMilli(10))));

    }

    private void verifyAllMethodsThrowExceptionFromStateStore(final KafkaStreams.State streamState,
                                                              final Class<? extends InvalidStateStoreException> expectExceptionClass) {
        verifyThrowExceptionFromSateStore(streamState, expectExceptionClass,
            windowStore -> windowStore.fetch("key", 0));

        verifyThrowExceptionFromSateStore(streamState, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.fetch("key", ofEpochMilli(1), ofEpochMilli(10)))
        );

        verifyThrowExceptionFromSateStore(streamState, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.fetch("a", "b", ofEpochMilli(1), ofEpochMilli(10)))
        );

        verifyThrowExceptionFromSateStore(streamState, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.all()));

        verifyThrowExceptionFromSateStore(streamState, expectExceptionClass,
            windowStore -> StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(1), ofEpochMilli(10))));

    }


    private <K, V> void verifyThrowExceptionFromStateStoreProvider(final KafkaStreams.State streamState,
                                                            final Class<? extends InvalidStateStoreException> throwExceptionClass,
                                                            final Class<? extends InvalidStateStoreException> expectExceptionClass,
                                                            final Consumer<ReadOnlyWindowStore<K, V>> storeConsumer) {
        try {
            final List<StateStoreProvider> providers = Collections.singletonList(new StateStoreProviderStub(throwExceptionClass));
            final CompositeReadOnlyWindowStore<K, V> store = createWindowStore(streamState, providers, storeName);
            storeConsumer.accept(store);
            fail("Should have thrown " + expectExceptionClass.getSimpleName());
        } catch (final InvalidStateStoreException e) {
            assertThat(e, instanceOf(expectExceptionClass));
        }
    }

    private <K, V> void verifyThrowExceptionFromSateStore(final KafkaStreams.State streamState,
                                                   final Class<? extends InvalidStateStoreException> expectExceptionClass,
                                                   final Consumer<ReadOnlyWindowStore<K, V>> storeConsumer) {
        try {
            final ReadOnlyWindowStoreStub<K, V> windowStoreStub = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            final StateStoreProviderStub providerStub = new StateStoreProviderStub(false);
            providerStub.addStore(storeName, windowStoreStub);
            windowStoreStub.close();       // close store to raise exception

            final List<StateStoreProvider> providers = Collections.singletonList(providerStub);
            final CompositeReadOnlyWindowStore<K, V> store = createWindowStore(streamState, providers, storeName);
            storeConsumer.accept(store);

            fail("Should have thrown " + expectExceptionClass.getSimpleName() + " with window store");
        } catch (final InvalidStateStoreException e) {
            assertThat(e, instanceOf(expectExceptionClass));
        }
    }


    private static <K, V> CompositeReadOnlyWindowStore<K, V> createWindowStore(final KafkaStreams.State streamState,
                                                                               final List<StateStoreProvider> providers,
                                                                               final String storeName) {
        final KafkaStreams streams = StreamsTestUtils.mockStreams(streamState);
        return new CompositeReadOnlyWindowStore<>(streams, new WrappingStoreProvider(providers),
                QueryableStoreTypes.windowStore(), storeName);
    }

}
