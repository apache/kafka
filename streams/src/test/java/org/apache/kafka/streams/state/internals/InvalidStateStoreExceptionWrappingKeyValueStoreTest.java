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
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamThreadNotRunningException;
import org.apache.kafka.streams.errors.StreamThreadRebalancingException;
import org.apache.kafka.streams.errors.StateStoreMigratedException;
import org.apache.kafka.streams.errors.StateStoreNotAvailableException;
import org.apache.kafka.streams.errors.internals.EmptyStateStoreException;
import org.apache.kafka.streams.errors.internals.StateStoreClosedException;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.StateStoreProviderStub;
import org.apache.kafka.test.ReadOnlyKeyValueStoreStub;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

public class InvalidStateStoreExceptionWrappingKeyValueStoreTest {

    private final String storeName = "my-store";

    @Before
    public void before() {

    }

    @Test
    public void shouldWrapStateStoreClosedException() {
        verifyAllMethodsThrowExceptionFromStateStoreProvider(State.REBALANCING,
                StateStoreClosedException.class, StateStoreMigratedException.class);
        verifyAllMethodsThrowExceptionFromStateStoreProvider(State.PENDING_SHUTDOWN,
                StateStoreClosedException.class, StateStoreNotAvailableException.class);

        verifyAllMethodsThrowExceptionFromStateStore(State.REBALANCING, StateStoreMigratedException.class);
        verifyAllMethodsThrowExceptionFromStateStore(State.PENDING_SHUTDOWN, StateStoreNotAvailableException.class);
    }

    @Test
    public void shouldWrapEmptyStateStoreException() {
        verifyAllMethodsThrowExceptionFromStateStoreProvider(State.REBALANCING,
                EmptyStateStoreException.class, StateStoreMigratedException.class);
        verifyAllMethodsThrowExceptionFromStateStoreProvider(State.PENDING_SHUTDOWN,
                EmptyStateStoreException.class, StateStoreNotAvailableException.class);
    }

    @Test
    public void shouldWrapStreamThreadNotRunningException() {
        verifyAllMethodsThrowExceptionFromStateStoreProvider(State.REBALANCING,
                StreamThreadNotRunningException.class, StreamThreadRebalancingException.class);
        verifyAllMethodsThrowExceptionFromStateStoreProvider(State.PENDING_SHUTDOWN,
                StreamThreadNotRunningException.class, StreamThreadNotRunningException.class);
    }

    private void verifyAllMethodsThrowExceptionFromStateStoreProvider(final KafkaStreams.State streamState,
                                                                      final Class<? extends InvalidStateStoreException> throwExceptionClass,
                                                                      final Class<? extends InvalidStateStoreException> expectExceptionClass) {
        verifyThrowExceptionFromStateStoreProvider(streamState, throwExceptionClass, expectExceptionClass,
            store -> store.get("anything"));
        verifyThrowExceptionFromStateStoreProvider(streamState, throwExceptionClass, expectExceptionClass,
            store -> store.approximateNumEntries());
        verifyThrowExceptionFromStateStoreProvider(streamState, throwExceptionClass, expectExceptionClass,
            store -> store.range("anything", "something"));
        verifyThrowExceptionFromStateStoreProvider(streamState, throwExceptionClass, expectExceptionClass,
            store -> store.all());
    }

    private void verifyAllMethodsThrowExceptionFromStateStore(final KafkaStreams.State streamState,
                                                              final Class<? extends InvalidStateStoreException> expectExceptionClass) {
        verifyThrowExceptionFromStateStore(streamState, expectExceptionClass,
            store -> store.get("anything"));
        verifyThrowExceptionFromStateStore(streamState, expectExceptionClass,
            store -> store.approximateNumEntries());
        verifyThrowExceptionFromStateStore(streamState, expectExceptionClass,
            store -> StreamsTestUtils.toList(store.range("anything", "something")));
        verifyThrowExceptionFromStateStore(streamState, expectExceptionClass,
            store -> StreamsTestUtils.toList(store.all()));
    }

    private <K, V> void verifyThrowExceptionFromStateStoreProvider(final KafkaStreams.State streamState,
                                                            final Class<? extends InvalidStateStoreException> throwExceptionClass,
                                                            final Class<? extends InvalidStateStoreException> expectExceptionClass,
                                                            final Consumer<ReadOnlyKeyValueStore<K, V>> storeConsumer) {
        try {
            final List<StateStoreProvider> providers = Collections.singletonList(new StateStoreProviderStub(throwExceptionClass));
            final CompositeReadOnlyKeyValueStore<K, V> store = createKeyValueStore(streamState, providers, storeName);
            storeConsumer.accept(store);
            fail("Should have thrown " + expectExceptionClass.getSimpleName());
        } catch (final InvalidStateStoreException e) {
            assertThat(e, instanceOf(expectExceptionClass));
        }
    }

    private <K, V> void verifyThrowExceptionFromStateStore(final KafkaStreams.State streamState,
                                                    final Class<? extends InvalidStateStoreException> expectExceptionClass,
                                                    final Consumer<ReadOnlyKeyValueStore<K, V>> storeConsumer) {
        try {
            final ReadOnlyKeyValueStoreStub keyValueStoreStub = new ReadOnlyKeyValueStoreStub(storeName);

            final StateStoreProviderStub providerStub = new StateStoreProviderStub(false);
            providerStub.addStore(storeName, keyValueStoreStub);
            keyValueStoreStub.close();      // close store to raise exception

            final List<StateStoreProvider> providers = Collections.singletonList(providerStub);
            final CompositeReadOnlyKeyValueStore<K, V> store = createKeyValueStore(streamState, providers, storeName);
            storeConsumer.accept(store);
            fail("Should have thrown " + expectExceptionClass.getSimpleName() + " with key value store");
        } catch (final InvalidStateStoreException e) {
            assertThat(e, instanceOf(expectExceptionClass));
        }
    }

    private static <K, V> CompositeReadOnlyKeyValueStore<K, V> createKeyValueStore(final KafkaStreams.State streamState,
                                                                            final List<StateStoreProvider> providers,
                                                                            final String storeName) {
        final KafkaStreams streams = StreamsTestUtils.mockStreams(streamState);
        return new CompositeReadOnlyKeyValueStore<>(streams, new WrappingStoreProvider(providers),
                QueryableStoreTypes.keyValueStore(), storeName);
    }

}
