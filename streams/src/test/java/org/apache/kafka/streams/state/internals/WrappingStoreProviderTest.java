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
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.InvalidStateStorePartitionException;
import org.apache.kafka.streams.state.NoOpWindowStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.test.StateStoreProviderStub;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.streams.state.QueryableStoreTypes.windowStore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class WrappingStoreProviderTest {

    private WrappingStoreProvider wrappingStoreProvider;

    private final int numStateStorePartitions = 2;

    @Before
    public void before() {
        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        final StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);

        for (int partition = 0; partition < numStateStorePartitions; partition++) {
            stubProviderOne.addStore("kv", partition, Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("kv"),
                    Serdes.serdeFrom(String.class),
                    Serdes.serdeFrom(String.class))
                    .build());
            stubProviderOne.addStore("window", partition, new NoOpWindowStore());
            wrappingStoreProvider = new WrappingStoreProvider(
                    Arrays.asList(stubProviderOne, stubProviderTwo),
                    StoreQueryParameters.fromNameAndType("kv", QueryableStoreTypes.keyValueStore())
            );
        }
    }

    @Test
    public void shouldFindKeyValueStores() {
        final List<ReadOnlyKeyValueStore<String, String>> results =
                wrappingStoreProvider.stores("kv", QueryableStoreTypes.<String, String>keyValueStore());
        assertEquals(2, results.size());
    }

    @Test
    public void shouldFindWindowStores() {
        wrappingStoreProvider.setStoreQueryParameters(StoreQueryParameters.fromNameAndType("window", windowStore()));
        final List<ReadOnlyWindowStore<Object, Object>>
                windowStores =
                wrappingStoreProvider.stores("window", windowStore());
        assertEquals(2, windowStores.size());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfNoStoreOfTypeFound() {
        wrappingStoreProvider.setStoreQueryParameters(StoreQueryParameters.fromNameAndType("doesn't exist", QueryableStoreTypes.<String, String>keyValueStore()));
        assertThrows(InvalidStateStoreException.class, () -> wrappingStoreProvider.stores("doesn't exist", QueryableStoreTypes.<String, String>keyValueStore()));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionIfNoPartitionFound() {
        final int invalidPartition = numStateStorePartitions + 1;
        wrappingStoreProvider.setStoreQueryParameters(StoreQueryParameters.fromNameAndType("kv", QueryableStoreTypes.<String, String>keyValueStore()).withPartition(invalidPartition));
        assertThrows(InvalidStateStorePartitionException.class, () -> wrappingStoreProvider.stores("kv", QueryableStoreTypes.<String, String>keyValueStore()));
    }

    @Test
    public void shouldReturnAllStoreWhenQueryWithoutPartition() {
        wrappingStoreProvider.setStoreQueryParameters(StoreQueryParameters.fromNameAndType("kv", QueryableStoreTypes.<String, String>keyValueStore()));
        final List<ReadOnlyKeyValueStore<String, String>> results =
                wrappingStoreProvider.stores("kv", QueryableStoreTypes.<String, String>keyValueStore());
        assertEquals(numStateStorePartitions, results.size());
    }

    @Test
    public void shouldReturnSingleStoreWhenQueryWithPartition() {
        wrappingStoreProvider.setStoreQueryParameters(StoreQueryParameters.fromNameAndType("kv", QueryableStoreTypes.<String, String>keyValueStore()).withPartition(numStateStorePartitions - 1));
        final List<ReadOnlyKeyValueStore<String, String>> results =
                wrappingStoreProvider.stores("kv", QueryableStoreTypes.<String, String>keyValueStore());
        assertEquals(1, results.size());
    }
}