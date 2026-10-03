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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.HeadersBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDbVersionedKeyValueBytesStoreWithHeadersSupplier;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilderWithHeaders;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class KeyValueStoreMaterializerWithHeadersTest {

    @Test
    public void shouldReturnDualInterfaceSupplier() {
        final VersionedBytesStoreSupplier supplier =
            Stores.persistentVersionedKeyValueStoreWithHeaders("test-store", Duration.ofMinutes(5));

        assertInstanceOf(VersionedBytesStoreSupplier.class, supplier);
        assertInstanceOf(HeadersBytesStoreSupplier.class, supplier);
        assertInstanceOf(RocksDbVersionedKeyValueBytesStoreWithHeadersSupplier.class, supplier);
    }

    @Test
    public void shouldRouteVersionedHeadersSupplierToVersionedHeadersBuilder() {
        final VersionedBytesStoreSupplier supplier =
            Stores.persistentVersionedKeyValueStoreWithHeaders("test-store", Duration.ofMinutes(5));
        final MaterializedInternal<String, String, KeyValueStore<Bytes, byte[]>> materialized =
            new MaterializedInternal<>(Materialized.as(supplier));

        final StoreBuilder<?> builder = new KeyValueStoreMaterializer<>(materialized).builder();

        assertInstanceOf(VersionedKeyValueStoreBuilderWithHeaders.class, builder);
    }
}
