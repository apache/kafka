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

import org.apache.kafka.streams.state.HeadersBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStoreWithHeaders;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VersionedKeyValueStoreBuilderWithHeadersTest {

    @Test
    public void shouldCreateSupplierWithDualInterface() {
        final VersionedBytesStoreSupplier supplier =
            Stores.persistentVersionedKeyValueStoreWithHeaders("test-store", Duration.ofMinutes(5));

        assertNotNull(supplier);
        assertInstanceOf(VersionedBytesStoreSupplier.class, supplier);
        assertInstanceOf(HeadersBytesStoreSupplier.class, supplier);
        assertInstanceOf(RocksDbVersionedKeyValueBytesStoreWithHeadersSupplier.class, supplier);
    }

    @Test
    public void shouldRejectNullName() {
        assertThrows(NullPointerException.class,
            () -> Stores.persistentVersionedKeyValueStoreWithHeaders(null, Duration.ofMinutes(5)));
    }

    @Test
    public void shouldRejectNegativeHistoryRetention() {
        assertThrows(IllegalArgumentException.class,
            () -> Stores.persistentVersionedKeyValueStoreWithHeaders("test", Duration.ofMillis(-1)));
    }

    @Test
    public void shouldCreateBuilderWithHeaders() {
        final var supplier =
            Stores.persistentVersionedKeyValueStoreWithHeaders("test-store", Duration.ofMinutes(5));
        final var builder = Stores.versionedKeyValueStoreBuilderWithHeaders(supplier, null, null);

        assertNotNull(builder);
        assertInstanceOf(VersionedKeyValueStoreBuilderWithHeaders.class, builder);
    }

    @Test
    public void shouldBuildHeadersAwareMeteredStore() {
        final var supplier =
            Stores.persistentVersionedKeyValueStoreWithHeaders("test-store", Duration.ofMinutes(5));
        final var builder = Stores.versionedKeyValueStoreBuilderWithHeaders(supplier, null, null);

        assertInstanceOf(VersionedKeyValueStoreWithHeaders.class, builder.build());
    }

    @Test
    public void shouldRejectNullSupplier() {
        assertThrows(NullPointerException.class,
            () -> Stores.versionedKeyValueStoreBuilderWithHeaders(null, null, null));
    }

    @Test
    public void shouldRejectCachingOnBuilder() {
        final var supplier =
            Stores.persistentVersionedKeyValueStoreWithHeaders("test-store", Duration.ofMinutes(5));
        final var builder = Stores.versionedKeyValueStoreBuilderWithHeaders(supplier, null, null);

        assertThrows(IllegalStateException.class, builder::withCachingEnabled);
    }
}
