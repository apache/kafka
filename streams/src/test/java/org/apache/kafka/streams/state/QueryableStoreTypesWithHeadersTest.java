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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serdes;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryableStoreTypesWithHeadersTest {

    @Test
    public void shouldAcceptTimestampedKeyValueStoreWithHeadersForTimestampedKeyValueStoreType() {
        final TimestampedKeyValueStoreWithHeaders<String, String> store =
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.inMemoryKeyValueStore("test-store"),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> storeType =
            QueryableStoreTypes.timestampedKeyValueStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptTimestampedKeyValueStoreWithHeadersForKeyValueStoreType() {
        final TimestampedKeyValueStoreWithHeaders<String, String> store =
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.inMemoryKeyValueStore("test-store"),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyKeyValueStore<String, String>> storeType =
            QueryableStoreTypes.keyValueStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldNotAcceptTimestampedKeyValueStoreWithHeadersForWindowStoreType() {
        final TimestampedKeyValueStoreWithHeaders<String, String> store =
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.inMemoryKeyValueStore("test-store"),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyWindowStore<String, String>> storeType =
            QueryableStoreTypes.windowStore();

        assertFalse(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptTimestampedWindowStoreWithHeadersForTimestampedWindowStoreType() {
        final TimestampedWindowStoreWithHeaders<String, String> store =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "test-window-store",
                    Duration.ofMillis(100),
                    Duration.ofMillis(10),
                    false),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyWindowStore<String, ValueAndTimestamp<String>>> storeType =
            QueryableStoreTypes.timestampedWindowStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptTimestampedWindowStoreWithHeadersForWindowStoreType() {
        final TimestampedWindowStoreWithHeaders<String, String> store =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "test-window-store",
                    Duration.ofMillis(100),
                    Duration.ofMillis(10),
                    false),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyWindowStore<String, String>> storeType =
            QueryableStoreTypes.windowStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldNotAcceptTimestampedWindowStoreWithHeadersForKeyValueStoreType() {
        final TimestampedWindowStoreWithHeaders<String, String> store =
            Stores.timestampedWindowStoreWithHeadersBuilder(
                Stores.inMemoryWindowStore(
                    "test-window-store",
                    Duration.ofMillis(100),
                    Duration.ofMillis(10),
                    false),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyKeyValueStore<String, String>> storeType =
            QueryableStoreTypes.keyValueStore();

        assertFalse(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptRegularTimestampedKeyValueStoreForTimestampedKeyValueStoreType() {
        final TimestampedKeyValueStore<String, String> store =
            Stores.timestampedKeyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("test-ts-store"),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>> storeType =
            QueryableStoreTypes.timestampedKeyValueStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptRegularKeyValueStoreForKeyValueStoreType() {
        final KeyValueStore<String, String> store =
            Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("test-store"),
                    Serdes.String(),
                    Serdes.String())
                .build();

        final QueryableStoreType<ReadOnlyKeyValueStore<String, String>> storeType =
            QueryableStoreTypes.keyValueStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptRegularTimestampedWindowStoreForTimestampedWindowStoreType() {
        final TimestampedWindowStore<String, String> store =
            Stores.timestampedWindowStoreBuilder(
                Stores.inMemoryWindowStore(
                    "test-ts-window-store",
                    Duration.ofMillis(100),
                    Duration.ofMillis(10),
                    false),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlyWindowStore<String, ValueAndTimestamp<String>>> storeType =
            QueryableStoreTypes.timestampedWindowStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptRegularWindowStoreForWindowStoreType() {
        final WindowStore<String, String> store =
            Stores.windowStoreBuilder(
                    Stores.inMemoryWindowStore(
                        "test-window-store",
                        Duration.ofMillis(100),
                        Duration.ofMillis(10),
                        false),
                    Serdes.String(),
                    Serdes.String())
                .build();

        final QueryableStoreType<ReadOnlyWindowStore<String, String>> storeType =
            QueryableStoreTypes.windowStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptSessionStoreWithHeadersForSessionStoreType() {
        final SessionStoreWithHeaders<String, String> store =
            Stores.sessionStoreBuilderWithHeaders(
                Stores.inMemorySessionStore(
                    "test-session-store",
                    Duration.ofMillis(100)),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlySessionStore<String, ValueAndTimestamp<String>>> storeType =
            QueryableStoreTypes.sessionStore();

        assertTrue(storeType.accepts(store));
    }

    @Test
    public void shouldAcceptRegularSessionStoreForSessionStoreType() {
        final SessionStore<String, String> store =
            Stores.sessionStoreBuilder(
                Stores.inMemorySessionStore(
                    "test-session-store",
                    Duration.ofMillis(100)),
                Serdes.String(),
                Serdes.String())
            .build();

        final QueryableStoreType<ReadOnlySessionStore<String, String>> storeType =
            QueryableStoreTypes.sessionStore();

        assertTrue(storeType.accepts(store));
    }
}