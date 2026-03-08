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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.RocksDbIndexedTimeOrderedWindowBytesStoreSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockProcessorContextStateStoreTest {

    public static Stream<Arguments> parameters() {
        final List<Boolean> booleans = asList(true, false);

        final List<Arguments> values = new ArrayList<>();

        for (final Boolean timestamped : booleans) {
            for (final Boolean caching : booleans) {
                for (final Boolean logging : booleans) {
                    final List<KeyValueBytesStoreSupplier> keyValueBytesStoreSuppliers = asList(
                        Stores.inMemoryKeyValueStore("kv" + timestamped + caching + logging),
                        Stores.persistentKeyValueStore("kv" + timestamped + caching + logging),
                        Stores.persistentTimestampedKeyValueStore("kv" + timestamped + caching + logging)
                    );
                    for (final KeyValueBytesStoreSupplier supplier : keyValueBytesStoreSuppliers) {
                        final StoreBuilder<? extends KeyValueStore<String, ?>> builder;
                        if (timestamped) {
                            builder = Stores.timestampedKeyValueStoreBuilder(supplier, Serdes.String(), Serdes.Long());
                        } else {
                            builder = Stores.keyValueStoreBuilder(supplier, Serdes.String(), Serdes.Long());
                        }
                        if (caching) {
                            builder.withCachingEnabled();
                        } else {
                            builder.withCachingDisabled();
                        }
                        if (logging) {
                            builder.withLoggingEnabled(Collections.emptyMap());
                        } else {
                            builder.withLoggingDisabled();
                        }

                        values.add(Arguments.of(builder, timestamped, caching, logging));
                    }
                }
            }
        }

        for (final Boolean timestamped : booleans) {
            for (final Boolean caching : booleans) {
                for (final Boolean logging : booleans) {
                    final List<WindowBytesStoreSupplier> windowBytesStoreSuppliers = new ArrayList<>(asList(
                        Stores.inMemoryWindowStore("w" + timestamped + caching + logging, Duration.ofSeconds(1), Duration.ofSeconds(1), false),
                        Stores.persistentWindowStore("w" + timestamped + caching + logging, Duration.ofSeconds(1), Duration.ofSeconds(1), false),
                        Stores.persistentTimestampedWindowStore("w" + timestamped + caching + logging, Duration.ofSeconds(1), Duration.ofSeconds(1), false)
                    ));
                    if (!timestamped) {
                        windowBytesStoreSuppliers.add(
                            RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                                "w-time-ordered-index" + caching + logging,
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(1),
                                false,
                                true
                            )
                        );
                        windowBytesStoreSuppliers.add(
                            RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                                "w-time-ordered-no-index" + caching + logging,
                                Duration.ofSeconds(1),
                                Duration.ofSeconds(1),
                                false,
                                false
                            )
                        );
                    }

                    for (final WindowBytesStoreSupplier supplier : windowBytesStoreSuppliers) {
                        final StoreBuilder<? extends WindowStore<String, ?>> builder;
                        if (timestamped) {
                            builder = Stores.timestampedWindowStoreBuilder(supplier, Serdes.String(), Serdes.Long());
                        } else {
                            builder = Stores.windowStoreBuilder(supplier, Serdes.String(), Serdes.Long());
                        }
                        if (caching) {
                            builder.withCachingEnabled();
                        } else {
                            builder.withCachingDisabled();
                        }
                        if (logging) {
                            builder.withLoggingEnabled(Collections.emptyMap());
                        } else {
                            builder.withLoggingDisabled();
                        }

                        values.add(Arguments.of(builder, timestamped, caching, logging));
                    }
                }
            }
        }

        for (final Boolean caching : booleans) {
            for (final Boolean logging : booleans) {
                final List<SessionBytesStoreSupplier> sessionBytesStoreSuppliers = asList(
                    Stores.inMemorySessionStore("s" + caching + logging, Duration.ofSeconds(1)),
                    Stores.persistentSessionStore("s" + caching + logging, Duration.ofSeconds(1))
                );

                for (final SessionBytesStoreSupplier supplier : sessionBytesStoreSuppliers) {
                    final StoreBuilder<? extends SessionStore<String, ?>> builder =
                        Stores.sessionStoreBuilder(supplier, Serdes.String(), Serdes.Long());
                    if (caching) {
                        builder.withCachingEnabled();
                    } else {
                        builder.withCachingDisabled();
                    }
                    if (logging) {
                        builder.withLoggingEnabled(Collections.emptyMap());
                    } else {
                        builder.withLoggingDisabled();
                    }

                    values.add(Arguments.of(builder, false, caching, logging));
                }
            }
        }

        return values.stream();
    }

    @ParameterizedTest
    @MethodSource(value = "parameters")
    public void shouldEitherInitOrThrow(final StoreBuilder<StateStore> builder,
                                        final boolean timestamped,
                                        final boolean caching,
                                        final boolean logging) {
        final File stateDir = TestUtils.tempDirectory();
        try {
            final MockProcessorContext<Void, Void> context = new MockProcessorContext<>(
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, ""),
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock-localhost:9092")
                )),
                new TaskId(0, 0),
                stateDir
            );
            final StateStore store = builder.build();
            if (caching || logging) {
                assertThrows(
                    IllegalArgumentException.class,
                    () -> store.init(context.getStateStoreContext(), store)
                );
            } else if (store instanceof WindowStore) {
                assertDoesNotThrow(() -> store.init(context.getStateStoreContext(), store));
                store.close();
            } else {
                final InternalProcessorContext<?, ?> internalProcessorContext = mock(InternalProcessorContext.class);
                when(internalProcessorContext.taskId()).thenReturn(context.taskId());
                when(internalProcessorContext.stateDir()).thenReturn(stateDir);
                when(internalProcessorContext.metrics()).thenReturn((StreamsMetricsImpl) context.metrics());
                when(internalProcessorContext.appConfigs()).thenReturn(context.appConfigs());
                store.init(internalProcessorContext, store);
                store.close();
            }
        } finally {
            try {
                Utils.delete(stateDir);
            } catch (final IOException e) {
                // Failed to clean up the state dir. The JVM hooks will try again later.
            }
        }
    }

    @Test
    public void shouldReadAndWriteInMemoryWindowStoreWithMockProcessorContextStateStoreContext() throws IOException {
        shouldReadAndWriteWindowStore(
            Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore("store-name", Duration.ofDays(1), Duration.ofDays(1), false),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    @Test
    public void shouldReadAndWriteInMemoryTimestampedWindowStoreWithMockProcessorContextStateStoreContext()
        throws IOException {
        shouldReadAndWriteTimestampedWindowStore(
            Stores.timestampedWindowStoreBuilder(
                Stores.inMemoryWindowStore("timestamped-store-name", Duration.ofDays(1), Duration.ofDays(1), false),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    @Test
    public void shouldReadAndWritePersistentWindowStoreWithMockProcessorContextStateStoreContext() throws IOException {
        shouldReadAndWriteWindowStore(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore("persistent-store-name", Duration.ofDays(1), Duration.ofDays(1), false),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    @Test
    public void shouldReadAndWritePersistentWindowStoreUsingTimestampedWindowStoreBuilderWithMockProcessorContextStateStoreContext()
        throws IOException {
        shouldReadAndWriteTimestampedWindowStore(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentWindowStore("persistent-window-store-for-timestamped-builder", Duration.ofDays(1), Duration.ofDays(1), false),
                Serdes.String(),
                Serdes.String()
            ),
            ConsumerRecord.NO_TIMESTAMP
        );
    }

    @Test
    public void shouldReadAndWritePersistentTimestampedWindowStoreUsingWindowStoreBuilderWithMockProcessorContextStateStoreContext()
        throws IOException {
        shouldReadAndWriteWindowStore(
            Stores.windowStoreBuilder(
                Stores.persistentTimestampedWindowStore("persistent-timestamped-store-for-window-builder", Duration.ofDays(1), Duration.ofDays(1), false),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    @Test
    public void shouldReadAndWritePersistentTimestampedWindowStoreWithMockProcessorContextStateStoreContext() throws IOException {
        shouldReadAndWriteTimestampedWindowStore(
            Stores.timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore("persistent-timestamped-store-name", Duration.ofDays(1), Duration.ofDays(1), false),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    @Test
    public void shouldReadAndWriteTimeOrderedWindowStoreWithIndexWithMockProcessorContextStateStoreContext() throws IOException {
        shouldReadAndWriteWindowStore(
            Stores.windowStoreBuilder(
                RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                    "time-ordered-window-store-with-index",
                    Duration.ofDays(1),
                    Duration.ofDays(1),
                    false,
                    true
                ),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    @Test
    public void shouldReadAndWriteTimeOrderedWindowStoreWithoutIndexWithMockProcessorContextStateStoreContext() throws IOException {
        shouldReadAndWriteWindowStore(
            Stores.windowStoreBuilder(
                RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(
                    "time-ordered-window-store-without-index",
                    Duration.ofDays(1),
                    Duration.ofDays(1),
                    false,
                    false
                ),
                Serdes.String(),
                Serdes.String()
            )
        );
    }

    private void shouldReadAndWriteWindowStore(final StoreBuilder<? extends WindowStore<String, String>> builder)
        throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final WindowStore<String, String> store = builder
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

        try {
            final MockProcessorContext<Void, Void> context = new MockProcessorContext<>(
                testProperties(),
                new TaskId(0, 0),
                stateDir
            );

            store.init(context.getStateStoreContext(), store);

            store.put("fresh", "fresh-value", 1_000L);
            assertEquals("fresh-value", store.fetch("fresh", 1_000L));

            store.put("advance-stream-time", "advance-value", Duration.ofDays(3).toMillis());
            assertDoesNotThrow(() -> store.put("expired", "expired-value", 1_000L));
            assertNull(store.fetch("expired", 1_000L));
        } finally {
            if (store.isOpen()) {
                store.close();
            }
            Utils.delete(stateDir);
        }
    }

    private void shouldReadAndWriteTimestampedWindowStore(
        final StoreBuilder<? extends TimestampedWindowStore<String, String>> builder
    ) throws IOException {
        shouldReadAndWriteTimestampedWindowStore(builder, 1_000L);
    }

    private void shouldReadAndWriteTimestampedWindowStore(
        final StoreBuilder<? extends TimestampedWindowStore<String, String>> builder,
        final long expectedFetchTimestamp
    ) throws IOException {
        final File stateDir = TestUtils.tempDirectory();
        final TimestampedWindowStore<String, String> store = builder
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

        try {
            final MockProcessorContext<Void, Void> context = new MockProcessorContext<>(
                testProperties(),
                new TaskId(0, 0),
                stateDir
            );

            store.init(context.getStateStoreContext(), store);

            store.put("fresh", ValueAndTimestamp.make("fresh-value", 1_000L), 1_000L);
            assertEquals(ValueAndTimestamp.make("fresh-value", expectedFetchTimestamp), store.fetch("fresh", 1_000L));

            store.put(
                "advance-stream-time",
                ValueAndTimestamp.make("advance-value", Duration.ofDays(3).toMillis()),
                Duration.ofDays(3).toMillis()
            );
            assertDoesNotThrow(() -> store.put("expired", ValueAndTimestamp.make("expired-value", 1_000L), 1_000L));
            assertNull(store.fetch("expired", 1_000L));
        } finally {
            if (store.isOpen()) {
                store.close();
            }
            Utils.delete(stateDir);
        }
    }

    private Properties testProperties() {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "mock-processor-context-test"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock-localhost:9092")
        ));
    }
}
