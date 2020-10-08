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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.junit.Assert.assertThrows;

@RunWith(value = Parameterized.class)
public class MockProcessorContextStateStoreTest {
    private final StoreBuilder<StateStore> builder;
    private final boolean timestamped;
    private final boolean caching;
    private final boolean logging;

    @Parameterized.Parameters(name = "builder = {0}, timestamped = {1}, caching = {2}, logging = {3}")
    public static Collection<Object[]> data() {
        final List<Boolean> booleans = asList(true, false);

        final List<Object[]> values = new ArrayList<>();

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

                        values.add(new Object[] {builder, timestamped, caching, logging});
                    }
                }
            }
        }

        for (final Boolean timestamped : booleans) {
            for (final Boolean caching : booleans) {
                for (final Boolean logging : booleans) {
                    final List<WindowBytesStoreSupplier> windowBytesStoreSuppliers = asList(
                        Stores.inMemoryWindowStore("w" + timestamped + caching + logging, Duration.ofSeconds(1), Duration.ofSeconds(1), false),
                        Stores.persistentWindowStore("w" + timestamped + caching + logging, Duration.ofSeconds(1), Duration.ofSeconds(1), false),
                        Stores.persistentTimestampedWindowStore("w" + timestamped + caching + logging, Duration.ofSeconds(1), Duration.ofSeconds(1), false)
                    );

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

                        values.add(new Object[] {builder, timestamped, caching, logging});
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

                    values.add(new Object[] {builder, false, caching, logging});
                }
            }
        }

        return values;
    }

    public MockProcessorContextStateStoreTest(final StoreBuilder<StateStore> builder,
                                              final boolean timestamped,
                                              final boolean caching,
                                              final boolean logging) {

        this.builder = builder;
        this.timestamped = timestamped;
        this.caching = caching;
        this.logging = logging;
    }

    @Test
    public void shouldEitherInitOrThrow() {
        final File stateDir = TestUtils.tempDirectory();
        try {
            final MockProcessorContext<Void, Void> context = new MockProcessorContext<>(
                mkProperties(mkMap(
                    mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, ""),
                    mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
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
            } else {
                store.init(context.getStateStoreContext(), store);
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
}
