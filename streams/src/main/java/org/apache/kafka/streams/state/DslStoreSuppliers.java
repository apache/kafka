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

import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.TopologyConfig;

/**
 * {@code DslStoreSuppliers} defines a grouping of factories to construct
 * stores for each of the types of state store implementations in Kafka
 * Streams. This allows configuration of a default store supplier beyond
 * the builtin defaults of RocksDB and In-Memory.
 *
 * <p>There are various ways that this configuration can be supplied to
 * the application (in order of precedence):
 * <ol>
 *     <li>Passed in directly to a DSL operator via either
 *     {@link org.apache.kafka.streams.kstream.Materialized#as(DslStoreSuppliers)},
 *     {@link org.apache.kafka.streams.kstream.Materialized#withStoreType(DslStoreSuppliers)}, or
 *     {@link org.apache.kafka.streams.kstream.StreamJoined#withDslStoreSuppliers(DslStoreSuppliers)}</li>
 *
 *     <li>Passed in via a Topology configuration override (configured in a
 *     {@link org.apache.kafka.streams.TopologyConfig} and passed into the
 *     {@link org.apache.kafka.streams.StreamsBuilder#StreamsBuilder(TopologyConfig)} constructor</li>
 *
 *     <li>Configured as a global default in {@link org.apache.kafka.streams.StreamsConfig} using
 *     the {@link org.apache.kafka.streams.StreamsConfig#DSL_STORE_SUPPLIERS_CLASS_CONFIG}</li>
 *     configuration.
 * </ol>
 *
 * <p>Kafka Streams is packaged with some pre-existing {@code DslStoreSuppliers}
 * that exist in {@link BuiltInDslStoreSuppliers}
 */
public interface DslStoreSuppliers extends Configurable {

    @Override
    default void configure(Map<String, ?> configs) {
        // optional to configure this class
    }

    KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams params);

    WindowBytesStoreSupplier windowStore(final DslWindowParams params);

    SessionBytesStoreSupplier sessionStore(final DslSessionParams params);

}