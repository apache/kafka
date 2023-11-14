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
package org.apache.kafka.streams.processor.internals;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.processor.StateStore;

/**
 * What! Another mechanism for obtaining a {@link StateStore}? This isn't just
 * an abuse of Java-isms... there's good reason for it. Here's how they are
 * interconnected:
 *
 * <ul>
 *     <li>{@link org.apache.kafka.streams.state.StoreBuilder} is the innermost
 *     layer to provide state stores and is used directly by the PAPI.</li>
 *
 *     <li>{@link org.apache.kafka.streams.state.StoreSupplier} is used by the
 *     DSL to provide preconfigured state stores as well as type-safe stores
 *     (e.g. {@link org.apache.kafka.streams.state.KeyValueBytesStoreSupplier}.</li>
 *
 *     <li>{@link StoreFactory} (this class) is internal and not exposed to
 *     the users. It allows the above store specifications to be wrapped in
 *     an internal interface that can then be configured <i>after</i> the
 *     creation of the Topology but before the stores themselves are created.
 *     This allows Kafka Streams to respect configurations such as
 *     {@link StreamsConfig#DEFAULT_DSL_STORE_CONFIG} even if it isn't passed
 *     to {@link org.apache.kafka.streams.StreamsBuilder#StreamsBuilder(TopologyConfig)}</li>
 * </ul>
 */
public interface StoreFactory {

    default void configure(final StreamsConfig config) {
        // do nothing
    }

    StateStore build();

    long retentionPeriod();

    long historyRetention();

    Set<String> connectedProcessorNames();

    boolean loggingEnabled();

    String name();

    boolean isWindowStore();

    boolean isVersionedStore();

    Map<String, String> logConfig();

    StoreFactory withCachingDisabled();

    StoreFactory withLoggingDisabled();

    boolean isCompatibleWith(StoreFactory storeFactory);

}
