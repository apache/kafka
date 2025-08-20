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

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * A store supplier that can be used to create one or more versioned key-value stores,
 * specifically, {@link VersionedBytesStore} instances.
 * <p>
 * Rather than representing the returned store as a {@link VersionedKeyValueStore} of
 * type &lt;Bytes, byte[]&gt;, this supplier interface represents the returned store as a
 * {@link KeyValueStore} of type &lt;Bytes, byte[]&gt; (via {@link VersionedBytesStore}) in order to be compatible with
 * existing DSL methods for passing key-value stores such as {@link StreamsBuilder#table(String, Materialized)}
 * and {@link KTable#filter(Predicate, Materialized)}. A {@code VersionedKeyValueStore<Bytes, byte[]>}
 * is represented as a {@code KeyValueStore KeyValueStore<Bytes, byte[]>} by interpreting the
 * value bytes as containing record timestamp information in addition to raw record values.
 */
public interface VersionedBytesStoreSupplier extends KeyValueBytesStoreSupplier {

    /**
     * Returns the history retention (in milliseconds) that stores created from this supplier will have.
     * This value is used to set compaction configs on store changelog topics (if relevant).
     *
     * @return history retention, i.e., length of time that old record versions are available for
     *         query from a versioned store
     */
    long historyRetentionMs();
}