/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.GlobalKTableImpl;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KTableSource;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * {@link KStreamBuilder} is a subclass of {@link TopologyBuilder} that provides the Kafka Streams DSL
 * for users to specify computational logic and translates the given logic to a {@link org.apache.kafka.streams.processor.internals.ProcessorTopology}.
 */
public class KStreamBuilder extends TopologyBuilder {

    private final AtomicInteger index = new AtomicInteger(0);

    /**
     * Create a new {@link KStreamBuilder} instance.
     */
    public KStreamBuilder() {
        super();
    }

    /**
     * Create a {@link KStream} instance from the specified topics.
     * The default deserializers specified in the config are used.
     * <p>
     * If multiple topics are specified there are nor ordering guaranteed for records from different topics.
     *
     * @param topics    the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(String... topics) {
        return stream(null, null, null, topics);
    }


    /**
     * Create a {@link KStream} instance from the specified topics.
     * The default deserializers specified in the config are used.
     * <p>
     * If multiple topics are specified there are nor ordering guaranteed for records from different topics.
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets available; acceptable values are earliest or latest
     * @param topics    the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(AutoOffsetReset offsetReset, String... topics) {
        return stream(offsetReset, null, null, topics);
    }


    /**
     * Create a {@link KStream} instance from the specified Pattern.
     * The default deserializers specified in the config are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created stream will read data from all of them,
     * and there is no ordering guarantee between records from different topics
     *
     * @param topicPattern    the Pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(Pattern topicPattern) {
        return stream(null, null, null, topicPattern);
    }

    /**
     * Create a {@link KStream} instance from the specified Pattern.
     * The default deserializers specified in the config are used.
     * <p>
     * If multiple topics are matched by the specified pattern, the created stream will read data from all of them,
     * and there is no ordering guarantee between records from different topics
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets available; acceptable values are earliest or latest
     * @param topicPattern    the Pattern to match for topic names
     * @return a {@link KStream} for topics matching the regex pattern.
     */
    public <K, V> KStream<K, V> stream(AutoOffsetReset offsetReset, Pattern topicPattern) {
        return stream(offsetReset, null, null, topicPattern);
    }



    /**
     * Create a {@link KStream} instance from the specified topics.
     * <p>
     * If multiple topics are specified there are nor ordering guaranteed for records from different topics.
     *
     * @param keySerde  key serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param valSerde  value serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param topics    the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(Serde<K> keySerde, Serde<V> valSerde, String... topics) {
        return stream(null, keySerde, valSerde, topics);
    }


    /**
     * Create a {@link KStream} instance from the specified topics.
     * <p>
     * If multiple topics are specified there are nor ordering guaranteed for records from different topics.
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets available; acceptable values are earliest or latest
     *
     * @param keySerde  key serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param valSerde  value serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param topics    the topic names; must contain at least one topic name
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(AutoOffsetReset offsetReset, Serde<K> keySerde, Serde<V> valSerde, String... topics) {
        String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(offsetReset, name,  keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topics);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }


    /**
     * Create a {@link KStream} instance from the specified Pattern.
     * <p>
     * If multiple topics are matched by the specified pattern, the created stream will read data from all of them,
     * and there is no ordering guarantee between records from different topics.
     *
     * @param keySerde  key serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param valSerde  value serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param topicPattern    the Pattern to match for topic names
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(Serde<K> keySerde, Serde<V> valSerde, Pattern topicPattern) {
        return stream(null, keySerde, valSerde, topicPattern);
    }

    /**
     * Create a {@link KStream} instance from the specified Pattern.
     * <p>
     * If multiple topics are matched by the specified pattern, the created stream will read data from all of them,
     * and there is no ordering guarantee between records from different topics.
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets available; acceptable values are earliest or latest
     * @param keySerde  key serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param valSerde  value serde used to read this source {@link KStream},
     *                  if not specified the default serde defined in the configs will be used
     * @param topicPattern    the Pattern to match for topic names
     * @return a {@link KStream} for the specified topics
     */
    public <K, V> KStream<K, V> stream(AutoOffsetReset offsetReset, Serde<K> keySerde, Serde<V> valSerde, Pattern topicPattern) {
        String name = newName(KStreamImpl.SOURCE_NAME);

        addSource(offsetReset, name, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topicPattern);

        return new KStreamImpl<>(this, name, Collections.singleton(name), false);
    }

    /**
     * Create a {@link KTable} instance for the specified topic.
     * Record keys of the topic should never by null, otherwise an exception will be thrown at runtime.
     * The default deserializers specified in the config are used.
     * The resulting {@link KTable} will be materialized in a local state store with the given store name.
     * However, no new changelog topic is created in this case since the underlying topic acts as one.
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets available; acceptable values are earliest or latest
     * @param topic     the topic name; cannot be null
     * @param storeName the state store name used if this KTable is materialized, can be null if materialization not expected
     * @return a {@link KTable} for the specified topics
     */
    public <K, V> KTable<K, V> table(AutoOffsetReset offsetReset, String topic, final String storeName) {
        return table(offsetReset, null, null, topic, storeName);
    }

    /**
     * Create a {@link KTable} instance for the specified topic.
     * Record keys of the topic should never by null, otherwise an exception will be thrown at runtime.
     * The default deserializers specified in the config are used.
     * The resulting {@link KTable} will be materialized in a local state store with the given store name.
     * However, no new changelog topic is created in this case since the underlying topic acts as one.
     *
     * @param topic     the topic name; cannot be null
     * @param storeName the state store name used if this KTable is materialized, can be null if materialization not expected
     * @return a {@link KTable} for the specified topics
     */
    public <K, V> KTable<K, V> table(String topic, final String storeName) {
        return table(null, null, null, topic, storeName);
    }


    /**
     * Create a {@link KTable} instance for the specified topic.
     * Record keys of the topic should never by null, otherwise an exception will be thrown at runtime.
     * The resulting {@link KTable} will be materialized in a local state store with the given store name.
     * However, no new changelog topic is created in this case since the underlying topic acts as one.
     *
     * @param keySerde   key serde used to send key-value pairs,
     *                   if not specified the default key serde defined in the configuration will be used
     * @param valSerde   value serde used to send key-value pairs,
     *                   if not specified the default value serde defined in the configuration will be used
     * @param topic      the topic name; cannot be null
     * @param storeName  the state store name used for the materialized KTable
     * @return a {@link KTable} for the specified topics
     */
    public <K, V> KTable<K, V> table(Serde<K> keySerde, Serde<V> valSerde, String topic, final String storeName) {
        return table(null, keySerde, valSerde, topic, storeName);
    }

    /**
     * Create a {@link KTable} instance for the specified topic.
     * Record keys of the topic should never by null, otherwise an exception will be thrown at runtime.
     * The resulting {@link KTable} will be materialized in a local state store with the given store name.
     * However, no new changelog topic is created in this case since the underlying topic acts as one.
     *
     * @param offsetReset the auto offset reset policy to use for this stream if no committed offsets available; acceptable values are earliest or latest
     * @param keySerde   key serde used to send key-value pairs,
     *                   if not specified the default key serde defined in the configuration will be used
     * @param valSerde   value serde used to send key-value pairs,
     *                   if not specified the default value serde defined in the configuration will be used
     * @param topic      the topic name; cannot be null
     * @param storeName  the state store name used if this KTable is materialized, can be null if materialization not expected
     * @return a {@link KTable} for the specified topics
     */
    public <K, V> KTable<K, V> table(AutoOffsetReset offsetReset, Serde<K> keySerde, Serde<V> valSerde, String topic, final String storeName) {
        final String source = newName(KStreamImpl.SOURCE_NAME);
        final String name = newName(KTableImpl.SOURCE_NAME);
        final ProcessorSupplier<K, V> processorSupplier = new KTableSource<>(storeName);

        addSource(offsetReset, source, keySerde == null ? null : keySerde.deserializer(), valSerde == null ? null : valSerde.deserializer(), topic);
        addProcessor(name, processorSupplier, source);

        final KTableImpl kTable = new KTableImpl<>(this, name, processorSupplier, Collections.singleton(source), storeName);
        StateStoreSupplier storeSupplier = new RocksDBKeyValueStoreSupplier<>(storeName,
            keySerde,
            valSerde,
            false,
            Collections.<String, String>emptyMap(),
            true);

        addStateStore(storeSupplier, name);
        connectSourceStoreAndTopic(storeName, topic);

        return kTable;
    }


    /**
     * Create a new  {@link GlobalKTable} instance for the specified topic.
     * Record keys of the topic should never by null, otherwise an exception will be thrown at runtime.
     * The resulting {@link GlobalKTable} will be materialized in a local state store with the given store name.
     * However, no new changelog topic is created in this case since the underlying topic acts as one.
     * @param keySerde   key serde used to send key-value pairs,
     *                   if not specified the default key serde defined in the configuration will be used
     * @param valSerde   value serde used to send key-value pairs,
     *                   if not specified the default value serde defined in the configuration will be used
     * @param topic      the topic name; cannot be null
     * @param storeName  the state store name used
     * @return a {@link GlobalKTable} for the specified topics
     */
    @SuppressWarnings("unchecked")
    public <K, V> GlobalKTable<K, V> globalTable(final Serde<K> keySerde, final Serde<V> valSerde, final String topic, final String storeName) {
        final String sourceName = newName(KStreamImpl.SOURCE_NAME);
        final String processorName = newName(KTableImpl.SOURCE_NAME);
        final KTableSource<K, V> tableSource = new KTableSource<>(storeName);


        final Deserializer<K> keyDeserializer = keySerde == null ? null : keySerde.deserializer();
        final Deserializer<V> valueDeserializer = valSerde == null ? null : valSerde.deserializer();

        final StateStore store = new RocksDBKeyValueStoreSupplier<>(storeName,
                                                                    keySerde,
                                                                    valSerde,
                                                                    false,
                                                                    Collections.<String, String>emptyMap(),
                                                                    true).get();

        addGlobalStore(store, sourceName, keyDeserializer, valueDeserializer, topic, processorName, tableSource);
        return new GlobalKTableImpl(new KTableSourceValueGetterSupplier<>(storeName));
    }

    /**
     * Create a new  {@link GlobalKTable} instance for the specified topic using the default key and value {@link Serde}s
     * Record keys of the topic should never by null, otherwise an exception will be thrown at runtime.
     * The resulting {@link GlobalKTable} will be materialized in a local state store with the given store name.
     * However, no new changelog topic is created in this case since the underlying topic acts as one.
     *
     * @param topic      the topic name; cannot be null
     * @param storeName  the state store name used if this KTable is materialized, can be null if materialization not expected
     * @return a {@link GlobalKTable} for the specified topics
     */
    public <K, V> GlobalKTable<K, V> globalTable(final String topic, final String storeName) {
        return globalTable(null, null, topic, storeName);
    }

    /**
     * Create a new instance of {@link KStream} by merging the given streams.
     * <p>
     * There are nor ordering guaranteed for records from different streams.
     *
     * @param streams   the instances of {@link KStream} to be merged
     * @return a {@link KStream} containing all records of the given streams
     */
    public <K, V> KStream<K, V> merge(KStream<K, V>... streams) {
        return KStreamImpl.merge(this, streams);
    }

    /**
     * Create a unique processor name used for translation into the processor topology.
     * This function is only for internal usage.
     *
     * @param prefix    processor name prefix
     * @return a new unique name
     */
    public String newName(String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }


}
