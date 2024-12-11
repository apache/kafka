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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.AutoOffsetReset;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Objects;

/**
 * The {@code Consumed} class is used to define the optional parameters when using {@link StreamsBuilder} to
 * build instances of {@link KStream}, {@link KTable}, and {@link GlobalKTable}.
 * <p>
 * For example, you can read a topic as {@link KStream} with a custom timestamp extractor and specify the corresponding
 * key and value serdes like:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KStream<String, Long> stream = builder.stream(
 *   "topicName",
 *   Consumed.with(Serdes.String(), Serdes.Long())
 *           .withTimestampExtractor(new LogAndSkipOnInvalidTimestamp()));
 * }</pre>
 * Similarly, you can read a topic as {@link KTable} with a custom {@code auto.offset.reset} configuration and force a
 * state store {@link org.apache.kafka.streams.kstream.Materialized materialization} to access the content via
 * interactive queries:
 * <pre>{@code
 * StreamsBuilder builder = new StreamsBuilder();
 * KTable<Integer, Integer> table = builder.table(
 *   "topicName",
 *   Consumed.with(AutoOffsetReset.LATEST),
 *   Materialized.as("queryable-store-name"));
 * }</pre>
 *
 * @param <K> type of record key
 * @param <V> type of record value
 */
public class Consumed<K, V> implements NamedOperation<Consumed<K, V>> {

    protected Serde<K> keySerde;
    protected Serde<V> valueSerde;
    protected TimestampExtractor timestampExtractor;
    @Deprecated
    protected Topology.AutoOffsetReset legacyResetPolicy;
    protected AutoOffsetReset resetPolicy;
    protected String processorName;

    @SuppressWarnings("deprecation")
    private Consumed(final Serde<K> keySerde,
                     final Serde<V> valueSerde,
                     final TimestampExtractor timestampExtractor,
                     final Topology.AutoOffsetReset legacyResetPolicy,
                     final AutoOffsetReset resetPolicy,
                     final String processorName) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.timestampExtractor = timestampExtractor;
        this.legacyResetPolicy = legacyResetPolicy;
        this.resetPolicy = resetPolicy;
        this.processorName = processorName;
    }

    /**
     * Create an instance of {@link Consumed} from an existing instance.
     * @param consumed  the instance of {@link Consumed} to copy
     */
    protected Consumed(final Consumed<K, V> consumed) {
        this(
            consumed.keySerde,
            consumed.valueSerde,
            consumed.timestampExtractor,
            consumed.legacyResetPolicy,
            consumed.resetPolicy,
            consumed.processorName
        );
    }

    @Deprecated
    private static AutoOffsetReset convertOldToNew(final Topology.AutoOffsetReset resetPolicy) {
        if (resetPolicy == null) {
            return null;
        }

        return resetPolicy == org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST
            ? AutoOffsetReset.earliest()
            : AutoOffsetReset.latest();
    }

    /**
     * Create an instance of {@link Consumed} with the supplied arguments. {@code null} values are acceptable.
     *
     * @param keySerde
     *        the key serde. If {@code null} the default key serde from config will be used
     * @param valueSerde
     *        the value serde. If {@code null} the default value serde from config will be used
     * @param timestampExtractor
     *        the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
     * @param resetPolicy
     *        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     *
     * @deprecated Since 4.0. Use {@link #with(Serde, Serde, TimestampExtractor, AutoOffsetReset)} instead.
     */
    @Deprecated
    public static <K, V> Consumed<K, V> with(final Serde<K> keySerde,
                                             final Serde<V> valueSerde,
                                             final TimestampExtractor timestampExtractor,
                                             final Topology.AutoOffsetReset resetPolicy) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, resetPolicy, convertOldToNew(resetPolicy), null);
    }

    /**
     * Create an instance of {@link Consumed} with the supplied arguments. {@code null} values are acceptable.
     *
     * @param keySerde
     *        the key serde. If {@code null} the default key serde from config will be used
     * @param valueSerde
     *        the value serde. If {@code null} the default value serde from config will be used
     * @param timestampExtractor
     *        the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
     * @param resetPolicy
     *        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     */
    public static <K, V> Consumed<K, V> with(final Serde<K> keySerde,
                                             final Serde<V> valueSerde,
                                             final TimestampExtractor timestampExtractor,
                                             final AutoOffsetReset resetPolicy) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, null, resetPolicy, null);
    }

    /**
     * Create an instance of {@link Consumed} with key and value {@link Serde}s.
     *
     * @param keySerde
     *        the key serde. If {@code null} the default key serde from config will be used
     * @param valueSerde
     *        the value serde. If {@code null} the default value serde from config will be used
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     */
    public static <K, V> Consumed<K, V> with(final Serde<K> keySerde,
                                             final Serde<V> valueSerde) {
        return new Consumed<>(keySerde, valueSerde, null, null, null, null);
    }

    /**
     * Create an instance of {@link Consumed} with a {@link TimestampExtractor}.
     *
     * @param timestampExtractor
     *        the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     */
    public static <K, V> Consumed<K, V> with(final TimestampExtractor timestampExtractor) {
        return new Consumed<>(null, null, timestampExtractor, null, null, null);
    }

    /**
     * Create an instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
     *
     * @param resetPolicy
     *        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     *
     * @deprecated Since 4.0. Use {@link #with(AutoOffsetReset)} instead.
     */
    @Deprecated
    public static <K, V> Consumed<K, V> with(final Topology.AutoOffsetReset resetPolicy) {
        return new Consumed<>(null, null, null, resetPolicy, convertOldToNew(resetPolicy), null);
    }

    /**
     * Create an instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
     *
     * @param resetPolicy
     *        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     */
    public static <K, V> Consumed<K, V> with(final AutoOffsetReset resetPolicy) {
        return new Consumed<>(null, null, null, null, resetPolicy, null);
    }

    /**
     * Create an instance of {@link Consumed} with provided processor name.
     *
     * @param processorName
     *        the processor name to be used. If {@code null} a default processor name will be generated
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return a new instance of {@link Consumed}
     */
    public static <K, V> Consumed<K, V> as(final String processorName) {
        return new Consumed<>(null, null, null, null, null, processorName);
    }

    /**
     * Configure the instance of {@link Consumed} with a key {@link Serde}.
     *
     * @param keySerde
     *        the key serde. If {@code null} the default key serde from config will be used
     *
     * @return a new instance of {@link Consumed}
     */
    public Consumed<K, V> withKeySerde(final Serde<K> keySerde) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, legacyResetPolicy, resetPolicy, processorName);
    }

    /**
     * Configure the instance of {@link Consumed} with a value {@link Serde}.
     *
     * @param valueSerde
     *        the value serde. If {@code null} the default value serde from config will be used
     *
     * @return a new instance of {@link Consumed}
     */
    public Consumed<K, V> withValueSerde(final Serde<V> valueSerde) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, legacyResetPolicy, resetPolicy, processorName);
    }

    /**
     * Configure the instance of {@link Consumed} with a {@link TimestampExtractor}.
     *
     * @param timestampExtractor
     *        the timestamp extractor to used. If {@code null} the default timestamp extractor from config will be used
     *
     * @return a new instance of {@link Consumed}
     */
    public Consumed<K, V> withTimestampExtractor(final TimestampExtractor timestampExtractor) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, legacyResetPolicy, resetPolicy, processorName);
    }

    /**
     * Configure the instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
     *
     * @param resetPolicy
     *        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     *
     * @return a new instance of {@link Consumed}
     *
     * @deprecated Since 4.0. Use {@link #withOffsetResetPolicy(AutoOffsetReset)} instead.
     */
    @Deprecated
    public Consumed<K, V> withOffsetResetPolicy(final Topology.AutoOffsetReset resetPolicy) {
        return new Consumed<>(
            keySerde,
            valueSerde,
            timestampExtractor,
            resetPolicy,
            convertOldToNew(resetPolicy),
            processorName
        );
    }

    /**
     * Configure the instance of {@link Consumed} with a {@link org.apache.kafka.streams.Topology.AutoOffsetReset Topology.AutoOffsetReset}.
     *
     * @param resetPolicy
     *        the offset reset policy to be used. If {@code null} the default reset policy from config will be used
     *
     * @return a new instance of {@link Consumed}
     */
    public Consumed<K, V> withOffsetResetPolicy(final AutoOffsetReset resetPolicy) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, null, resetPolicy, processorName);
    }

    /**
     * Configure the instance of {@link Consumed} with a processor name.
     *
     * @param processorName
     *        the processor name to be used. If {@code null} a default processor name will be generated
     *
     * @return a new instance of {@link Consumed}
     */
    @Override
    public Consumed<K, V> withName(final String processorName) {
        return new Consumed<>(keySerde, valueSerde, timestampExtractor, legacyResetPolicy, resetPolicy, processorName);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Consumed<?, ?> consumed = (Consumed<?, ?>) o;
        return Objects.equals(keySerde, consumed.keySerde) &&
               Objects.equals(valueSerde, consumed.valueSerde) &&
               Objects.equals(timestampExtractor, consumed.timestampExtractor) &&
               legacyResetPolicy == consumed.legacyResetPolicy &&
               resetPolicy == consumed.resetPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySerde, valueSerde, timestampExtractor, legacyResetPolicy, resetPolicy);
    }
}
