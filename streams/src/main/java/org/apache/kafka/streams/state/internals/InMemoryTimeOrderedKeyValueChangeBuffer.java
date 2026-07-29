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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordQueue;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.DeserializationResult;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV0;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV1;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.deserializeV3;
import static org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBufferChangelogDeserializationHelper.duckTypeV2;

public final class InMemoryTimeOrderedKeyValueChangeBuffer<K, V, T> implements TimeOrderedKeyValueBuffer<K, V, Change<V>> {
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();
    private static final byte[] V_1_CHANGELOG_HEADER_VALUE = {(byte) 1};
    private static final byte[] V_2_CHANGELOG_HEADER_VALUE = {(byte) 2};
    private static final byte[] V_3_CHANGELOG_HEADER_VALUE = {(byte) 3};
    static final RecordHeaders CHANGELOG_HEADERS =
        new RecordHeaders(new Header[] {new RecordHeader("v", V_3_CHANGELOG_HEADER_VALUE)});
    // The prior and old value parts' headers/timestamp travel in the changelog record's own Kafka headers -- one per
    // part, holding that part's [headersSize][headers][timestamp] prefix -- so the value bytes stay V3 and remain
    // restorable by older versions, which simply ignore these headers. The new value part needs no such header: the
    // record context encoded in the V3 value already describes it, because put() captures the context of the very
    // record the new value came from.
    static final String PRIOR_VALUE_HEADERS_KEY = "vh.prior";
    static final String OLD_VALUE_HEADERS_KEY = "vh.old";
    private static final String METRIC_SCOPE = "in-memory-suppression";

    private final Map<Bytes, BufferKey> index = new HashMap<>();
    private final TreeMap<BufferKey, BufferValue> sortedMap = new TreeMap<>();

    private final Set<Bytes> dirtyKeys = new HashSet<>();
    private final String storeName;
    private final boolean loggingEnabled;

    private Serde<K> keySerde;
    private FullChangeSerde<V> valueSerde;

    // When headers-aware stores are enabled (dsl.store.format=HEADERS) each buffered value part is
    // stored as a ValueTimestampHeaders blob so that the old and new value can carry their own
    // headers and timestamp independently. Otherwise plain values are stored (the pre-existing V3
    // behavior) to avoid inflating memory/changelog size for users who do not use header stores.
    private boolean storeHeaders;
    private ValueTimestampHeadersSerializer<V> valueTimestampHeadersSerializer;
    private ValueTimestampHeadersDeserializer<V> valueTimestampHeadersDeserializer;

    private long memBufferSize = 0L;
    private long minTimestamp = Long.MAX_VALUE;
    private InternalProcessorContext<?, ?> context;
    private String changelogTopic;
    private Sensor bufferSizeSensor;
    private Sensor bufferCountSensor;
    private StreamsMetricsImpl streamsMetrics;
    private String taskId;

    private volatile boolean open;

    private int partition;

    public static class Builder<K, V> implements StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> {

        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private boolean loggingEnabled = true;
        private Map<String, String> logConfig = new HashMap<>();

        public Builder(final String storeName, final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.storeName = storeName;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         * <p>
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withCachingEnabled() {
            return this;
        }

        /**
         * As of 2.1, there's no way for users to directly interact with the buffer,
         * so this method is implemented solely to be called by Streams (which
         * it will do based on the {@code cache.max.bytes.buffering} config.
         * <p>
         * It's currently a no-op.
         */
        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withCachingDisabled() {
            return this;
        }

        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withLoggingEnabled(final Map<String, String> config) {
            logConfig = config;
            return this;
        }

        @Override
        public StoreBuilder<InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>>> withLoggingDisabled() {
            loggingEnabled = false;
            return this;
        }

        @Override
        public InMemoryTimeOrderedKeyValueChangeBuffer<K, V, Change<V>> build() {
            return new InMemoryTimeOrderedKeyValueChangeBuffer<>(storeName, loggingEnabled, keySerde, valueSerde);
        }

        @Override
        public Map<String, String> logConfig() {
            return loggingEnabled() ? Collections.unmodifiableMap(logConfig) : Collections.emptyMap();
        }

        @Override
        public boolean loggingEnabled() {
            return loggingEnabled;
        }

        @Override
        public String name() {
            return storeName;
        }
    }

    private InMemoryTimeOrderedKeyValueChangeBuffer(final String storeName,
                                                    final boolean loggingEnabled,
                                                    final Serde<K> keySerde,
                                                    final Serde<V> valueSerde) {
        this.storeName = storeName;
        this.loggingEnabled = loggingEnabled;
        this.keySerde = keySerde;
        this.valueSerde = FullChangeSerde.wrap(valueSerde);
    }

    @Override
    public String name() {
        return storeName;
    }


    @Override
    public boolean persistent() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setSerdesIfNull(final SerdeGetter getter) {
        keySerde = keySerde == null ? (Serde<K>) getter.keySerde() : keySerde;
        valueSerde = valueSerde == null ? FullChangeSerde.wrap((Serde<V>) getter.valueSerde()) : valueSerde;
    }
    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        this.context = ProcessorContextUtils.asInternalProcessorContext(stateStoreContext);
        changelogTopic = ProcessorContextUtils.changelogFor(stateStoreContext, name(), Boolean.TRUE);
        taskId = context.taskId().toString();
        streamsMetrics = context.metrics();

        final Object dslStoreFormat = stateStoreContext.appConfigs().get(StreamsConfig.DSL_STORE_FORMAT_CONFIG);
        storeHeaders = dslStoreFormat != null
            && StreamsConfig.DSL_STORE_FORMAT_HEADERS.equalsIgnoreCase(dslStoreFormat.toString());

        bufferSizeSensor = StateStoreMetrics.suppressionBufferSizeSensor(
            taskId,
            METRIC_SCOPE,
            storeName,
            streamsMetrics
        );
        bufferCountSensor = StateStoreMetrics.suppressionBufferCountSensor(
            taskId,
            METRIC_SCOPE,
            storeName,
            streamsMetrics
        );

        this.context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        updateBufferMetrics();
        open = true;
        partition = context.taskId().partition();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("This store does not keep track of the position.");
    }

    @Override
    public void close() {
        open = false;
        index.clear();
        sortedMap.clear();
        dirtyKeys.clear();
        memBufferSize = 0;
        minTimestamp = Long.MAX_VALUE;
        updateBufferMetrics();
        streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId, storeName);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        if (loggingEnabled) {
            // counting on this getting called before the record collector's flush
            for (final Bytes key : dirtyKeys) {

                final BufferKey bufferKey = index.get(key);

                if (bufferKey == null) {
                    // The record was evicted from the buffer. Send a tombstone.
                    logTombstone(key);
                } else {
                    final BufferValue value = sortedMap.get(bufferKey);

                    logValue(key, bufferKey, value);
                }
            }
            dirtyKeys.clear();
        }
    }

    private void logValue(final Bytes key, final BufferKey bufferKey, final BufferValue value) {

        final int sizeOfBufferTime = Long.BYTES;
        // The logged value is always in the V3 format: in headers mode the in-memory value parts are
        // ValueTimestampHeaders blobs, so split each one and log only its plain value bytes, sending
        // the headers/timestamp prefixes alongside in the record's Kafka headers.
        final ByteBuffer buffer = (storeHeaders ? plainEncoded(value) : value).serialize(sizeOfBufferTime);
        buffer.putLong(bufferKey.time());
        final byte[] array = buffer.array();
        ((RecordCollector.Supplier) context).recordCollector().send(
            changelogTopic,
            key,
            array,
            storeHeaders ? changelogHeadersWithValuePartHeaders(value) : CHANGELOG_HEADERS,
            partition,
            null,
            KEY_SERIALIZER,
            VALUE_SERIALIZER,
            null,
            null);
    }

    private static BufferValue plainEncoded(final BufferValue value) {
        return new BufferValue(
            Utils.rawPlainValue(value.priorValue()),
            Utils.rawPlainValue(value.oldValue()),
            Utils.rawPlainValue(value.newValue()),
            value.context()
        );
    }

    private static RecordHeaders changelogHeadersWithValuePartHeaders(final BufferValue value) {
        final RecordHeaders headers = new RecordHeaders(new Header[] {new RecordHeader("v", V_3_CHANGELOG_HEADER_VALUE)});
        // BufferValue collapses the prior and old values onto one array whenever they are equal -- the
        // common case, since on the first buffering of a key the prior value IS the old value -- and
        // serialize() then writes those bytes only once. Skip the duplicate prefix as well; restoring
        // recovers the prior part from vh.old whenever vh.prior is absent.
        if (value.priorValue() != value.oldValue()) {
            addValuePartHeader(headers, PRIOR_VALUE_HEADERS_KEY, value.priorValue());
        }
        addValuePartHeader(headers, OLD_VALUE_HEADERS_KEY, value.oldValue());
        return headers;
    }

    private static void addValuePartHeader(final RecordHeaders headers,
                                           final String headerKey,
                                           final byte[] rawValueTimestampHeaders) {
        if (rawValueTimestampHeaders != null) {
            headers.add(new RecordHeader(headerKey, Utils.rawHeadersTimestampPrefix(rawValueTimestampHeaders)));
        }
    }

    private void logTombstone(final Bytes key) {
        ((RecordCollector.Supplier) context).recordCollector().send(
            changelogTopic,
            key,
            null,
            null,
            partition,
            null,
            KEY_SERIALIZER,
            VALUE_SERIALIZER,
            null,
            null);
    }

    private void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> batch) {
        for (final ConsumerRecord<byte[], byte[]> record : batch) {
            if (record.partition() != partition) {
                throw new IllegalStateException(
                    String.format(
                        "record partition [%d] is being restored by the wrong suppress partition [%d]",
                        record.partition(),
                        partition
                    )
                );
            }
            final Bytes key = Bytes.wrap(record.key());
            if (record.value() == null) {
                // This was a tombstone. Delete the record.
                final BufferKey bufferKey = index.remove(key);
                if (bufferKey != null) {
                    final BufferValue removed = sortedMap.remove(bufferKey);
                    if (removed != null) {
                        memBufferSize -= computeRecordSize(bufferKey.key(), removed);
                    }
                    if (bufferKey.time() == minTimestamp) {
                        minTimestamp = sortedMap.isEmpty() ? Long.MAX_VALUE : sortedMap.firstKey().time();
                    }
                }
            } else {
                final Header versionHeader = record.headers().lastHeader("v");
                final DeserializationResult deserializationResult;
                if (versionHeader == null) {
                    // Version 0:
                    // value:
                    //  - buffer time
                    //  - old value
                    //  - new value
                    final byte[] previousBufferedValue = index.containsKey(key)
                        ? plainPriorValueForBuffered(key)
                        : null;
                    deserializationResult = deserializeV0(record, key, previousBufferedValue);
                } else if (Arrays.equals(versionHeader.value(), V_3_CHANGELOG_HEADER_VALUE)) {
                    // Version 3:
                    // value:
                    //  - record context
                    //  - prior value
                    //  - old value
                    //  - new value
                    //  - buffer time
                    deserializationResult = deserializeV3(record, key);
                } else if (Arrays.equals(versionHeader.value(), V_2_CHANGELOG_HEADER_VALUE)) {
                    // Version 2:
                    // value:
                    //  - record context
                    //  - old value
                    //  - new value
                    //  - prior value
                    //  - buffer time
                    // NOTE: 2.4.0, 2.4.1, and 2.5.0 actually encode Version 3 formatted data,
                    // but still set the Version 2 flag, so to deserialize, we have to duck type.
                    deserializationResult = duckTypeV2(record, key);
                } else if (Arrays.equals(versionHeader.value(), V_1_CHANGELOG_HEADER_VALUE)) {
                    // Version 1:
                    // value:
                    //  - buffer time
                    //  - record context
                    //  - old value
                    //  - new value
                    final byte[] previousBufferedValue = index.containsKey(key)
                        ? plainPriorValueForBuffered(key)
                        : null;
                    deserializationResult = deserializeV1(record, key, previousBufferedValue);
                } else {
                    throw new IllegalArgumentException("Restoring apparently invalid changelog record: " + record);
                }
                cleanPut(
                    deserializationResult.time(),
                    deserializationResult.key(),
                    toInMemoryEncoding(deserializationResult.bufferValue(), record.headers())
                );
            }
        }
        updateBufferMetrics();
    }


    @Override
    public void evictWhile(final Supplier<Boolean> predicate,
                           final Consumer<Eviction<K, Change<V>>> callback) {
        final Iterator<Map.Entry<BufferKey, BufferValue>> delegate = sortedMap.entrySet().iterator();
        int evictions = 0;

        if (predicate.get()) {
            Map.Entry<BufferKey, BufferValue> next = null;
            if (delegate.hasNext()) {
                next = delegate.next();
            }

            // predicate being true means we read one record, call the callback, and then remove it
            while (next != null && predicate.get()) {
                if (next.getKey().time() != minTimestamp) {
                    throw new IllegalStateException(
                        "minTimestamp [" + minTimestamp + "] did not match the actual min timestamp [" +
                            next.getKey().time() + "]"
                    );
                }
                final BufferValue bufferValue = next.getValue();
                final ProcessorRecordContext bufferedContext = bufferValue.context();

                final K key;
                final Change<V> value;
                final ProcessorRecordContext evictionContext;
                if (storeHeaders) {
                    // Each value part was stored as a ValueTimestampHeaders blob carrying the headers of
                    // the record it originated from, so take them from the value rather than from the
                    // buffered context: the old value came from an earlier record than the new one, and
                    // deserializing it with the new record's headers would hand the serde the wrong ones.
                    // (ValueTimestampHeadersDeserializer feeds each part's own headers to the inner
                    // value deserializer.)
                    final ValueTimestampHeaders<V> newPart = deserializeValueTimestampHeaders(bufferValue.newValue());
                    final ValueTimestampHeaders<V> oldPart = deserializeValueTimestampHeaders(bufferValue.oldValue());
                    // A tombstone has no new value to take headers from; fall back to the buffered context.
                    // That fallback is not a second-best guess: the buffered context and the new value's
                    // embedded headers always agree. put() hands the very same record.headers() instance to
                    // both, and ValueTimestampHeadersSerializer snapshots the headers only AFTER running the
                    // inner value serializer, so even a serde that writes into the headers it is given (a
                    // Schema Registry serde recording the schema id, say) is reflected in both. Restoring
                    // preserves the equality, because toInMemoryEncoding rebuilds the new part from that
                    // same context. Only the old and prior parts can differ from the context, which is why
                    // they are the ones read from the value here.
                    final Headers newHeaders = newPart == null ? bufferedContext.headers() : newPart.headers();
                    key = keySerde.deserializer().deserialize(changelogTopic, newHeaders, next.getKey().key().get());
                    value = new Change<>(
                        ValueTimestampHeaders.getValueOrNull(newPart),
                        ValueTimestampHeaders.getValueOrNull(oldPart)
                    );
                    // The emitted record carries the new value's headers, so hand them to the callback
                    // via the eviction's context (which is what the suppress processor forwards).
                    evictionContext = withHeaders(bufferedContext, newHeaders);
                } else {
                    // Plain values store no headers of their own; the buffered context is the only
                    // carrier, exactly as before.
                    final Headers headers = bufferedContext.headers();
                    key = keySerde.deserializer().deserialize(changelogTopic, headers, next.getKey().key().get());
                    value = new Change<>(
                        deserializeValue(bufferValue.newValue(), headers),
                        deserializeValue(bufferValue.oldValue(), headers)
                    );
                    evictionContext = bufferedContext;
                }
                callback.accept(new Eviction<K, Change<V>>(key, value, evictionContext));

                delegate.remove();
                index.remove(next.getKey().key());

                if (loggingEnabled) {
                    dirtyKeys.add(next.getKey().key());
                }

                memBufferSize -= computeRecordSize(next.getKey().key(), bufferValue);

                // peek at the next record so we can update the minTimestamp
                if (delegate.hasNext()) {
                    next = delegate.next();
                    minTimestamp = next == null ? Long.MAX_VALUE : next.getKey().time();
                } else {
                    next = null;
                    minTimestamp = Long.MAX_VALUE;
                }

                evictions++;
            }
        }
        if (evictions > 0) {
            updateBufferMetrics();
        }
    }

    // Copy of the given context with different headers, preserving everything else -- including
    // sourceRawKey/sourceRawValue, which the shorter ProcessorRecordContext constructor drops.
    private static ProcessorRecordContext withHeaders(final ProcessorRecordContext context, final Headers headers) {
        if (context.headers() == headers) {
            return context;
        }
        return new ProcessorRecordContext(
            context.timestamp(),
            context.offset(),
            context.partition(),
            context.topic(),
            headers,
            context.sourceRawKey(),
            context.sourceRawValue()
        );
    }

    @Override
    public Maybe<ValueTimestampHeaders<V>> priorValueForBuffered(final K key) {
        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, context.headers(), key));
        final BufferKey bufferKey = index.get(serializedKey);
        if (bufferKey != null) {
            final BufferValue bufferValue = sortedMap.get(bufferKey);
            final byte[] serializedValue = bufferValue.priorValue();

            if (storeHeaders) {
                // The prior value is stored as a ValueTimestampHeaders blob, so we can recover its
                // timestamp and headers directly (they are unknown/empty when the key was first
                // buffered, but preserved across restarts via the changelog).
                return Maybe.defined(deserializeValueTimestampHeaders(serializedValue));
            }

            final V deserializedValue = valueSerde.innerSerde().deserializer().deserialize(
                changelogTopic,
                bufferValue.context().headers(),
                serializedValue
            );

            // it's unfortunately not possible to know this, unless we materialize the suppressed result, since our only
            // knowledge of the prior value is what the upstream processor sends us as the "old value" when we first
            // buffer something.
            return Maybe.defined(ValueTimestampHeaders.make(deserializedValue, RecordQueue.UNKNOWN, new RecordHeaders()));
        } else {
            return Maybe.undefined();
        }
    }

    private byte[] internalPriorValueForBuffered(final Bytes key) {
        final BufferKey bufferKey = index.get(key);
        if (bufferKey == null) {
            throw new NoSuchElementException("Key [" + key + "] is not in the buffer.");
        } else {
            final BufferValue bufferValue = sortedMap.get(bufferKey);
            return bufferValue.priorValue();
        }
    }

    // The legacy V0/V1 restore paths feed the currently-buffered prior value back in as plain value
    // bytes, so unwrap it when the in-memory encoding is the headers-aware format.
    private byte[] plainPriorValueForBuffered(final Bytes key) {
        final byte[] priorValue = internalPriorValueForBuffered(key);
        return storeHeaders ? Utils.rawPlainValue(priorValue) : priorValue;
    }

    // The changelog value format is plain for every version, so a restored buffer value always has
    // plain value parts. When header stores are enabled the in-memory encoding is
    // ValueTimestampHeaders, so re-attach each part's headers and timestamp. The new value gets them
    // from the restored record context, which describes exactly that part (see put()); the prior and
    // old values get them from the changelog record's own Kafka headers. Records written before this
    // feature -- or by a run configured without header stores -- carry no such Kafka headers, in
    // which case the prior/old originals are genuinely unknown and we fall back to empty headers with
    // the record-context timestamp.
    private BufferValue toInMemoryEncoding(final BufferValue value, final Headers recordHeaders) {
        if (!storeHeaders) {
            return value;
        }
        final ProcessorRecordContext context = value.context();
        final byte[] oldValue = valuePartWithHeaders(value.oldValue(), recordHeaders, OLD_VALUE_HEADERS_KEY, context.timestamp());
        // A missing vh.prior on a row whose prior and old values share an array means the writer
        // skipped the duplicate prefix, so the old part IS the prior part. The two conditions have to
        // be checked together: the changelog value dedups on the PLAIN bytes, so a row can come back
        // sharing an array even though the parts carried different headers and were written with a
        // vh.prior of their own. In that case the header wins.
        final byte[] priorValue = value.priorValue() == value.oldValue()
            && recordHeaders.lastHeader(PRIOR_VALUE_HEADERS_KEY) == null
            ? oldValue
            : valuePartWithHeaders(value.priorValue(), recordHeaders, PRIOR_VALUE_HEADERS_KEY, context.timestamp());
        return new BufferValue(
            priorValue,
            oldValue,
            Utils.rawValueTimestampHeaders(value.newValue(), context.timestamp(), context.headers()),
            context
        );
    }

    private static byte[] valuePartWithHeaders(final byte[] rawPlainValue,
                                               final Headers recordHeaders,
                                               final String headerKey,
                                               final long fallbackTimestamp) {
        if (rawPlainValue == null) {
            return null;
        }
        final Header headersAndTimestamp = recordHeaders.lastHeader(headerKey);
        return headersAndTimestamp == null
            ? Utils.rawValueTimestampHeaders(rawPlainValue, fallbackTimestamp)
            : Utils.rawValueTimestampHeaders(headersAndTimestamp.value(), rawPlainValue);
    }

    private ValueTimestampHeadersSerializer<V> valueTimestampHeadersSerializer() {
        if (valueTimestampHeadersSerializer == null) {
            valueTimestampHeadersSerializer = new ValueTimestampHeadersSerializer<>(valueSerde.innerSerde().serializer());
        }
        return valueTimestampHeadersSerializer;
    }

    private ValueTimestampHeadersDeserializer<V> valueTimestampHeadersDeserializer() {
        if (valueTimestampHeadersDeserializer == null) {
            valueTimestampHeadersDeserializer = new ValueTimestampHeadersDeserializer<>(valueSerde.innerSerde().deserializer());
        }
        return valueTimestampHeadersDeserializer;
    }

    // Serialize a single value part. When storeHeaders is set, the value is wrapped as a
    // ValueTimestampHeaders blob carrying the given timestamp and headers; otherwise the plain
    // value bytes are stored (the pre-existing V3 behavior). Returns null for a null value.
    private byte[] serializeValuePart(final V value, final long timestamp, final Headers headers) {
        if (value == null) {
            return null;
        }
        if (storeHeaders) {
            return valueTimestampHeadersSerializer().serialize(changelogTopic, ValueTimestampHeaders.make(value, timestamp, headers));
        }
        return valueSerde.innerSerde().serializer().serialize(changelogTopic, headers, value);
    }

    // Deserialize a single stored value part into a ValueTimestampHeaders. Only valid when
    // storeHeaders is set (i.e. the part bytes are a ValueTimestampHeaders blob).
    private ValueTimestampHeaders<V> deserializeValueTimestampHeaders(final byte[] bytes) {
        return bytes == null ? null : valueTimestampHeadersDeserializer().deserialize(changelogTopic, bytes);
    }

    // Deserialize a single stored value part into the plain value, handling both the headers-aware
    // (ValueTimestampHeaders) and plain encodings.
    private V deserializeValue(final byte[] bytes, final Headers fallbackHeaders) {
        if (bytes == null) {
            return null;
        }
        if (storeHeaders) {
            return ValueTimestampHeaders.getValueOrNull(deserializeValueTimestampHeaders(bytes));
        }
        return valueSerde.innerSerde().deserializer().deserialize(changelogTopic, fallbackHeaders, bytes);
    }

    @Override
    public boolean put(final long time,
                       final Record<K, Change<V>> record,
                       final ProcessorRecordContext recordContext) {
        requireNonNull(record.value(), "value cannot be null");
        requireNonNull(recordContext, "recordContext cannot be null");

        // The headers of the record being processed. These describe the new value: the old value came
        // from an earlier record and gets its own headers below. The framework keeps recordContext in
        // sync with the record (see StreamTask#doProcess and ProcessorContextImpl#forward), so this is
        // the same object as recordContext.headers(); we read it off the record because that is where
        // it conceptually belongs.
        final Headers headers = record.headers();
        final long timestamp = record.timestamp();
        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(changelogTopic, headers, record.key()));
        final BufferValue buffered = getBuffered(serializedKey);

        // The old value's original headers/timestamp are not carried by the incoming record. On an
        // in-place update we recover them from the entry's previous new value (whose value is exactly
        // this update's old value); on the first insert for a key they are genuinely unknown.
        Headers oldHeaders = new RecordHeaders();
        long oldTimestamp = RecordQueue.UNKNOWN;
        if (storeHeaders && buffered != null) {
            final ValueTimestampHeaders<V> previousNewValue = deserializeValueTimestampHeaders(buffered.newValue());
            if (previousNewValue != null) {
                oldHeaders = previousNewValue.headers();
                oldTimestamp = previousNewValue.timestamp();
            }
        }

        final Change<V> change = record.value();
        // Order matters, and must stay old-then-new as in FullChangeSerde#serializeParts: a serializer
        // may write into the headers it is handed (Schema Registry serdes record the schema id there),
        // and in plain mode both parts are serialized against the same live record headers, so
        // whichever is serialized last determines what the emitted record carries. That has to be the
        // new value. In plain mode the old value is therefore serialized with the current record's headers
        // exactly as before, leaving the stored bytes unchanged for non-header stores.
        final byte[] oldValue = serializeValuePart(change.oldValue, oldTimestamp, storeHeaders ? oldHeaders : headers);
        final byte[] newValue = serializeValuePart(change.newValue, timestamp, headers);
        final byte[] serializedPriorValue = buffered == null ? oldValue : buffered.priorValue();

        cleanPut(
            time,
            serializedKey,
            new BufferValue(serializedPriorValue, oldValue, newValue, recordContext)
        );
        if (loggingEnabled) {
            dirtyKeys.add(serializedKey);
        }
        updateBufferMetrics();
        return true;
    }

    private BufferValue getBuffered(final Bytes key) {
        final BufferKey bufferKey = index.get(key);
        return bufferKey == null ? null : sortedMap.get(bufferKey);
    }

    private void cleanPut(final long time, final Bytes key, final BufferValue value) {
        // non-resetting semantics:
        // if there was a previous version of the same record,
        // then insert the new record in the same place in the priority queue

        final BufferKey previousKey = index.get(key);
        if (previousKey == null) {
            final BufferKey nextKey = new BufferKey(time, key);
            index.put(key, nextKey);
            sortedMap.put(nextKey, value);
            minTimestamp = Math.min(minTimestamp, time);
            memBufferSize += computeRecordSize(key, value);
        } else {
            final BufferValue removedValue = sortedMap.put(previousKey, value);
            memBufferSize =
                memBufferSize
                    + computeRecordSize(key, value)
                    - (removedValue == null ? 0 : computeRecordSize(key, removedValue));
        }
    }

    @Override
    public int numRecords() {
        return index.size();
    }

    @Override
    public long bufferSize() {
        return memBufferSize;
    }

    @Override
    public long minTimestamp() {
        return minTimestamp;
    }

    private static long computeRecordSize(final Bytes key, final BufferValue value) {
        long size = 0L;
        size += 8; // buffer time
        size += key.get().length;
        if (value != null) {
            size += value.residentMemorySizeEstimate();
        }
        return size;
    }

    private void updateBufferMetrics() {
        bufferSizeSensor.record(memBufferSize, context.currentSystemTimeMs());
        bufferCountSensor.record(index.size(), context.currentSystemTimeMs());
    }

    @Override
    public String toString() {
        return "InMemoryTimeOrderedKeyValueChangeBuffer{" +
            "storeName='" + storeName + '\'' +
            ", changelogTopic='" + changelogTopic + '\'' +
            ", open=" + open +
            ", loggingEnabled=" + loggingEnabled +
            ", minTimestamp=" + minTimestamp +
            ", memBufferSize=" + memBufferSize +
            ", \n\tdirtyKeys=" + dirtyKeys +
            ", \n\tindex=" + index +
            ", \n\tsortedMap=" + sortedMap +
            '}';
    }
}
