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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.RecordConverter;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class ChangeLoggingKeyValueWithTimestampBytesStore extends ChangeLoggingKeyValueBytesStore {
    private final static LongSerializer LONG_SERIALIZER = new LongSerializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();
    private final boolean enableUpgradeMode;


    ChangeLoggingKeyValueWithTimestampBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        this(inner, false);
    }

    ChangeLoggingKeyValueWithTimestampBytesStore(final KeyValueStore<Bytes, byte[]> inner,
                                                 final boolean enableUpgradeMode) {
        super(inner, new byte[]{(byte) 1});
        this.enableUpgradeMode = enableUpgradeMode;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        super.init(new ChangeLoggingKeyValueWithTimestampContext(context), root);
    }

    @Override
    public void put(final Bytes key,
                    final byte[] valueAndTimestamp) {
        if (valueAndTimestamp != null) {
            final byte[] rawTimestamp = new byte[8];
            final byte[] rawValue = new byte[valueAndTimestamp.length - 8];

            System.arraycopy(valueAndTimestamp, 0, rawTimestamp, 0, 8);
            System.arraycopy(valueAndTimestamp, 8, rawValue, 0, valueAndTimestamp.length - 8);

            inner.put(key, valueAndTimestamp);
            changeLogger.logChange(key, rawValue, longDeserializer.deserialize(null, rawTimestamp));
        } else {
            inner.put(key, null);
            changeLogger.logChange(key, null);
        }
    }

    static KeyValue<byte[], byte[]> convertRecord(final ConsumerRecord<byte[], byte[]> oldRecord) {
        final String topic = oldRecord.topic();
        final long timestamp = oldRecord.timestamp();
        final byte[] oldRawValue = oldRecord.value();
        final byte[] rawValueAndTimestamp = new byte[8 + oldRawValue.length];

        final byte[] rawTimestamp = LONG_SERIALIZER.serialize(topic, timestamp);
        System.arraycopy(rawTimestamp, 0, rawValueAndTimestamp, 0, rawTimestamp.length);
        System.arraycopy(oldRawValue, 0, rawValueAndTimestamp, rawTimestamp.length, oldRawValue.length);

        return KeyValue.pair(oldRecord.key(), rawValueAndTimestamp);
    }

    private class ChangeLoggingKeyValueWithTimestampContext implements ProcessorContext, RecordCollector.Supplier {
        private final ProcessorContext context;

        ChangeLoggingKeyValueWithTimestampContext(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public String applicationId() {
            return context.applicationId();
        }

        @Override
        public TaskId taskId() {
            return context.taskId();
        }

        @Override
        public Serde<?> keySerde() {
            return context.keySerde();
        }

        @Override
        public Serde<?> valueSerde() {
            return context.valueSerde();
        }

        @Override
        public File stateDir() {
            return context.stateDir();
        }

        @Override
        public StreamsMetrics metrics() {
            return context.metrics();
        }

        @Override
        public void register(final StateStore store,
                             final StateRestoreCallback stateRestoreCallback) {
            context.register(store, new VersionedStateRestoreCallback(enableUpgradeMode, stateRestoreCallback));
        }

        @Override
        public StateStore getStateStore(final String name) {
            return context.getStateStore(name);
        }

        @Override
        public Cancellable schedule(final long intervalMs,
                                    final PunctuationType type,
                                    final Punctuator callback) {
            return context.schedule(intervalMs, type, callback);
        }

        @Override
        public <K, V> void forward(final K key,
                                   final V value) {
            context.forward(key, value);
        }

        @Override
        public <K, V> void forward(final K key,
                                   final V value,
                                   final To to) {
            context.forward(key, value, to);
        }

        @Override
        public <K, V> void forward(final K key,
                                   final V value,
                                   final int childIndex) {
            context.forward(key, value, childIndex);
        }

        @Override
        public <K, V> void forward(final K key,
                                   final V value,
                                   final String childName) {
            context.forward(key, value, childName);
        }

        @Override
        public void commit() {
            context.commit();
        }

        @Override
        public String topic() {
            return context.topic();
        }

        @Override
        public int partition() {
            return context.partition();
        }

        @Override
        public long offset() {
            return context.offset();
        }

        @Override
        public Headers headers() {
            return context.headers();
        }

        @Override
        public long timestamp() {
            return context.timestamp();
        }

        @Override
        public Map<String, Object> appConfigs() {
            return context.appConfigs();
        }

        @Override
        public Map<String, Object> appConfigsWithPrefix(final String prefix) {
            return context.appConfigsWithPrefix(prefix);
        }

        @Override
        public RecordCollector recordCollector() {
            return ((RecordCollector.Supplier) context).recordCollector();
        }
    }

    public class VersionedStateRestoreCallback implements BatchingStateRestoreCallback, RecordConverter {
        private final boolean enableUpgradeMode;
        private final StateRestoreCallback stateRestoreCallback;
        private final BatchingStateRestoreCallback batchingStateRestoreCallback;

        private VersionedStateRestoreCallback(final boolean enableUpgradeMode,
                                              final StateRestoreCallback stateRestoreCallback) {
            this.enableUpgradeMode = enableUpgradeMode;
            this.stateRestoreCallback = stateRestoreCallback;
            this.batchingStateRestoreCallback = stateRestoreCallback instanceof BatchingStateRestoreCallback ? (BatchingStateRestoreCallback) stateRestoreCallback : null;
        }

        @Override
        public void restore(final byte[] key, final byte[] value) {
            stateRestoreCallback.restore(key, value);
            if (enableUpgradeMode) {
                final byte[] rawTimestamp = new byte[8];
                final byte[] rawValue = new byte[value.length - 8];

                System.arraycopy(value, 0, rawTimestamp, 0, 8);
                System.arraycopy(value, 8, rawValue, 0, value.length - 8);

                changeLogger.logChange(new Bytes(key), rawValue, longDeserializer.deserialize(null, rawTimestamp));
            }
        }

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
            if (batchingStateRestoreCallback != null) {
                batchingStateRestoreCallback.restoreAll(records);

                if (enableUpgradeMode) {
                    for (final KeyValue<byte[], byte[]> record : records) {
                        final byte[] rawTimestamp = new byte[8];
                        final byte[] rawValue = new byte[record.value.length - 8];

                        System.arraycopy(record.value, 0, rawTimestamp, 0, 8);
                        System.arraycopy(record.value, 8, rawValue, 0, record.value.length - 8);

                        changeLogger.logChange(new Bytes(record.key), rawValue, longDeserializer.deserialize(null, rawTimestamp));
                    }
                }
            } else {
                for (final KeyValue<byte[], byte[]> record : records) {
                    restore(record.key, record.value);
                }
            }
        }

        @Override
        public KeyValue<byte[], byte[]> convert(final ConsumerRecord<byte[], byte[]> record) {
            if (record.headers().equals(dataFormatVersionHeader)) {
                return convertRecord(record);
            } else {
                return null;
            }
        }
    }

}
