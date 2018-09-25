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
package org.apache.kafka.streams.state.internals.buffer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.ContextualRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;

public class InMemoryTimeOrderedKeyValueBufferStore implements StateStore {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimeOrderedKeyValueBufferStore.class);
    private static final String BUFFER_TIME_HEADER = "_bufferTime";
    private static final BytesSerializer KEY_SERIALIZER = new BytesSerializer();
    private static final ByteArraySerializer VALUE_SERIALIZER = new ByteArraySerializer();

    private final InMemoryTimeOrderedKeyValueBuffer buffer;
    private final String storeName;

    private RecordCollector collector;
    private String changelogTopic;
    private boolean open;

    public static class Builder implements StoreBuilder<StateStore> {
        private final String storeName;

        public Builder(final String storeName) {
            this.storeName = storeName;
        }

        @Override
        public StoreBuilder<StateStore> withCachingEnabled() {
            return null;
        }

        @Override
        public StoreBuilder<StateStore> withCachingDisabled() {
            return null;
        }

        @Override
        public StoreBuilder<StateStore> withLoggingEnabled(final Map<String, String> config) {
            return null;
        }

        @Override
        public StoreBuilder<StateStore> withLoggingDisabled() {
            return null;
        }

        @Override
        public StateStore build() {
            return new InMemoryTimeOrderedKeyValueBufferStore(
                new InMemoryTimeOrderedKeyValueBuffer(),
                storeName
            );
        }

        @Override
        public Map<String, String> logConfig() {
            return Collections.emptyMap();
        }

        @Override
        public boolean loggingEnabled() {
            return true;
        }

        @Override
        public String name() {
            return storeName;
        }
    }

    private InMemoryTimeOrderedKeyValueBufferStore(final InMemoryTimeOrderedKeyValueBuffer buffer,
                                                   final String storeName) {
        this.buffer = buffer;
        this.storeName = storeName;
    }

    public InMemoryTimeOrderedKeyValueBuffer getBuffer() {
        return buffer;
    }

    @Override
    public String name() {
        return storeName;
    }

    @Override
    public void close() {
        buffer.clear();
        open = false;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        context.register(root, (RecordBatchingStateRestoreCallback) this::restoreBatch);
        try {
            collector = ((RecordCollector.Supplier) context).recordCollector();
            changelogTopic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
        } catch (final UnsupportedOperationException e) {
            LOG.warn("Disabling durability, since the context doesn't support it.", e);
        }
        open = true;
    }

    @Override
    public void flush() {
        // counting on this getting called before the record collector's flush
        for (final Bytes key : buffer.getAndClearDirtyKeys()) {
            final KeyValue<Long, ContextualRecord> bufferTimeAndValue = buffer.get(key);
            if (bufferTimeAndValue == null) {
                // The record was evicted from the buffer. Send a tombstone.
                collector.send(changelogTopic, key, null, null, null, null, KEY_SERIALIZER, VALUE_SERIALIZER);
            } else {
                final ContextualRecord value = bufferTimeAndValue.value;
                final byte[] innerValue = value == null ? null : value.value();
                final ProcessorRecordContext recordContext = value == null ? null : value.recordContext();
                final Headers headers = recordContext == null ? new RecordHeaders() : new RecordHeaders(recordContext.headers());
                headers.add(BUFFER_TIME_HEADER, ByteBuffer.wrap(new byte[8]).putLong(bufferTimeAndValue.key).array());
                final Integer partition = recordContext == null ? null : recordContext.partition();
                final Long timestamp = recordContext == null ? null : recordContext.timestamp();
                collector.send(changelogTopic, key, innerValue, headers, partition, timestamp, KEY_SERIALIZER, VALUE_SERIALIZER);
            }

        }
    }

    private void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> batch) {
        for (final ConsumerRecord<byte[], byte[]> record : batch) {
            final Bytes key = Bytes.wrap(record.key());
            if (record.value() == null) {
                // This was a tombstone. Delete the record.
                buffer.cleanDrop(key);
            } else {
                final Headers headers = record.headers();
                final Header header = extractBufferTimeHeader(headers);

                final long time = ByteBuffer.wrap(header.value()).getLong();

                final byte[] innerValue = record.value();
                final ContextualRecord value = new ContextualRecord(
                    innerValue,
                    new ProcessorRecordContext(
                        record.timestamp(),
                        record.offset(),
                        record.partition(),
                        record.topic(),
                        headers
                    )
                );

                buffer.cleanPut(time, key, value);
            }
        }
    }

    private Header extractBufferTimeHeader(final Headers headers) {
        // We know we were the last header to get added.

        // Just in case there were already headers with our header name,
        // we just extract ours and leave the rest there in the same order.
        final Header result;
        final LinkedList<Header> bufferTimeHeaders = new LinkedList<>();
        for (final Header header : headers.headers(BUFFER_TIME_HEADER)) {
            bufferTimeHeaders.add(header);
        }
        result = bufferTimeHeaders.removeLast();
        headers.remove(BUFFER_TIME_HEADER);
        for (final Header preExistingHeader : bufferTimeHeaders) {
            headers.add(preExistingHeader);
        }
        return result;
    }
}
