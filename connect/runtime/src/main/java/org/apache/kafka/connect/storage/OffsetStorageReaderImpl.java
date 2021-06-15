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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of OffsetStorageReader. Unlike OffsetStorageWriter which is implemented
 * directly, the interface is only separate from this implementation because it needs to be
 * included in the public API package.
 */
public class OffsetStorageReaderImpl implements CloseableOffsetStorageReader {
    private static final Logger log = LoggerFactory.getLogger(OffsetStorageReaderImpl.class);

    private final OffsetBackingStore globalBackingStore;
    private final OffsetBackingStore connectorBackingStore;
    private final String namespace;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final AtomicBoolean closed;
    private final Set<OffsetRead> activeOffsetReads;

    public OffsetStorageReaderImpl(OffsetBackingStore globalBackingStore, OffsetBackingStore connectorBackingStore,
                                   String namespace, Converter keyConverter, Converter valueConverter) {
        this.globalBackingStore = globalBackingStore;
        this.connectorBackingStore = connectorBackingStore;
        this.namespace = namespace;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.closed = new AtomicBoolean(false);
        this.activeOffsetReads = new HashSet<>();
    }

    @Override
    public <T> Map<String, Object> offset(Map<String, T> partition) {
        return offsets(Collections.singletonList(partition)).get(partition);
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        // Serialize keys so backing store can work with them
        Map<ByteBuffer, Map<String, T>> serializedToOriginal = new HashMap<>(partitions.size());
        for (Map<String, T> key : partitions) {
            try {
                // Offsets are treated as schemaless, their format is only validated here (and the returned value below)
                OffsetUtils.validateFormat(key);
                byte[] keySerialized = keyConverter.fromConnectData(namespace, null, Arrays.asList(namespace, key));
                ByteBuffer keyBuffer = (keySerialized != null) ? ByteBuffer.wrap(keySerialized) : null;
                serializedToOriginal.put(keyBuffer, key);
            } catch (Throwable t) {
                log.error("CRITICAL: Failed to serialize partition key when getting offsets for task with "
                        + "namespace {}. No value for this data will be returned, which may break the "
                        + "task or cause it to skip some data.", namespace, t);
            }
        }

        OffsetRead offsetRead;
        try {
            synchronized (activeOffsetReads) {
                if (closed.get()) {
                    throw new ConnectException(
                        "Offset reader is closed. This is likely because the task has already been "
                            + "scheduled to stop but has taken longer than the graceful shutdown "
                            + "period to do so.");
                }
                offsetRead = new OffsetRead(
                        globalBackingStore.get(serializedToOriginal.keySet()),
                        connectorBackingStore != null
                                ? connectorBackingStore.get(serializedToOriginal.keySet())
                                : null
                );
                activeOffsetReads.add(offsetRead);
            }

            try {
                offsetRead.complete();
            } catch (CancellationException e) {
                throw new ConnectException(
                    "Offset reader closed while attempting to read offsets. This is likely because "
                        + "the task was been scheduled to stop but has taken longer than the "
                        + "graceful shutdown period to do so.");
            } finally {
                synchronized (activeOffsetReads) {
                    activeOffsetReads.remove(offsetRead);
                }
            }
        } catch (Exception e) {
            log.error("Failed to fetch offsets from namespace {}: ", namespace, e);
            throw new ConnectException("Failed to fetch offsets.", e);
        }

        return offsetRead.offsets(serializedToOriginal);
    }

    private class OffsetRead {
        private final Future<Map<ByteBuffer, ByteBuffer>> globalOffsetRead;
        private final Future<Map<ByteBuffer, ByteBuffer>> connectorOffsetRead;

        private Map<ByteBuffer, ByteBuffer> rawGlobalOffsets;
        private Map<ByteBuffer, ByteBuffer> rawConnectorOffsets;

        public OffsetRead(
                Future<Map<ByteBuffer, ByteBuffer>> globalOffsetRead,
                Future<Map<ByteBuffer, ByteBuffer>> connectorOffsetRead) {
            this.globalOffsetRead = globalOffsetRead;
            this.connectorOffsetRead = connectorOffsetRead != null
                    ? connectorOffsetRead
                    : CompletableFuture.completedFuture(Collections.emptyMap());

            this.rawGlobalOffsets = null;
            this.rawConnectorOffsets = null;
        }

        public void complete() throws InterruptedException, ExecutionException {
            rawGlobalOffsets = globalOffsetRead.get();
            rawConnectorOffsets = connectorOffsetRead.get();
        }

        public void cancel() {
            globalOffsetRead.cancel(true);
            connectorOffsetRead.cancel(true);
        }

        public <T> Map<Map<String, T>, Map<String, Object>> offsets(Map<ByteBuffer, Map<String, T>> serializedToOriginal) {
            Map<Map<String, T>, Map<String, Object>> result = new HashMap<>();
            result.putAll(globalOffsets(serializedToOriginal));
            result.putAll(connectorOffsets(serializedToOriginal));
            return result;
        }

        private <T> Map<Map<String, T>, Map<String, Object>> globalOffsets(Map<ByteBuffer, Map<String, T>> serializedToOriginal) {
            if (rawGlobalOffsets == null) {
                throw new IllegalStateException("Global offsets read has not completed yet");
            }
            return convert(rawGlobalOffsets, serializedToOriginal);
        }

        private <T> Map<Map<String, T>, Map<String, Object>> connectorOffsets(Map<ByteBuffer, Map<String, T>> serializedToOriginal) {
            if (rawConnectorOffsets == null) {
                throw new IllegalStateException("Connector-specific offsets read has not completed yet");
            }
            return convert(rawConnectorOffsets, serializedToOriginal);
        }

        @SuppressWarnings("unchecked")
        private <T> Map<Map<String, T>, Map<String, Object>> convert(Map<ByteBuffer, ByteBuffer> raw, Map<ByteBuffer, Map<String, T>> serializedToOriginal) {
            Map<Map<String, T>, Map<String, Object>> result = new HashMap<>(serializedToOriginal.size());
            for (Map.Entry<ByteBuffer, ByteBuffer> rawEntry : raw.entrySet()) {
                try {
                    // Since null could be a valid key, explicitly check whether map contains the key
                    if (!serializedToOriginal.containsKey(rawEntry.getKey())) {
                        log.error("Should be able to map {} back to a requested partition-offset key, backing "
                                + "store may have returned invalid data", rawEntry.getKey());
                        continue;
                    }

                    Map<String, T> origKey = serializedToOriginal.get(rawEntry.getKey());
                    SchemaAndValue deserializedSchemaAndValue = valueConverter.toConnectData(namespace, rawEntry.getValue() != null ? rawEntry.getValue().array() : null);
                    Object deserializedValue = deserializedSchemaAndValue.value();
                    OffsetUtils.validateFormat(deserializedValue);

                    result.put(origKey, (Map<String, Object>) deserializedValue);
                } catch (Throwable t) {
                    log.error("CRITICAL: Failed to deserialize offset data when getting offsets for task with"
                            + " namespace {}. No value for this data will be returned, which may break the "
                            + "task or cause it to skip some data. This could either be due to an error in "
                            + "the connector implementation or incompatible schema.", namespace, t);
                }
            }
            return result;
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            synchronized (activeOffsetReads) {
                for (OffsetRead offsetRead : activeOffsetReads) {
                    try {
                        offsetRead.cancel();
                    } catch (Throwable t) {
                        log.error("Failed to cancel offset read future", t);
                    }
                }
                activeOffsetReads.clear();
            }

            if (connectorBackingStore != null) {
                connectorBackingStore.stop();
            }
        }
    }
}
