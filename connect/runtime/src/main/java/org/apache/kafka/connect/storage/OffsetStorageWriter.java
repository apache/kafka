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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * OffsetStorageWriter is a buffered writer that wraps the simple {@link OffsetBackingStore} interface.
 * It maintains a copy of the key-value data in memory and buffers writes. It allows you to take
 * a snapshot, which can then be asynchronously flushed to the backing store while new writes
 * continue to be processed. This allows Kafka Connect to process offset commits in the background
 * while continuing to process messages.
 * </p>
 * <p>
 * Connect uses an OffsetStorage implementation to save state about the current progress of
 * source (import to Kafka) connectors, which may have many input partitions and "offsets" may not be as
 * simple as they are for Kafka partitions or files. Offset storage is not required for sink connectors
 * because they can use Kafka's native offset storage (or the sink data store can handle offset
 * storage to achieve exactly once semantics).
 * </p>
 * <p>
 * Both partitions and offsets are generic data objects. This allows different connectors to use
 * whatever representation they need, even arbitrarily complex records. These are translated
 * internally into the serialized form the OffsetBackingStore uses.
 * </p>
 * <p>
 * Note that this only provides write functionality. This is intentional to ensure stale data is
 * never read. Offset data should only be read during startup or reconfiguration of a task. By
 * always serving those requests by reading the values from the backing store, we ensure we never
 * accidentally use stale data. (One example of how this can occur: a task is processing input
 * partition A, writing offsets; reconfiguration causes partition A to be reassigned elsewhere;
 * reconfiguration causes partition A to be reassigned to this node, but now the offset data is out
 * of date). Since these offsets are created and managed by the connector itself, there's no way
 * for the offset management layer to know which keys are "owned" by which tasks at any given
 * time.
 * </p>
 * <p>
 * This class is thread-safe.
 * </p>
 */
public class OffsetStorageWriter {
    private static final Logger log = LoggerFactory.getLogger(OffsetStorageWriter.class);

    private final OffsetBackingStore backingStore;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final String namespace;
    // Offset data in Connect format
    private Map<Map<String, Object>, Map<String, Object>> data = new HashMap<>();

    private Map<Map<String, Object>, Map<String, Object>> toFlush = null;
    private final Semaphore flushInProgress = new Semaphore(1);
    // Unique ID for each flush request to handle callbacks after timeouts
    private long currentFlushId = 0;

    public OffsetStorageWriter(OffsetBackingStore backingStore,
                               String namespace, Converter keyConverter, Converter valueConverter) {
        this.backingStore = backingStore;
        this.namespace = namespace;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    /**
     * Set an offset for a partition using Connect data values
     * @param partition the partition to store an offset for
     * @param offset the offset
     */
    @SuppressWarnings("unchecked")
    public synchronized void offset(Map<String, ?> partition, Map<String, ?> offset) {
        data.put((Map<String, Object>) partition, (Map<String, Object>) offset);
    }

    private boolean flushing() {
        return toFlush != null;
    }

    /**
     * Performs the first step of a flush operation, snapshotting the current state. This does not
     * actually initiate the flush with the underlying storage. Ensures that any previous flush operations
     * have finished before beginning a new flush.
     *
     * @return true if a flush was initiated, false if no data was available
     * @throws ConnectException if the previous flush is not complete before this method is called
     */
    public boolean beginFlush() {
        try {
            return beginFlush(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException | TimeoutException e) {
            log.error("Invalid call to OffsetStorageWriter beginFlush() while already flushing, the "
                    + "framework should not allow this");
            throw new ConnectException("OffsetStorageWriter is already flushing");
        }
    }

    /**
     * Performs the first step of a flush operation, snapshotting the current state. This does not
     * actually initiate the flush with the underlying storage. Ensures that any previous flush operations
     * have finished before beginning a new flush.
     * <p>If and only if this method returns true, the caller must call {@link #doFlush(Callback)}
     * or {@link #cancelFlush()} to finish the flush operation and allow later calls to complete.
     *
     * @param timeout A maximum duration to wait for previous flushes to finish before giving up on waiting
     * @param timeUnit Units of the timeout argument
     * @return true if a flush was initiated, false if no data was available
     * @throws InterruptedException if this thread was interrupted while waiting for the previous flush to complete
     * @throws TimeoutException if the {@code timeout} elapses before previous flushes are complete.
     */
    public boolean beginFlush(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (flushInProgress.tryAcquire(Math.max(0, timeout), timeUnit)) {
            synchronized (this) {
                if (data.isEmpty()) {
                    flushInProgress.release();
                    return false;
                } else {
                    toFlush = data;
                    data = new HashMap<>();
                    return true;
                }
            }
        } else {
            throw new TimeoutException("Timed out waiting for previous flush to finish");
        }
    }

    /**
     * Flush the current offsets and clear them from this writer. This is non-blocking: it
     * moves the current set of offsets out of the way, serializes the data, and asynchronously
     * writes the data to the backing store. If no offsets need to be written, the callback is
     * still invoked, but no Future is returned.
     *
     * @return a Future, or null if there are no offsets to commit
     */
    public Future<Void> doFlush(final Callback<Void> callback) {

        final long flushId;
        // Serialize
        final Map<ByteBuffer, ByteBuffer> offsetsSerialized;

        synchronized (this) {
            flushId = currentFlushId;

            try {
                offsetsSerialized = new HashMap<>(toFlush.size());
                for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : toFlush.entrySet()) {
                    // Offsets are specified as schemaless to the converter, using whatever internal schema is appropriate
                    // for that data. The only enforcement of the format is here.
                    OffsetUtils.validateFormat(entry.getKey());
                    OffsetUtils.validateFormat(entry.getValue());
                    // When serializing the key, we add in the namespace information so the key is [namespace, real key]
                    byte[] key = keyConverter.fromConnectData(namespace, null, Arrays.asList(namespace, entry.getKey()));
                    ByteBuffer keyBuffer = (key != null) ? ByteBuffer.wrap(key) : null;
                    byte[] value = valueConverter.fromConnectData(namespace, null, entry.getValue());
                    ByteBuffer valueBuffer = (value != null) ? ByteBuffer.wrap(value) : null;
                    offsetsSerialized.put(keyBuffer, valueBuffer);
                }
            } catch (Throwable t) {
                // Must handle errors properly here or the writer will be left mid-flush forever and be
                // unable to make progress.
                log.error("CRITICAL: Failed to serialize offset data, making it impossible to commit "
                        + "offsets under namespace {}. This likely won't recover unless the "
                        + "unserializable partition or offset information is overwritten.", namespace);
                log.error("Cause of serialization failure:", t);
                callback.onCompletion(t, null);
                return null;
            }

            // And submit the data
            log.debug("Submitting {} entries to backing store. The offsets are: {}", offsetsSerialized.size(), toFlush);
        }

        return backingStore.set(offsetsSerialized, (error, result) -> {
            boolean isCurrent = handleFinishWrite(flushId, error, result);
            if (isCurrent && callback != null) {
                callback.onCompletion(error, result);
            }
        });
    }

    /**
     * Cancel a flush that has been initiated by {@link #beginFlush}. This should not be called if
     * {@link #doFlush} has already been invoked. It should be used if an operation performed
     * between beginFlush and doFlush failed.
     */
    public synchronized void cancelFlush() {
        // Verify we're still flushing data to handle a race between cancelFlush() calls from up the
        // call stack and callbacks from the write request to underlying storage
        if (flushing()) {
            // Just recombine the data and place it back in the primary storage
            toFlush.putAll(data);
            data = toFlush;
            currentFlushId++;
            flushInProgress.release();
            toFlush = null;
        }
    }

    /**
     * Handle completion of a write. Returns true if this callback is for the current flush
     * operation, false if it's for an old operation that should now be ignored.
     */
    private synchronized boolean handleFinishWrite(long flushId, Throwable error, Void result) {
        // Callbacks need to be handled carefully since the flush operation may have already timed
        // out and been cancelled.
        if (flushId != currentFlushId)
            return false;

        if (error != null) {
            cancelFlush();
        } else {
            currentFlushId++;
            flushInProgress.release();
            toFlush = null;
        }
        return true;
    }
}
