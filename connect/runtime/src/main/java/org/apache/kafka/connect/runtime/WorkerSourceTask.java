/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.ShutdownableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * WorkerTask that uses a SourceTask to ingest data into Kafka.
 */
class WorkerSourceTask implements WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);

    private final ConnectorTaskId id;
    private final SourceTask task;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private KafkaProducer<byte[], byte[]> producer;
    private WorkerSourceTaskThread workThread;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final WorkerConfig workerConfig;
    private final Time time;

    // Use IdentityHashMap to ensure correctness with duplicate records. This is a HashMap because
    // there is no IdentityHashSet.
    private IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> outstandingMessages;
    // A second buffer is used while an offset flush is running
    private IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> outstandingMessagesBacklog;
    private boolean flushing;

    public WorkerSourceTask(ConnectorTaskId id, SourceTask task,
                            Converter keyConverter, Converter valueConverter,
                            KafkaProducer<byte[], byte[]> producer,
                            OffsetStorageReader offsetReader, OffsetStorageWriter offsetWriter,
                            WorkerConfig workerConfig, Time time) {
        this.id = id;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.producer = producer;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.workerConfig = workerConfig;
        this.time = time;

        this.outstandingMessages = new IdentityHashMap<>();
        this.outstandingMessagesBacklog = new IdentityHashMap<>();
        this.flushing = false;
    }

    @Override
    public void start(Map<String, String> props) {
        workThread = new WorkerSourceTaskThread("WorkerSourceTask-" + id, props);
        workThread.start();
    }

    @Override
    public void stop() {
        if (workThread != null)
            workThread.startGracefulShutdown();
    }

    @Override
    public boolean awaitStop(long timeoutMs) {
        boolean success = true;
        if (workThread != null) {
            try {
                success = workThread.awaitShutdown(timeoutMs, TimeUnit.MILLISECONDS);
                if (!success)
                    workThread.forceShutdown();
            } catch (InterruptedException e) {
                success = false;
            }
        }
        return success;
    }

    @Override
    public void close() {
        // Nothing to do
    }

    /**
     * Send a batch of records. This is atomic up to the point of getting the messages into the
     * Producer and recorded in our set of outstanding messages, so either all or none will be sent
     * @param records
     */
    private synchronized void sendRecords(List<SourceRecord> records) {
        for (final SourceRecord record : records) {
            byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(record.topic(), record.kafkaPartition(), key, value);
            log.trace("Appending record with key {}, value {}", record.key(), record.value());
            if (!flushing) {
                outstandingMessages.put(producerRecord, producerRecord);
            } else {
                outstandingMessagesBacklog.put(producerRecord, producerRecord);
            }
            producer.send(
                    producerRecord,
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                // Given the default settings for zero data loss, this should basically never happen --
                                // between "infinite" retries, indefinite blocking on full buffers, and "infinite" request
                                // timeouts, callbacks with exceptions should never be invoked in practice. If the
                                // user overrode these settings, the best we can do is notify them of the failure via
                                // logging.
                                log.error("{} failed to send record to {}: {}", id, record.topic(), e);
                                log.debug("Failed record: topic {}, Kafka partition {}, key {}, value {}, source offset {}, source partition {}",
                                        record.topic(), record.kafkaPartition(), record.key(), record.value(),
                                        record.sourceOffset(), record.sourcePartition());
                            } else {
                                log.trace("Wrote record successfully: topic {} partition {} offset {}",
                                        recordMetadata.topic(), recordMetadata.partition(),
                                        recordMetadata.offset());
                            }
                            recordSent(producerRecord);
                        }
                    });
            // Offsets are converted & serialized in the OffsetWriter
            offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
        }
    }

    private synchronized void recordSent(final ProducerRecord<byte[], byte[]> record) {
        ProducerRecord<byte[], byte[]> removed = outstandingMessages.remove(record);
        // While flushing, we may also see callbacks for items in the backlog
        if (removed == null && flushing)
            removed = outstandingMessagesBacklog.remove(record);
        // But if neither one had it, something is very wrong
        if (removed == null) {
            log.error("Saw callback for record that was not present in the outstanding message set: "
                    + "{}", record);
        } else if (flushing && outstandingMessages.isEmpty()) {
            // flush thread may be waiting on the outstanding messages to clear
            this.notifyAll();
        }
    }

    public boolean commitOffsets() {
        long commitTimeoutMs = workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);

        log.debug("{} Committing offsets", this);

        long started = time.milliseconds();
        long timeout = started + commitTimeoutMs;

        synchronized (this) {
            // First we need to make sure we snapshot everything in exactly the current state. This
            // means both the current set of messages we're still waiting to finish, stored in this
            // class, which setting flushing = true will handle by storing any new values into a new
            // buffer; and the current set of user-specified offsets, stored in the
            // OffsetStorageWriter, for which we can use beginFlush() to initiate the snapshot.
            flushing = true;
            boolean flushStarted = offsetWriter.beginFlush();
            // Still wait for any producer records to flush, even if there aren't any offsets to write
            // to persistent storage

            // Next we need to wait for all outstanding messages to finish sending
            while (!outstandingMessages.isEmpty()) {
                try {
                    long timeoutMs = timeout - time.milliseconds();
                    if (timeoutMs <= 0) {
                        log.error(
                                "Failed to flush {}, timed out while waiting for producer to flush outstanding "
                                        + "messages", this.toString());
                        finishFailedFlush();
                        return false;
                    }
                    this.wait(timeoutMs);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            if (!flushStarted) {
                // There was nothing in the offsets to process, but we still waited for the data in the
                // buffer to flush. This is useful since this can feed into metrics to monitor, e.g.
                // flush time, which can be used for monitoring even if the connector doesn't record any
                // offsets.
                finishSuccessfulFlush();
                log.debug("Finished {} offset commitOffsets successfully in {} ms",
                        this, time.milliseconds() - started);
                return true;
            }
        }

        // Now we can actually flush the offsets to user storage.
        Future<Void> flushFuture = offsetWriter.doFlush(new org.apache.kafka.connect.util.Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                if (error != null) {
                    log.error("Failed to flush {} offsets to storage: ", this, error);
                } else {
                    log.trace("Finished flushing {} offsets to storage", this);
                }
            }
        });
        // Very rare case: offsets were unserializable and we finished immediately, unable to store
        // any data
        if (flushFuture == null) {
            finishFailedFlush();
            return false;
        }
        try {
            flushFuture.get(Math.max(timeout - time.milliseconds(), 0), TimeUnit.MILLISECONDS);
            // There's a small race here where we can get the callback just as this times out (and log
            // success), but then catch the exception below and cancel everything. This won't cause any
            // errors, is only wasteful in this minor edge case, and the worst result is that the log
            // could look a little confusing.
        } catch (InterruptedException e) {
            log.warn("Flush of {} offsets interrupted, cancelling", this);
            finishFailedFlush();
            return false;
        } catch (ExecutionException e) {
            log.error("Flush of {} offsets threw an unexpected exception: ", this, e);
            finishFailedFlush();
            return false;
        } catch (TimeoutException e) {
            log.error("Timed out waiting to flush {} offsets to storage", this);
            finishFailedFlush();
            return false;
        }

        finishSuccessfulFlush();
        log.info("Finished {} commitOffsets successfully in {} ms",
                this, time.milliseconds() - started);
        return true;
    }

    private synchronized void finishFailedFlush() {
        offsetWriter.cancelFlush();
        outstandingMessages.putAll(outstandingMessagesBacklog);
        outstandingMessagesBacklog.clear();
        flushing = false;
    }

    private void finishSuccessfulFlush() {
        // If we were successful, we can just swap instead of replacing items back into the original map
        IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> temp = outstandingMessages;
        outstandingMessages = outstandingMessagesBacklog;
        outstandingMessagesBacklog = temp;
        flushing = false;
    }


    private class WorkerSourceTaskThread extends ShutdownableThread {
        private Map<String, String> workerProps;
        private boolean finishedStart;
        private boolean startedShutdownBeforeStartCompleted;

        public WorkerSourceTaskThread(String name, Map<String, String> workerProps) {
            super(name);
            this.workerProps = workerProps;
            this.finishedStart = false;
            this.startedShutdownBeforeStartCompleted = false;
        }

        @Override
        public void execute() {
            try {
                task.initialize(new WorkerSourceTaskContext(offsetReader));
                task.start(workerProps);
                log.info("Source task {} finished initialization and start", this);
                synchronized (this) {
                    if (startedShutdownBeforeStartCompleted) {
                        task.stop();
                        return;
                    }
                    finishedStart = true;
                }

                while (getRunning()) {
                    List<SourceRecord> records = task.poll();
                    if (records == null)
                        continue;
                    sendRecords(records);
                }
            } catch (InterruptedException e) {
                // Ignore and allow to exit.
            } catch (Throwable t) {
                log.error("Task {} threw an uncaught and unrecoverable exception", id);
                log.error("Task is being killed and will not recover until manually restarted:", t);
                // It should still be safe to let this fall through and commit offsets since this exception would have
                // simply resulted in not getting more records but all the existing records should be ok to flush
                // and commit offsets. Worst case, task.flush() will also throw an exception causing the offset commit
                // to fail.
            }

            commitOffsets();
        }

        @Override
        public void startGracefulShutdown() {
            super.startGracefulShutdown();
            synchronized (this) {
                if (finishedStart)
                    task.stop();
                else
                    startedShutdownBeforeStartCompleted = true;
            }
        }
    }

    @Override
    public String toString() {
        return "WorkerSourceTask{" +
                "id=" + id +
                '}';
    }
}
