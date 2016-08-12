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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * WorkerTask that uses a SourceTask to ingest data into Kafka.
 */
class WorkerSourceTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);

    private static final long SEND_FAILED_BACKOFF_MS = 100;

    private final WorkerConfig workerConfig;
    private final SourceTask task;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private KafkaProducer<byte[], byte[]> producer;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final Time time;

    private List<SourceRecord> toSend;
    private boolean lastSendFailed; // Whether the last send failed *synchronously*, i.e. never made it into the producer's RecordAccumulator
    // Use IdentityHashMap to ensure correctness with duplicate records. This is a HashMap because
    // there is no IdentityHashSet.
    private IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> outstandingMessages;
    // A second buffer is used while an offset flush is running
    private IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> outstandingMessagesBacklog;
    private boolean flushing;
    private CountDownLatch stopRequestedLatch;

    private Map<String, String> taskConfig;
    private boolean finishedStart = false;
    private boolean startedShutdownBeforeStartCompleted = false;

    public WorkerSourceTask(ConnectorTaskId id,
                            SourceTask task,
                            TaskStatus.Listener statusListener,
                            TargetState initialState,
                            Converter keyConverter,
                            Converter valueConverter,
                            KafkaProducer<byte[], byte[]> producer,
                            OffsetStorageReader offsetReader,
                            OffsetStorageWriter offsetWriter,
                            WorkerConfig workerConfig,
                            Time time) {
        super(id, statusListener, initialState);

        this.workerConfig = workerConfig;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.producer = producer;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.time = time;

        this.toSend = null;
        this.lastSendFailed = false;
        this.outstandingMessages = new IdentityHashMap<>();
        this.outstandingMessagesBacklog = new IdentityHashMap<>();
        this.flushing = false;
        this.stopRequestedLatch = new CountDownLatch(1);
    }

    @Override
    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
        } catch (Throwable t) {
            log.error("Task {} failed initialization and will not be started.", t);
            onFailure(t);
        }
    }

    protected void close() {
        producer.close(30, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        super.stop();
        stopRequestedLatch.countDown();
        synchronized (this) {
            if (finishedStart)
                task.stop();
            else
                startedShutdownBeforeStartCompleted = true;
        }
    }

    @Override
    public void execute() {
        try {
            task.initialize(new WorkerSourceTaskContext(offsetReader));
            task.start(taskConfig);
            log.info("Source task {} finished initialization and start", this);
            synchronized (this) {
                if (startedShutdownBeforeStartCompleted) {
                    task.stop();
                    return;
                }
                finishedStart = true;
            }

            while (!isStopping()) {
                if (shouldPause()) {
                    awaitUnpause();
                    continue;
                }

                if (toSend == null) {
                    log.debug("Nothing to send to Kafka. Polling source for additional records");
                    toSend = task.poll();
                }
                if (toSend == null)
                    continue;
                log.debug("About to send " + toSend.size() + " records to Kafka");
                if (!sendRecords())
                    stopRequestedLatch.await(SEND_FAILED_BACKOFF_MS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            // Ignore and allow to exit.
        } finally {
            // It should still be safe to commit offsets since any exception would have
            // simply resulted in not getting more records but all the existing records should be ok to flush
            // and commit offsets. Worst case, task.flush() will also throw an exception causing the offset commit
            // to fail.
            commitOffsets();
        }
    }

    /**
     * Try to send a batch of records. If a send fails and is retriable, this saves the remainder of the batch so it can
     * be retried after backing off. If a send fails and is not retriable, this will throw a ConnectException.
     * @return true if all messages were sent, false if some need to be retried
     */
    private boolean sendRecords() {
        int processed = 0;
        for (final SourceRecord record : toSend) {
            byte[] key = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            byte[] value = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(record.topic(), record.kafkaPartition(), record.timestamp(), key, value);
            log.trace("Appending record with key {}, value {}", record.key(), record.value());
            // We need this queued first since the callback could happen immediately (even synchronously in some cases).
            // Because of this we need to be careful about handling retries -- we always save the previously attempted
            // record as part of toSend and need to use a flag to track whether we should actually add it to the outstanding
            // messages and update the offsets.
            synchronized (this) {
                if (!lastSendFailed) {
                    if (!flushing) {
                        outstandingMessages.put(producerRecord, producerRecord);
                    } else {
                        outstandingMessagesBacklog.put(producerRecord, producerRecord);
                    }
                    // Offsets are converted & serialized in the OffsetWriter
                    offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
                }
            }
            try {
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
                                    log.debug("Failed record: {}", record);
                                } else {
                                    log.trace("Wrote record successfully: topic {} partition {} offset {}",
                                            recordMetadata.topic(), recordMetadata.partition(),
                                            recordMetadata.offset());
                                    commitTaskRecord(record);
                                }
                                recordSent(producerRecord);
                            }
                        });
                lastSendFailed = false;
            } catch (RetriableException e) {
                log.warn("Failed to send {}, backing off before retrying:", producerRecord, e);
                toSend = toSend.subList(processed, toSend.size());
                lastSendFailed = true;
                return false;
            } catch (KafkaException e) {
                throw new ConnectException("Unrecoverable exception trying to send", e);
            }
            processed++;
        }
        toSend = null;
        return true;
    }

    private void commitTaskRecord(SourceRecord record) {
        try {
            task.commitRecord(record);
        } catch (InterruptedException e) {
            log.error("Exception thrown", e);
        } catch (Throwable t) {
            log.error("Exception thrown while calling task.commitRecord()", t);
        }
    }

    private synchronized void recordSent(final ProducerRecord<byte[], byte[]> record) {
        ProducerRecord<byte[], byte[]> removed = outstandingMessages.remove(record);
        // While flushing, we may also see callbacks for items in the backlog
        if (removed == null && flushing)
            removed = outstandingMessagesBacklog.remove(record);
        // But if neither one had it, something is very wrong
        if (removed == null) {
            log.error("CRITICAL Saw callback for record that was not present in the outstanding message set: "
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
            log.debug("{} flushing {} outstanding messages for offset commit", this, outstandingMessages.size());
            while (!outstandingMessages.isEmpty()) {
                try {
                    long timeoutMs = timeout - time.milliseconds();
                    if (timeoutMs <= 0) {
                        log.error("Failed to flush {}, timed out while waiting for producer to flush outstanding {} messages", this, outstandingMessages.size());
                        finishFailedFlush();
                        return false;
                    }
                    this.wait(timeoutMs);
                } catch (InterruptedException e) {
                    // We can get interrupted if we take too long committing when the work thread shutdown is requested,
                    // requiring a forcible shutdown. Give up since we can't safely commit any offsets, but also need
                    // to stop immediately
                    log.error("{} Interrupted while flushing messages, offsets will not be committed", this);
                    finishFailedFlush();
                    return false;
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

                commitSourceTask();
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

        commitSourceTask();

        return true;
    }

    private void commitSourceTask() {
        try {
            this.task.commit();
        } catch (InterruptedException ex) {
            log.warn("Commit interrupted", ex);
        } catch (Throwable t) {
            log.error("Exception thrown while calling task.commit()", t);
        }
    }

    private synchronized void finishFailedFlush() {
        offsetWriter.cancelFlush();
        outstandingMessages.putAll(outstandingMessagesBacklog);
        outstandingMessagesBacklog.clear();
        flushing = false;
    }

    private synchronized void finishSuccessfulFlush() {
        // If we were successful, we can just swap instead of replacing items back into the original map
        IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> temp = outstandingMessages;
        outstandingMessages = outstandingMessagesBacklog;
        outstandingMessagesBacklog = temp;
        flushing = false;
    }

    @Override
    public String toString() {
        return "WorkerSourceTask{" +
                "id=" + id +
                '}';
    }
}
