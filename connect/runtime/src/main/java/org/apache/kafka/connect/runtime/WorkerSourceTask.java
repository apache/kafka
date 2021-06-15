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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * WorkerTask that uses a SourceTask to ingest data into Kafka.
 */
class WorkerSourceTask extends AbstractWorkerSourceTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);

    // Use IdentityHashMap to ensure correctness with duplicate records. This is a HashMap because
    // there is no IdentityHashSet.
    private IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> outstandingMessages;
    // A second buffer is used while an offset flush is running
    private IdentityHashMap<ProducerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> outstandingMessagesBacklog;
    private boolean flushing;

    public WorkerSourceTask(ConnectorTaskId id,
                            SourceTask task,
                            TaskStatus.Listener statusListener,
                            TargetState initialState,
                            Converter keyConverter,
                            Converter valueConverter,
                            HeaderConverter headerConverter,
                            TransformationChain<SourceRecord> transformationChain,
                            Producer<byte[], byte[]> producer,
                            TopicAdmin admin,
                            Map<String, TopicCreationGroup> topicGroups,
                            CloseableOffsetStorageReader offsetReader,
                            OffsetStorageWriter offsetWriter,
                            WorkerConfig workerConfig,
                            ClusterConfigState configState,
                            ConnectMetrics connectMetrics,
                            ClassLoader loader,
                            Time time,
                            RetryWithToleranceOperator retryWithToleranceOperator,
                            StatusBackingStore statusBackingStore,
                            Executor closeExecutor) {

        super(id, task, statusListener, initialState, keyConverter, valueConverter, headerConverter, transformationChain,
                new WorkerSourceTaskContext(offsetReader, id, configState, null), () -> producer,
                admin, topicGroups, offsetReader, offsetWriter, workerConfig, connectMetrics, loader,
                time, retryWithToleranceOperator, statusBackingStore, closeExecutor);

        this.outstandingMessages = new IdentityHashMap<>();
        this.outstandingMessagesBacklog = new IdentityHashMap<>();
        this.flushing = false;
    }

    @Override
    protected void prepareToInitializeTask() {
        // No-op
    }

    @Override
    protected void prepareToEnterSendLoop() {
        // No-op
    }

    @Override
    protected void prepareToPollTask() {
        maybeThrowProducerSendException();
    }

    @Override
    protected void recordDropped(SourceRecord record) {
        commitTaskRecord(record, null);
    }

    @Override
    protected void prepareToSendRecord(SourceRecord sourceRecord, ProducerRecord<byte[], byte[]> producerRecord, boolean newRecord) {
        maybeThrowProducerSendException();

        // We need this queued first since the callback could happen immediately (even synchronously in some cases).
        // Because of this we need to be careful about handling retries -- we always save the previously attempted
        // record as part of toSend and need to use a flag to track whether we should actually add it to the outstanding
        // messages and update the offsets.
        synchronized (this) {
            if (newRecord) {
                if (!flushing) {
                    outstandingMessages.put(producerRecord, producerRecord);
                } else {
                    outstandingMessagesBacklog.put(producerRecord, producerRecord);
                }
                // Offsets are converted & serialized in the OffsetWriter
                offsetWriter.offset(sourceRecord.sourcePartition(), sourceRecord.sourceOffset());
            }
        }
    }

    @Override
    protected void recordDispatched(SourceRecord record) {
        // No-op
    }

    @Override
    protected void batchDispatched() {
        // No-op
    }

    @Override
    protected void recordSent(SourceRecord sourceRecord, ProducerRecord<byte[], byte[]> producerRecord, RecordMetadata recordMetadata) {
        synchronized (this) {
            ProducerRecord<byte[], byte[]> removed = outstandingMessages.remove(producerRecord);
            // While flushing, we may also see callbacks for items in the backlog
            if (removed == null && flushing)
                removed = outstandingMessagesBacklog.remove(producerRecord);
            // But if neither one had it, something is very wrong
            if (removed == null) {
                log.error("{} CRITICAL Saw callback for record that was not present in the outstanding message set: {}", this, producerRecord);
            } else if (flushing && outstandingMessages.isEmpty()) {
                // flush thread may be waiting on the outstanding messages to clear
                this.notifyAll();
            }
        }
        commitTaskRecord(sourceRecord, recordMetadata);
    }

    @Override
    protected void finalOffsetCommit(boolean failed) {
        // It should still be safe to commit offsets since any exception would have
        // simply resulted in not getting more records but all the existing records should be ok to flush
        // and commit offsets. Worst case, task.flush() will also throw an exception causing the offset commit
        // to fail.
        commitOffsets();
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
            log.info("{} flushing {} outstanding messages for offset commit", this, outstandingMessages.size());
            while (!outstandingMessages.isEmpty()) {
                try {
                    long timeoutMs = timeout - time.milliseconds();
                    // If the task has been cancelled, no more records will be sent from the producer; in that case, if any outstanding messages remain,
                    // we can stop flushing immediately
                    if (isCancelled() || timeoutMs <= 0) {
                        log.error("{} Failed to flush, timed out while waiting for producer to flush outstanding {} messages", this, outstandingMessages.size());
                        finishFailedFlush();
                        recordCommitFailure(time.milliseconds() - started, null);
                        return false;
                    }
                    this.wait(timeoutMs);
                } catch (InterruptedException e) {
                    // We can get interrupted if we take too long committing when the work thread shutdown is requested,
                    // requiring a forcible shutdown. Give up since we can't safely commit any offsets, but also need
                    // to stop immediately
                    log.error("{} Interrupted while flushing messages, offsets will not be committed", this);
                    finishFailedFlush();
                    recordCommitFailure(time.milliseconds() - started, null);
                    return false;
                }
            }

            if (!flushStarted) {
                // There was nothing in the offsets to process, but we still waited for the data in the
                // buffer to flush. This is useful since this can feed into metrics to monitor, e.g.
                // flush time, which can be used for monitoring even if the connector doesn't record any
                // offsets.
                finishSuccessfulFlush();
                long durationMillis = time.milliseconds() - started;
                recordCommitSuccess(durationMillis);
                log.debug("{} Finished offset commitOffsets successfully in {} ms",
                        this, durationMillis);

                commitSourceTask();
                return true;
            }
        }

        // Now we can actually flush the offsets to user storage.
        Future<Void> flushFuture = offsetWriter.doFlush((error, result) -> {
            if (error != null) {
                log.error("{} Failed to flush offsets to storage: ", WorkerSourceTask.this, error);
            } else {
                log.trace("{} Finished flushing offsets to storage", WorkerSourceTask.this);
            }
        });
        // Very rare case: offsets were unserializable and we finished immediately, unable to store
        // any data
        if (flushFuture == null) {
            finishFailedFlush();
            recordCommitFailure(time.milliseconds() - started, null);
            return false;
        }
        try {
            flushFuture.get(Math.max(timeout - time.milliseconds(), 0), TimeUnit.MILLISECONDS);
            // There's a small race here where we can get the callback just as this times out (and log
            // success), but then catch the exception below and cancel everything. This won't cause any
            // errors, is only wasteful in this minor edge case, and the worst result is that the log
            // could look a little confusing.
        } catch (InterruptedException e) {
            log.warn("{} Flush of offsets interrupted, cancelling", this);
            finishFailedFlush();
            recordCommitFailure(time.milliseconds() - started, e);
            return false;
        } catch (ExecutionException e) {
            log.error("{} Flush of offsets threw an unexpected exception: ", this, e);
            finishFailedFlush();
            recordCommitFailure(time.milliseconds() - started, e);
            return false;
        } catch (TimeoutException e) {
            log.error("{} Timed out waiting to flush offsets to storage", this);
            finishFailedFlush();
            recordCommitFailure(time.milliseconds() - started, null);
            return false;
        }

        finishSuccessfulFlush();
        long durationMillis = time.milliseconds() - started;
        recordCommitSuccess(durationMillis);
        log.debug("{} Finished commitOffsets successfully in {} ms",
                this, durationMillis);

        commitSourceTask();

        return true;
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
