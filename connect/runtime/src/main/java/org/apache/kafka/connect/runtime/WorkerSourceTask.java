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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.SubmittedRecords.CommittableOffsets;

/**
 * {@link WorkerTask} that uses a {@link SourceTask} to ingest data into Kafka.
 */
class WorkerSourceTask extends AbstractWorkerSourceTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);

    private volatile CommittableOffsets committableOffsets;
    //VisibleForTesting
    final SubmittedRecords submittedRecords;
    private final AtomicReference<Exception> producerSendException;

    public WorkerSourceTask(ConnectorTaskId id,
                            SourceTask task,
                            TaskStatus.Listener statusListener,
                            TargetState initialState,
                            Converter keyConverter,
                            Converter valueConverter,
                            ErrorHandlingMetrics errorMetrics,
                            HeaderConverter headerConverter,
                            TransformationChain<SourceRecord, SourceRecord> transformationChain,
                            Producer<byte[], byte[]> producer,
                            TopicAdmin admin,
                            Map<String, TopicCreationGroup> topicGroups,
                            CloseableOffsetStorageReader offsetReader,
                            OffsetStorageWriter offsetWriter,
                            ConnectorOffsetBackingStore offsetStore,
                            WorkerConfig workerConfig,
                            ClusterConfigState configState,
                            ConnectMetrics connectMetrics,
                            ClassLoader loader,
                            Time time,
                            RetryWithToleranceOperator<SourceRecord> retryWithToleranceOperator,
                            StatusBackingStore statusBackingStore,
                            Executor closeExecutor,
                            Supplier<List<ErrorReporter<SourceRecord>>> errorReportersSupplier) {

        super(id, task, statusListener, initialState, keyConverter, valueConverter, headerConverter, transformationChain,
                new WorkerSourceTaskContext(offsetReader, id, configState, null), producer,
                admin, topicGroups, offsetReader, offsetWriter, offsetStore, workerConfig, connectMetrics, errorMetrics, loader,
                time, retryWithToleranceOperator, statusBackingStore, closeExecutor, errorReportersSupplier);

        this.committableOffsets = CommittableOffsets.EMPTY;
        this.submittedRecords = new SubmittedRecords();
        this.producerSendException = new AtomicReference<>();
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
    protected void beginSendIteration() {
        updateCommittableOffsets();
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
    protected Optional<SubmittedRecords.SubmittedRecord> prepareToSendRecord(
            SourceRecord sourceRecord,
            ProducerRecord<byte[], byte[]> producerRecord
    ) {
        maybeThrowProducerSendException();
        return Optional.of(submittedRecords.submit(sourceRecord));
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
    protected void recordSent(
            SourceRecord sourceRecord,
            ProducerRecord<byte[], byte[]> producerRecord,
            RecordMetadata recordMetadata
    ) {
        commitTaskRecord(sourceRecord, recordMetadata);
    }

    @Override
    protected void producerSendFailed(
            ProcessingContext<SourceRecord> context,
            boolean synchronous,
            ProducerRecord<byte[], byte[]> producerRecord,
            SourceRecord preTransformRecord,
            Exception e
    ) {
        if (synchronous) {
            throw new ConnectException("Unrecoverable exception trying to send", e);
        }

        String topic = producerRecord.topic();
        if (retryWithToleranceOperator.getErrorToleranceType() == ToleranceType.ALL) {
            log.trace(
                    "Ignoring failed record send: {} failed to send record to {}: ",
                    WorkerSourceTask.this,
                    topic,
                    e
            );
            // executeFailed here allows the use of existing logging infrastructure/configuration
            retryWithToleranceOperator.executeFailed(
                    context,
                    Stage.KAFKA_PRODUCE,
                    WorkerSourceTask.class,
                    e
            );
            commitTaskRecord(preTransformRecord, null);
        } else {
            producerSendException.compareAndSet(null, e);
        }
    }

    @Override
    protected void finalOffsetCommit(boolean failed) {
        // It should still be safe to commit offsets since any exception would have
        // simply resulted in not getting more records but all the existing records should be ok to flush
        // and commit offsets. Worst case, task.commit() will also throw an exception causing the offset
        // commit to fail.
        submittedRecords.awaitAllMessages(
                workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG),
                TimeUnit.MILLISECONDS
        );
        updateCommittableOffsets();
        commitOffsets();
    }

    /**
     * @return whether an attempt to commit offsets should be made for the task (i.e., there are pending uncommitted
     * offsets and the task's producer has not already failed to send a record with a non-retriable error).
     */
    public boolean shouldCommitOffsets() {
        return !isFailed();
    }

    public boolean commitOffsets() {
        long commitTimeoutMs = workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);

        log.debug("{} Committing offsets", this);

        long started = time.milliseconds();
        long timeout = started + commitTimeoutMs;

        CommittableOffsets offsetsToCommit;
        synchronized (this) {
            offsetsToCommit = this.committableOffsets;
            this.committableOffsets = CommittableOffsets.EMPTY;
        }

        if (offsetsToCommit.isEmpty()) {
            log.debug("{} Either no records were produced by the task since the last offset commit, " 
                    + "or every record has been filtered out by a transformation " 
                    + "or dropped due to transformation or conversion errors.",
                    this
            );
            // We continue with the offset commit process here instead of simply returning immediately
            // in order to invoke SourceTask::commit and record metrics for a successful offset commit
        } else {
            log.info("{} Committing offsets for {} acknowledged messages", this, offsetsToCommit.numCommittableMessages());
            if (offsetsToCommit.hasPending()) {
                log.debug("{} There are currently {} pending messages spread across {} source partitions whose offsets will not be committed. "
                                + "The source partition with the most pending messages is {}, with {} pending messages",
                        this,
                        offsetsToCommit.numUncommittableMessages(),
                        offsetsToCommit.numDeques(),
                        offsetsToCommit.largestDequePartition(),
                        offsetsToCommit.largestDequeSize()
                );
            } else {
                log.debug("{} There are currently no pending messages for this offset commit; "
                                + "all messages dispatched to the task's producer since the last commit have been acknowledged",
                        this
                );
            }
        }

        // Update the offset writer with any new offsets for records that have been acked.
        // The offset writer will continue to track all offsets until they are able to be successfully flushed.
        // IOW, if the offset writer fails to flush, it keeps those offset for the next attempt,
        // though we may update them here with newer offsets for acked records.
        offsetsToCommit.offsets().forEach(offsetWriter::offset);

        boolean shouldFlush;
        try {
            shouldFlush = offsetWriter.beginFlush(timeout - time.milliseconds(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("{} Interrupted while waiting for previous offset flush to complete, cancelling", this);
            recordCommitFailure(time.milliseconds() - started);
            return false;
        } catch (TimeoutException e) {
            log.warn("{} Timed out while waiting for previous offset flush to complete, cancelling", this);
            recordCommitFailure(time.milliseconds() - started);
            return false;
        }
        if (!shouldFlush) {
            // There was nothing in the offsets to process, but we still mark a successful offset commit.
            long durationMillis = time.milliseconds() - started;
            recordCommitSuccess(durationMillis);
            log.debug("{} Finished offset commitOffsets successfully in {} ms",
                    this, durationMillis);

            commitSourceTask();
            return true;
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
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started);
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
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started);
            return false;
        } catch (ExecutionException e) {
            log.error("{} Flush of offsets threw an unexpected exception: ", this, e);
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started);
            return false;
        } catch (TimeoutException e) {
            log.error("{} Timed out waiting to flush offsets to storage; will try again on next flush interval with latest offsets", this);
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started);
            return false;
        }

        long durationMillis = time.milliseconds() - started;
        recordCommitSuccess(durationMillis);
        log.debug("{} Finished commitOffsets successfully in {} ms",
                this, durationMillis);

        commitSourceTask();

        return true;
    }

    // Visible for testing
    void updateCommittableOffsets() {
        CommittableOffsets newOffsets = submittedRecords.committableOffsets();
        synchronized (this) {
            this.committableOffsets = this.committableOffsets.updatedWith(newOffsets);
        }
    }

    private void maybeThrowProducerSendException() {
        if (producerSendException.get() != null) {
            throw new ConnectException(
                    "Unrecoverable exception from producer send callback",
                    producerSendException.get()
            );
        }
    }

    @Override
    public String toString() {
        return "WorkerSourceTask{" +
                "id=" + id +
                '}';
    }

}
