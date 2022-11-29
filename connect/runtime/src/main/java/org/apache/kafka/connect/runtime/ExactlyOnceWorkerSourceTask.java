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
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTask.TransactionBoundary;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;


/**
 * WorkerTask that uses a SourceTask to ingest data into Kafka, with support for exactly-once delivery guarantees.
 */
class ExactlyOnceWorkerSourceTask extends AbstractWorkerSourceTask {
    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceWorkerSourceTask.class);

    private boolean transactionOpen;
    private final LinkedHashMap<SourceRecord, RecordMetadata> commitableRecords;

    private final TransactionBoundaryManager transactionBoundaryManager;
    private final TransactionMetricsGroup transactionMetrics;

    private final Runnable preProducerCheck;
    private final Runnable postProducerCheck;

    public ExactlyOnceWorkerSourceTask(ConnectorTaskId id,
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
                                       ConnectorOffsetBackingStore offsetStore,
                                       WorkerConfig workerConfig,
                                       ClusterConfigState configState,
                                       ConnectMetrics connectMetrics,
                                       ErrorHandlingMetrics errorMetrics,
                                       ClassLoader loader,
                                       Time time,
                                       RetryWithToleranceOperator retryWithToleranceOperator,
                                       StatusBackingStore statusBackingStore,
                                       SourceConnectorConfig sourceConfig,
                                       Executor closeExecutor,
                                       Runnable preProducerCheck,
                                       Runnable postProducerCheck) {
        super(id, task, statusListener, initialState, keyConverter, valueConverter, headerConverter, transformationChain,
                new WorkerSourceTaskContext(offsetReader, id, configState, buildTransactionContext(sourceConfig)),
                producer, admin, topicGroups, offsetReader, offsetWriter, offsetStore, workerConfig, connectMetrics, errorMetrics,
                loader, time, retryWithToleranceOperator, statusBackingStore, closeExecutor);

        this.transactionOpen = false;
        this.commitableRecords = new LinkedHashMap<>();

        this.preProducerCheck = preProducerCheck;
        this.postProducerCheck = postProducerCheck;

        this.transactionBoundaryManager = buildTransactionManager(workerConfig, sourceConfig, sourceTaskContext.transactionContext());
        this.transactionMetrics = new TransactionMetricsGroup(id, connectMetrics);
    }

    private static WorkerTransactionContext buildTransactionContext(SourceConnectorConfig sourceConfig) {
        return TransactionBoundary.CONNECTOR.equals(sourceConfig.transactionBoundary())
                ? new WorkerTransactionContext()
                : null;
    }

    @Override
    protected void prepareToInitializeTask() {
        preProducerCheck.run();

        // Try not to initialize the transactional producer (which may accidentally fence out other, later task generations) if we've already
        // been shut down at this point
        if (isStopping())
            return;
        producer.initTransactions();

        postProducerCheck.run();
    }

    @Override
    protected void prepareToEnterSendLoop() {
        transactionBoundaryManager.initialize();
    }

    @Override
    protected void beginSendIteration() {
        // No-op
    }

    @Override
    protected void prepareToPollTask() {
        // No-op
    }

    @Override
    protected void recordDropped(SourceRecord record) {
        synchronized (commitableRecords) {
            commitableRecords.put(record, null);
        }
        transactionBoundaryManager.maybeCommitTransactionForRecord(record);
    }

    @Override
    protected Optional<SubmittedRecords.SubmittedRecord> prepareToSendRecord(
            SourceRecord sourceRecord,
            ProducerRecord<byte[], byte[]> producerRecord
    ) {
        if (offsetStore.primaryOffsetsTopic().equals(producerRecord.topic())) {
            // This is to prevent deadlock that occurs when:
            //     1. A task provides a record whose topic is the task's offsets topic
            //     2. That record is dispatched to the task's producer in a transaction that remains open
            //        at least until the worker polls the task again
            //     3. In the subsequent call to SourceTask::poll, the task requests offsets from the worker
            //        (which requires a read to the end of the offsets topic, and will block until any open
            //        transactions on the topic are either committed or aborted)
            throw new ConnectException("Source tasks may not produce to their own offsets topics when exactly-once support is enabled");
        }
        maybeBeginTransaction();
        return Optional.empty();
    }

    @Override
    protected void recordDispatched(SourceRecord record) {
        // Offsets are converted & serialized in the OffsetWriter
        // Important: we only save offsets for the record after it has been accepted by the producer; this way,
        // we commit those offsets if and only if the record is sent successfully.
        offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
        transactionMetrics.addRecord();
        transactionBoundaryManager.maybeCommitTransactionForRecord(record);
    }

    @Override
    protected void batchDispatched() {
        transactionBoundaryManager.maybeCommitTransactionForBatch();
    }

    @Override
    protected void recordSent(
            SourceRecord sourceRecord,
            ProducerRecord<byte[], byte[]> producerRecord,
            RecordMetadata recordMetadata
    ) {
        synchronized (commitableRecords) {
            commitableRecords.put(sourceRecord, recordMetadata);
        }
    }

    @Override
    protected void producerSendFailed(
            boolean synchronous,
            ProducerRecord<byte[], byte[]> producerRecord,
            SourceRecord preTransformRecord,
            Exception e
    ) {
        if (synchronous) {
            throw maybeWrapProducerSendException(
                    "Unrecoverable exception trying to send",
                    e
            );
        } else {
            // No-op; all asynchronously-reported producer exceptions should be bubbled up again by Producer::commitTransaction
        }
    }

    @Override
    protected void finalOffsetCommit(boolean failed) {
        if (failed) {
            log.debug("Skipping final offset commit as task has failed");
            return;
        } else if (isCancelled()) {
            log.debug("Skipping final offset commit as task has been cancelled");
            return;
        }

        // It should be safe to commit here even if we were in the middle of retrying on RetriableExceptions in the
        // send loop since we only track source offsets for records that have been successfully dispatched to the
        // producer.
        // Any records that we were retrying on (and any records after them in the batch) won't be included in the
        // transaction and their offsets won't be committed, but (unless the user has requested connector-defined
        // transaction boundaries), it's better to commit some data than none.
        transactionBoundaryManager.maybeCommitFinalTransaction();
    }

    @Override
    public void removeMetrics() {
        Utils.closeQuietly(transactionMetrics, "source task transaction metrics tracker");
    }

    @Override
    protected void onPause() {
        super.onPause();
        // Commit the transaction now so that we don't end up with a hanging transaction, or worse, get fenced out
        // and fail the task once unpaused
        transactionBoundaryManager.maybeCommitFinalTransaction();
    }

    private void maybeBeginTransaction() {
        if (!transactionOpen) {
            producer.beginTransaction();
            transactionOpen = true;
        }
    }

    private void commitTransaction() {
        log.debug("{} Committing offsets", this);

        long started = time.milliseconds();

        // We might have just aborted a transaction, in which case we'll have to begin a new one
        // in order to commit offsets
        maybeBeginTransaction();

        AtomicReference<Throwable> flushError = new AtomicReference<>();
        if (offsetWriter.beginFlush()) {
            // Now we can actually write the offsets to the internal topic.
            // No need to track the flush future here since it's guaranteed to complete by the time
            // Producer::commitTransaction completes
            // We do have to track failures for that callback though, since they may originate from outside
            // the producer (i.e., the offset writer or the backing offset store), and would not cause
            // Producer::commitTransaction to fail
            offsetWriter.doFlush((error, result) -> {
                if (error != null) {
                    log.error("{} Failed to flush offsets to storage: ", ExactlyOnceWorkerSourceTask.this, error);
                    flushError.compareAndSet(null, error);
                } else {
                    log.trace("{} Finished flushing offsets to storage", ExactlyOnceWorkerSourceTask.this);
                }
            });
        }

        // Only commit the transaction if we were able to serialize the offsets.
        // Otherwise, we may commit source records without committing their offsets
        Throwable error = flushError.get();
        if (error == null) {
            try {
                // Commit the transaction
                // Blocks until all outstanding records have been sent and ack'd
                producer.commitTransaction();
            } catch (Throwable t) {
                log.error("{} Failed to commit producer transaction", ExactlyOnceWorkerSourceTask.this, t);
                flushError.compareAndSet(null, t);
            }
            transactionOpen = false;
        }

        error = flushError.get();
        if (error != null) {
            recordCommitFailure(time.milliseconds() - started, null);
            offsetWriter.cancelFlush();
            throw maybeWrapProducerSendException(
                    "Failed to flush offsets and/or records for task " + id,
                    error
            );
        }

        transactionMetrics.commitTransaction();

        long durationMillis = time.milliseconds() - started;
        recordCommitSuccess(durationMillis);
        log.debug("{} Finished commitOffsets successfully in {} ms", this, durationMillis);

        // Synchronize in order to guarantee that writes on other threads are picked up by this one
        synchronized (commitableRecords) {
            commitableRecords.forEach(this::commitTaskRecord);
            commitableRecords.clear();
        }
        commitSourceTask();
    }

    private RuntimeException maybeWrapProducerSendException(String message, Throwable error) {
        if (isPossibleTransactionTimeoutError(error)) {
            return wrapTransactionTimeoutError(error);
        } else {
            return new ConnectException(message, error);
        }
    }

    private static boolean isPossibleTransactionTimeoutError(Throwable error) {
        return error instanceof InvalidProducerEpochException
            || error.getCause() instanceof InvalidProducerEpochException;
    }

    private ConnectException wrapTransactionTimeoutError(Throwable error) {
        return new ConnectException(
            "The task " + id + " was unable to finish writing records to Kafka before its producer transaction expired. "
                + "It may be necessary to reconfigure this connector in order for it to run healthily with exactly-once support. "
                + "Options for this include: tune the connector's producer configuration for higher throughput, "
                + "increase the transaction timeout for the connector's producers, "
                + "decrease the offset commit interval (if using interval-based transaction boundaries), "
                + "or use the 'poll' transaction boundary (if the connector is not already configured to use it).",
            error
        );
    }

    @Override
    public String toString() {
        return "ExactlyOnceWorkerSourceTask{" +
            "id=" + id +
            '}';
    }

    private abstract class TransactionBoundaryManager {
        protected boolean shouldCommitTransactionForRecord(SourceRecord record) {
            return false;
        }

        protected boolean shouldCommitTransactionForBatch(long currentTimeMs) {
            return false;
        }

        protected boolean shouldCommitFinalTransaction() {
            return false;
        }

        /**
         * Hook to signal that a new transaction cycle has been started. May be invoked
         * multiple times if the task is paused and then resumed. It can be assumed that
         * a new transaction is created at least every time an existing transaction is
         * committed; this is just a hook to notify that a new transaction may have been
         * created outside of that flow as well.
         */
        protected void initialize() {
        }

        public void maybeCommitTransactionForRecord(SourceRecord record) {
            maybeCommitTransaction(shouldCommitTransactionForRecord(record));
        }

        public void maybeCommitTransactionForBatch() {
            maybeCommitTransaction(shouldCommitTransactionForBatch(time.milliseconds()));
        }

        public void maybeCommitFinalTransaction() {
            maybeCommitTransaction(shouldCommitFinalTransaction());
        }

        private void maybeCommitTransaction(boolean shouldCommit) {
            if (shouldCommit && (transactionOpen || offsetWriter.willFlush())) {
                try (LoggingContext loggingContext = LoggingContext.forOffsets(id)) {
                    commitTransaction();
                }
            }
        }
    }

    private TransactionBoundaryManager buildTransactionManager(
            WorkerConfig workerConfig,
            SourceConnectorConfig sourceConfig,
            WorkerTransactionContext transactionContext) {
        TransactionBoundary boundary = sourceConfig.transactionBoundary();
        switch (boundary) {
            case POLL:
                return new TransactionBoundaryManager() {
                    @Override
                    protected boolean shouldCommitTransactionForBatch(long currentTimeMs) {
                        return true;
                    }

                    @Override
                    protected boolean shouldCommitFinalTransaction() {
                        return true;
                    }
                };

            case INTERVAL:
                long transactionBoundaryInterval = Optional.ofNullable(sourceConfig.transactionBoundaryInterval())
                        .orElse(workerConfig.offsetCommitInterval());
                return new TransactionBoundaryManager() {
                    private final long commitInterval = transactionBoundaryInterval;
                    private long lastCommit;

                    @Override
                    public void initialize() {
                        this.lastCommit = time.milliseconds();
                    }

                    @Override
                    protected boolean shouldCommitTransactionForBatch(long currentTimeMs) {
                        if (time.milliseconds() >= lastCommit + commitInterval) {
                            lastCommit = time.milliseconds();
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    protected  boolean shouldCommitFinalTransaction() {
                        return true;
                    }
                };

            case CONNECTOR:
                Objects.requireNonNull(transactionContext, "Transaction context must be provided when using connector-defined transaction boundaries");
                return new TransactionBoundaryManager() {
                    @Override
                    protected boolean shouldCommitFinalTransaction() {
                        return shouldCommitTransactionForBatch(time.milliseconds());
                    }

                    @Override
                    protected boolean shouldCommitTransactionForBatch(long currentTimeMs) {
                        if (transactionContext.shouldAbortBatch()) {
                            log.info("Aborting transaction for batch as requested by connector");
                            abortTransaction();
                            // We abort the transaction, which causes all the records up to this point to be dropped, but we still want to
                            // commit offsets so that the task doesn't see the same records all over again
                            return true;
                        }
                        return transactionContext.shouldCommitBatch();
                    }

                    @Override
                    protected boolean shouldCommitTransactionForRecord(SourceRecord record) {
                        if (transactionContext.shouldAbortOn(record)) {
                            log.info("Aborting transaction for record on topic {} as requested by connector", record.topic());
                            log.trace("Last record in aborted transaction: {}", record);
                            abortTransaction();
                            // We abort the transaction, which causes all the records up to this point to be dropped, but we still want to
                            // commit offsets so that the task doesn't see the same records all over again
                            return true;
                        }
                        return transactionContext.shouldCommitOn(record);
                    }

                    private void abortTransaction() {
                        producer.abortTransaction();
                        transactionMetrics.abortTransaction();
                        transactionOpen = false;
                    }
                };
            default:
                throw new IllegalArgumentException("Unrecognized transaction boundary: " + boundary);
        }
    }

    TransactionMetricsGroup transactionMetricsGroup() {
        return transactionMetrics;
    }


    static class TransactionMetricsGroup implements AutoCloseable {
        private final Sensor transactionSize;
        private int size;
        private final ConnectMetrics.MetricGroup metricGroup;

        public TransactionMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.sourceTaskGroupName(),
                    registry.connectorTagName(), id.connector(),
                    registry.taskTagName(), Integer.toString(id.task()));

            transactionSize = metricGroup.sensor("transaction-size");
            transactionSize.add(metricGroup.metricName(registry.transactionSizeAvg), new Avg());
            transactionSize.add(metricGroup.metricName(registry.transactionSizeMin), new Min());
            transactionSize.add(metricGroup.metricName(registry.transactionSizeMax), new Max());
        }

        @Override
        public void close() {
            metricGroup.close();
        }

        void addRecord() {
            size++;
        }

        void abortTransaction() {
            size = 0;
        }

        void commitTransaction() {
            transactionSize.record(size);
            size = 0;
        }

        protected ConnectMetrics.MetricGroup metricGroup() {
            return metricGroup;
        }

    }

}
