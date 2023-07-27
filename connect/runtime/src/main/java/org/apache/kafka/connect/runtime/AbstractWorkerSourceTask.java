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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.TopicAdmin;
import org.apache.kafka.connect.util.TopicCreation;
import org.apache.kafka.connect.util.TopicCreationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;

/**
 * WorkerTask that contains shared logic for running source tasks with either standard semantics
 * (i.e., either at-least-once or at-most-once) or exactly-once semantics.
 */
public abstract class AbstractWorkerSourceTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(AbstractWorkerSourceTask.class);

    private static final long SEND_FAILED_BACKOFF_MS = 100;

    /**
     * Hook to define custom startup behavior before the calling {@link SourceTask#initialize(SourceTaskContext)}
     * and {@link SourceTask#start(Map)}.
     */
    protected abstract void prepareToInitializeTask();

    /**
     * Hook to define custom initialization behavior when preparing to begin the poll-convert-send loop for the first time,
     * or when re-entering the loop after being paused.
     */
    protected abstract void prepareToEnterSendLoop();

    /**
     * Hook to define custom periodic behavior to be performed at the top of every iteration of the poll-convert-send loop.
     */
    protected abstract void beginSendIteration();

    /**
     * Hook to define custom periodic checks for health, metrics, etc. Called whenever {@link SourceTask#poll()} is about to be invoked.
     */
    protected abstract void prepareToPollTask();

    /**
     * Invoked when a record provided by the task has been filtered out by a transform or the converter,
     * or will be discarded due to failures during transformation or conversion.
     * @param record the pre-transform record that has been dropped; never null.
     */
    protected abstract void recordDropped(SourceRecord record);

    /**
     * Invoked when a record is about to be dispatched to the producer. May be invoked multiple times for the same
     * record if retriable errors are encountered.
     * @param sourceRecord the pre-transform {@link SourceRecord} provided by the source task; never null.
     * @param producerRecord the {@link ProducerRecord} produced by transforming and converting the
     * {@code sourceRecord}; never null;
     * @return a {@link SubmittedRecords.SubmittedRecord} to be {@link SubmittedRecords.SubmittedRecord#ack() acknowledged}
     * if the corresponding producer record is ack'd by Kafka or {@link SubmittedRecords.SubmittedRecord#drop() dropped}
     * if synchronously rejected by the producer. Can also be {@link Optional#empty()} if it is not necessary to track the acknowledgment
     * of individual producer records
     */
    protected abstract Optional<SubmittedRecords.SubmittedRecord> prepareToSendRecord(
            SourceRecord sourceRecord,
            ProducerRecord<byte[], byte[]> producerRecord
    );

    /**
     * Invoked when a record has been transformed, converted, and dispatched to the producer successfully via
     * {@link Producer#send}. Does not guarantee that the record has been sent to Kafka or ack'd by the required number
     * of brokers, but does guarantee that it will never be re-processed.
     * @param record the pre-transform {@link SourceRecord} that was successfully dispatched to the producer; never null.
     */
    protected abstract void recordDispatched(SourceRecord record);

    /**
     * Invoked when an entire batch of records returned from {@link SourceTask#poll} has been transformed, converted,
     * and either discarded due to transform/conversion errors, filtered by a transform, or dispatched to the producer
     * successfully via {@link Producer#send}. Does not guarantee that the records have been sent to Kafka or ack'd by the
     * required number of brokers, but does guarantee that none of the records in the batch will ever be re-processed during
     * the lifetime of this task. At most one record batch is polled from the task in between calls to this method.
     */
    protected abstract void batchDispatched();

    /**
     * Invoked when a record has been sent and ack'd by the Kafka cluster. Note that this method may be invoked
     *  concurrently and should therefore be made thread-safe.
     * @param sourceRecord  the pre-transform {@link SourceRecord} that was successfully sent to Kafka; never null.
     * @param producerRecord the {@link ProducerRecord} produced by transforming and converting the
     * {@code sourceRecord}; never null;
     * @param recordMetadata the {@link RecordMetadata} for the corresponding producer record; never null.
     */
    protected abstract void recordSent(
            SourceRecord sourceRecord,
            ProducerRecord<byte[], byte[]> producerRecord,
            RecordMetadata recordMetadata
    );

    /**
     * Invoked when a record given to {@link Producer#send(ProducerRecord, Callback)} has failed with a non-retriable error.
     * @param synchronous whether the error occurred during the invocation of {@link Producer#send(ProducerRecord, Callback)}.
     *                    If {@code false}, indicates that the error was reported asynchronously by the producer by a {@link Callback}
     * @param producerRecord the {@link ProducerRecord} that the producer failed to send; never null
     * @param preTransformRecord the pre-transform {@link SourceRecord} that the producer record was derived from; never null
     * @param e the exception that was either thrown from {@link Producer#send(ProducerRecord, Callback)}, or reported by the producer
     *          via {@link Callback} after the call to {@link Producer#send(ProducerRecord, Callback)} completed
     */
    protected abstract void producerSendFailed(
            boolean synchronous,
            ProducerRecord<byte[], byte[]> producerRecord,
            SourceRecord preTransformRecord,
            Exception e
    );

    /**
     * Invoked when no more records will be polled from the task or dispatched to the producer. Should attempt to
     * commit the offsets for any outstanding records when possible.
     * @param failed whether the task is undergoing a healthy or an unhealthy shutdown
     */
    protected abstract void finalOffsetCommit(boolean failed);


    protected final WorkerConfig workerConfig;
    protected final WorkerSourceTaskContext sourceTaskContext;
    protected final ConnectorOffsetBackingStore offsetStore;
    protected final OffsetStorageWriter offsetWriter;
    protected final Producer<byte[], byte[]> producer;

    private final SourceTask task;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final TransformationChain<SourceRecord> transformationChain;
    private final TopicAdmin admin;
    private final CloseableOffsetStorageReader offsetReader;
    private final SourceTaskMetricsGroup sourceTaskMetricsGroup;
    private final CountDownLatch stopRequestedLatch;
    private final boolean topicTrackingEnabled;
    private final TopicCreation topicCreation;
    private final Executor closeExecutor;
    private final Supplier<List<ErrorReporter>> errorReportersSupplier;

    // Visible for testing
    List<SourceRecord> toSend;
    protected Map<String, String> taskConfig;
    protected boolean started = false;
    private volatile boolean producerClosed = false;

    protected AbstractWorkerSourceTask(ConnectorTaskId id,
                                       SourceTask task,
                                       TaskStatus.Listener statusListener,
                                       TargetState initialState,
                                       Converter keyConverter,
                                       Converter valueConverter,
                                       HeaderConverter headerConverter,
                                       TransformationChain<SourceRecord> transformationChain,
                                       WorkerSourceTaskContext sourceTaskContext,
                                       Producer<byte[], byte[]> producer,
                                       TopicAdmin admin,
                                       Map<String, TopicCreationGroup> topicGroups,
                                       CloseableOffsetStorageReader offsetReader,
                                       OffsetStorageWriter offsetWriter,
                                       ConnectorOffsetBackingStore offsetStore,
                                       WorkerConfig workerConfig,
                                       ConnectMetrics connectMetrics,
                                       ErrorHandlingMetrics errorMetrics,
                                       ClassLoader loader,
                                       Time time,
                                       RetryWithToleranceOperator retryWithToleranceOperator,
                                       StatusBackingStore statusBackingStore,
                                       Executor closeExecutor,
                                       Supplier<List<ErrorReporter>> errorReportersSupplier) {

        super(id, statusListener, initialState, loader, connectMetrics, errorMetrics,
                retryWithToleranceOperator, time, statusBackingStore);

        this.workerConfig = workerConfig;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
        this.producer = producer;
        this.admin = admin;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.offsetStore = Objects.requireNonNull(offsetStore, "offset store cannot be null for source tasks");
        this.closeExecutor = closeExecutor;
        this.sourceTaskContext = sourceTaskContext;
        this.errorReportersSupplier = errorReportersSupplier;

        this.stopRequestedLatch = new CountDownLatch(1);
        this.sourceTaskMetricsGroup = new SourceTaskMetricsGroup(id, connectMetrics);
        this.topicTrackingEnabled = workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG);
        this.topicCreation = TopicCreation.newTopicCreation(workerConfig, topicGroups);
    }

    @Override
    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
        } catch (Throwable t) {
            log.error("{} Task failed initialization and will not be started.", this, t);
            onFailure(t);
        }
    }

    @Override
    protected void initializeAndStart() {
        retryWithToleranceOperator.reporters(errorReportersSupplier.get());
        prepareToInitializeTask();
        offsetStore.start();
        // If we try to start the task at all by invoking initialize, then count this as
        // "started" and expect a subsequent call to the task's stop() method
        // to properly clean up any resources allocated by its initialize() or
        // start() methods. If the task throws an exception during stop(),
        // the worst thing that happens is another exception gets logged for an already-
        // failed task
        started = true;
        task.initialize(sourceTaskContext);
        task.start(taskConfig);
        log.info("{} Source task finished initialization and start", this);
    }

    @Override
    public void cancel() {
        super.cancel();
        // Preemptively close the offset reader in case the task is blocked on an offset read.
        offsetReader.close();
        // We proactively close the producer here as the main work thread for the task may
        // be blocked indefinitely in a call to Producer::send if automatic topic creation is
        // not enabled on either the connector or the Kafka cluster. Closing the producer should
        // unblock it in that case and allow shutdown to proceed normally.
        // With a duration of 0, the producer's own shutdown logic should be fairly quick,
        // but closing user-pluggable classes like interceptors may lag indefinitely. So, we
        // call close on a separate thread in order to avoid blocking the herder's tick thread.
        closeExecutor.execute(() -> closeProducer(Duration.ZERO));
    }

    @Override
    public void stop() {
        super.stop();
        stopRequestedLatch.countDown();
    }

    @Override
    public void removeMetrics() {
        Utils.closeQuietly(sourceTaskMetricsGroup, "source task metrics tracker");
        super.removeMetrics();
    }

    @Override
    protected void close() {
        if (started) {
            Utils.closeQuietly(task::stop, "source task");
        }

        closeProducer(Duration.ofSeconds(30));

        if (admin != null) {
            Utils.closeQuietly(() -> admin.close(Duration.ofSeconds(30)), "source task admin");
        }
        Utils.closeQuietly(transformationChain, "transformation chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
        Utils.closeQuietly(offsetReader, "offset reader");
        Utils.closeQuietly(offsetStore::stop, "offset backing store");
        Utils.closeQuietly(headerConverter, "header converter");
    }

    private void closeProducer(Duration duration) {
        if (producer != null) {
            producerClosed = true;
            Utils.closeQuietly(() -> producer.close(duration), "source task producer");
        }
    }

    @Override
    public void execute() {
        try {
            prepareToEnterSendLoop();
            while (!isStopping()) {
                beginSendIteration();

                if (shouldPause()) {
                    onPause();
                    if (awaitUnpause()) {
                        onResume();
                        prepareToEnterSendLoop();
                    }
                    continue;
                }

                if (toSend == null) {
                    prepareToPollTask();

                    log.trace("{} Nothing to send to Kafka. Polling source for additional records", this);
                    long start = time.milliseconds();
                    toSend = poll();
                    if (toSend != null) {
                        recordPollReturned(toSend.size(), time.milliseconds() - start);
                    }
                }
                if (toSend == null) {
                    batchDispatched();
                    continue;
                }
                log.trace("{} About to send {} records to Kafka", this, toSend.size());
                if (!sendRecords()) {
                    stopRequestedLatch.await(SEND_FAILED_BACKOFF_MS, TimeUnit.MILLISECONDS);
                }
            }
        } catch (InterruptedException e) {
            // Ignore and allow to exit.
        } catch (RuntimeException e) {
            if (isCancelled()) {
                log.debug("Skipping final offset commit as task has been cancelled");
                throw e;
            }
            try {
                finalOffsetCommit(true);
            } catch (Exception offsetException) {
                log.error("Failed to commit offsets for already-failing task", offsetException);
            }
            throw e;
        }
        finalOffsetCommit(false);
    }

    /**
     * Try to send a batch of records. If a send fails and is retriable, this saves the remainder of the batch so it can
     * be retried after backing off. If a send fails and is not retriable, this will throw a ConnectException.
     * @return true if all messages were sent, false if some need to be retried
     */
    // Visible for testing
    boolean sendRecords() {
        int processed = 0;
        recordBatch(toSend.size());
        final SourceRecordWriteCounter counter =
                toSend.size() > 0 ? new SourceRecordWriteCounter(toSend.size(), sourceTaskMetricsGroup) : null;
        for (final SourceRecord preTransformRecord : toSend) {
            retryWithToleranceOperator.sourceRecord(preTransformRecord);
            final SourceRecord record = transformationChain.apply(preTransformRecord);
            final ProducerRecord<byte[], byte[]> producerRecord = convertTransformedRecord(record);
            if (producerRecord == null || retryWithToleranceOperator.failed()) {
                counter.skipRecord();
                recordDropped(preTransformRecord);
                processed++;
                continue;
            }

            log.trace("{} Appending record to the topic {} with key {}, value {}", this, record.topic(), record.key(), record.value());
            Optional<SubmittedRecords.SubmittedRecord> submittedRecord = prepareToSendRecord(preTransformRecord, producerRecord);
            try {
                final String topic = producerRecord.topic();
                maybeCreateTopic(topic);
                producer.send(
                    producerRecord,
                    (recordMetadata, e) -> {
                        if (e != null) {
                            if (producerClosed) {
                                log.trace("{} failed to send record to {}; this is expected as the producer has already been closed", AbstractWorkerSourceTask.this, topic, e);
                            } else {
                                log.error("{} failed to send record to {}: ", AbstractWorkerSourceTask.this, topic, e);
                            }
                            log.trace("{} Failed record: {}", AbstractWorkerSourceTask.this, preTransformRecord);
                            producerSendFailed(false, producerRecord, preTransformRecord, e);
                            if (retryWithToleranceOperator.getErrorToleranceType() == ToleranceType.ALL) {
                                counter.skipRecord();
                                submittedRecord.ifPresent(SubmittedRecords.SubmittedRecord::ack);
                            }
                        } else {
                            counter.completeRecord();
                            log.trace("{} Wrote record successfully: topic {} partition {} offset {}",
                                    AbstractWorkerSourceTask.this,
                                    recordMetadata.topic(), recordMetadata.partition(),
                                    recordMetadata.offset());
                            recordSent(preTransformRecord, producerRecord, recordMetadata);
                            submittedRecord.ifPresent(SubmittedRecords.SubmittedRecord::ack);
                            if (topicTrackingEnabled) {
                                recordActiveTopic(producerRecord.topic());
                            }
                        }
                    });
                // Note that this will cause retries to take place within a transaction
            } catch (RetriableException | org.apache.kafka.common.errors.RetriableException e) {
                log.warn("{} Failed to send record to topic '{}' and partition '{}'. Backing off before retrying: ",
                        this, producerRecord.topic(), producerRecord.partition(), e);
                toSend = toSend.subList(processed, toSend.size());
                submittedRecord.ifPresent(SubmittedRecords.SubmittedRecord::drop);
                counter.retryRemaining();
                return false;
            } catch (ConnectException e) {
                log.warn("{} Failed to send record to topic '{}' and partition '{}' due to an unrecoverable exception: ",
                        this, producerRecord.topic(), producerRecord.partition(), e);
                log.trace("{} Failed to send {} with unrecoverable exception: ", this, producerRecord, e);
                throw e;
            } catch (KafkaException e) {
                producerSendFailed(true, producerRecord, preTransformRecord, e);
            }
            processed++;
            recordDispatched(preTransformRecord);
        }
        toSend = null;
        batchDispatched();
        return true;
    }

    protected List<SourceRecord> poll() throws InterruptedException {
        try {
            return task.poll();
        } catch (RetriableException | org.apache.kafka.common.errors.RetriableException e) {
            log.warn("{} failed to poll records from SourceTask. Will retry operation.", this, e);
            // Do nothing. Let the framework poll whenever it's ready.
            return null;
        }
    }

    /**
     * Convert the source record into a producer record.
     *
     * @param record the transformed record
     * @return the producer record which can sent over to Kafka. A null is returned if the input is null or
     * if an error was encountered during any of the converter stages.
     */
    protected ProducerRecord<byte[], byte[]> convertTransformedRecord(SourceRecord record) {
        if (record == null) {
            return null;
        }

        RecordHeaders headers = retryWithToleranceOperator.execute(() -> convertHeaderFor(record), Stage.HEADER_CONVERTER, headerConverter.getClass());

        byte[] key = retryWithToleranceOperator.execute(() -> keyConverter.fromConnectData(record.topic(), headers, record.keySchema(), record.key()),
                Stage.KEY_CONVERTER, keyConverter.getClass());

        byte[] value = retryWithToleranceOperator.execute(() -> valueConverter.fromConnectData(record.topic(), headers, record.valueSchema(), record.value()),
                Stage.VALUE_CONVERTER, valueConverter.getClass());

        if (retryWithToleranceOperator.failed()) {
            return null;
        }

        return new ProducerRecord<>(record.topic(), record.kafkaPartition(),
                ConnectUtils.checkAndConvertTimestamp(record.timestamp()), key, value, headers);
    }

    // Due to transformations that may change the destination topic of a record (such as
    // RegexRouter) topic creation can not be batched for multiple topics
    private void maybeCreateTopic(String topic) {
        if (!topicCreation.isTopicCreationRequired(topic)) {
            log.trace("Topic creation by the connector is disabled or the topic {} was previously created." +
                    "If auto.create.topics.enable is enabled on the broker, " +
                    "the topic will be created with default settings", topic);
            return;
        }
        log.info("The task will send records to topic '{}' for the first time. Checking "
                + "whether topic exists", topic);
        Map<String, TopicDescription> existing = admin.describeTopics(topic);
        if (!existing.isEmpty()) {
            log.info("Topic '{}' already exists.", topic);
            topicCreation.addTopic(topic);
            return;
        }

        log.info("Creating topic '{}'", topic);
        TopicCreationGroup topicGroup = topicCreation.findFirstGroup(topic);
        log.debug("Topic '{}' matched topic creation group: {}", topic, topicGroup);
        NewTopic newTopic = topicGroup.newTopic(topic);

        TopicAdmin.TopicCreationResponse response = admin.createOrFindTopics(newTopic);
        if (response.isCreated(newTopic.name())) {
            topicCreation.addTopic(topic);
            log.info("Created topic '{}' using creation group {}", newTopic, topicGroup);
        } else if (response.isExisting(newTopic.name())) {
            topicCreation.addTopic(topic);
            log.info("Found existing topic '{}'", newTopic);
        } else {
            // The topic still does not exist and could not be created, so treat it as a task failure
            log.warn("Request to create new topic '{}' failed", topic);
            throw new ConnectException("Task failed to create new topic " + newTopic + ". Ensure "
                    + "that the task is authorized to create topics or that the topic exists and "
                    + "restart the task");
        }
    }

    protected RecordHeaders convertHeaderFor(SourceRecord record) {
        Headers headers = record.headers();
        RecordHeaders result = new RecordHeaders();
        if (headers != null) {
            String topic = record.topic();
            for (Header header : headers) {
                String key = header.key();
                byte[] rawHeader = headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                result.add(key, rawHeader);
            }
        }
        return result;
    }

    protected void commitTaskRecord(SourceRecord record, RecordMetadata metadata) {
        try {
            task.commitRecord(record, metadata);
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commitRecord()", this, t);
        }
    }

    protected void commitSourceTask() {
        try {
            this.task.commit();
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commit()", this, t);
        }
    }

    protected void recordPollReturned(int numRecordsInBatch, long duration) {
        sourceTaskMetricsGroup.recordPoll(numRecordsInBatch, duration);
    }

    SourceTaskMetricsGroup sourceTaskMetricsGroup() {
        return sourceTaskMetricsGroup;
    }

    static class SourceRecordWriteCounter {
        private final SourceTaskMetricsGroup metricsGroup;
        private final int batchSize;
        private boolean completed = false;
        private int counter;
        private int skipped; // Keeps track of filtered records

        public SourceRecordWriteCounter(int batchSize, SourceTaskMetricsGroup metricsGroup) {
            assert batchSize > 0;
            assert metricsGroup != null;
            this.batchSize = batchSize;
            counter = batchSize;
            this.metricsGroup = metricsGroup;
        }
        public void skipRecord() {
            skipped += 1;
            if (counter > 0 && --counter == 0) {
                finishedAllWrites();
            }
        }
        public void completeRecord() {
            if (counter > 0 && --counter == 0) {
                finishedAllWrites();
            }
        }
        public void retryRemaining() {
            finishedAllWrites();
        }
        private void finishedAllWrites() {
            if (!completed) {
                metricsGroup.recordWrite(batchSize - counter, skipped);
                completed = true;
            }
        }
    }

    static class SourceTaskMetricsGroup implements AutoCloseable {
        private final ConnectMetrics.MetricGroup metricGroup;
        private final Sensor sourceRecordPoll;
        private final Sensor sourceRecordWrite;
        private final Sensor sourceRecordActiveCount;
        private final Sensor pollTime;
        private int activeRecordCount;

        public SourceTaskMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics.group(registry.sourceTaskGroupName(),
                    registry.connectorTagName(), id.connector(),
                    registry.taskTagName(), Integer.toString(id.task()));
            // remove any previously created metrics in this group to prevent collisions.
            metricGroup.close();

            sourceRecordPoll = metricGroup.sensor("source-record-poll");
            sourceRecordPoll.add(metricGroup.metricName(registry.sourceRecordPollRate), new Rate());
            sourceRecordPoll.add(metricGroup.metricName(registry.sourceRecordPollTotal), new CumulativeSum());

            sourceRecordWrite = metricGroup.sensor("source-record-write");
            sourceRecordWrite.add(metricGroup.metricName(registry.sourceRecordWriteRate), new Rate());
            sourceRecordWrite.add(metricGroup.metricName(registry.sourceRecordWriteTotal), new CumulativeSum());

            pollTime = metricGroup.sensor("poll-batch-time");
            pollTime.add(metricGroup.metricName(registry.sourceRecordPollBatchTimeMax), new Max());
            pollTime.add(metricGroup.metricName(registry.sourceRecordPollBatchTimeAvg), new Avg());

            sourceRecordActiveCount = metricGroup.sensor("source-record-active-count");
            sourceRecordActiveCount.add(metricGroup.metricName(registry.sourceRecordActiveCount), new Value());
            sourceRecordActiveCount.add(metricGroup.metricName(registry.sourceRecordActiveCountMax), new Max());
            sourceRecordActiveCount.add(metricGroup.metricName(registry.sourceRecordActiveCountAvg), new Avg());
        }

        @Override
        public void close() {
            metricGroup.close();
        }

        void recordPoll(int batchSize, long duration) {
            sourceRecordPoll.record(batchSize);
            pollTime.record(duration);
            activeRecordCount += batchSize;
            sourceRecordActiveCount.record(activeRecordCount);
        }

        void recordWrite(int recordCount, int skippedCount) {
            sourceRecordWrite.record(recordCount - skippedCount);
            activeRecordCount -= recordCount;
            activeRecordCount = Math.max(0, activeRecordCount);
            sourceRecordActiveCount.record(activeRecordCount);
        }

        protected ConnectMetrics.MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}
