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
import org.apache.kafka.clients.producer.KafkaProducer;
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
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.SubmittedRecords.SubmittedRecord;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.connect.runtime.SubmittedRecords.CommittableOffsets;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;

/**
 * WorkerTask that uses a SourceTask to ingest data into Kafka.
 */
class WorkerSourceTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSourceTask.class);

    private static final long SEND_FAILED_BACKOFF_MS = 100;

    private final WorkerConfig workerConfig;
    private final SourceTask task;
    private final ClusterConfigState configState;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final TransformationChain<SourceRecord> transformationChain;
    private final KafkaProducer<byte[], byte[]> producer;
    private final TopicAdmin admin;
    private final CloseableOffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final Executor closeExecutor;
    private final SourceTaskMetricsGroup sourceTaskMetricsGroup;
    private final AtomicReference<Exception> producerSendException;
    private final boolean isTopicTrackingEnabled;
    private final TopicCreation topicCreation;

    private List<SourceRecord> toSend;
    private volatile CommittableOffsets committableOffsets;
    private final SubmittedRecords submittedRecords;
    private final CountDownLatch stopRequestedLatch;

    private Map<String, String> taskConfig;
    private boolean started = false;

    public WorkerSourceTask(ConnectorTaskId id,
                            SourceTask task,
                            TaskStatus.Listener statusListener,
                            TargetState initialState,
                            Converter keyConverter,
                            Converter valueConverter,
                            HeaderConverter headerConverter,
                            TransformationChain<SourceRecord> transformationChain,
                            KafkaProducer<byte[], byte[]> producer,
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

        super(id, statusListener, initialState, loader, connectMetrics,
                retryWithToleranceOperator, time, statusBackingStore);

        this.workerConfig = workerConfig;
        this.task = task;
        this.configState = configState;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
        this.producer = producer;
        this.admin = admin;
        this.offsetReader = offsetReader;
        this.offsetWriter = offsetWriter;
        this.closeExecutor = closeExecutor;

        this.toSend = null;
        this.committableOffsets = CommittableOffsets.EMPTY;
        this.submittedRecords = new SubmittedRecords();
        this.stopRequestedLatch = new CountDownLatch(1);
        this.sourceTaskMetricsGroup = new SourceTaskMetricsGroup(id, connectMetrics);
        this.producerSendException = new AtomicReference<>();
        this.isTopicTrackingEnabled = workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG);
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
    protected void close() {
        if (started) {
            try {
                task.stop();
            } catch (Throwable t) {
                log.warn("Could not stop task", t);
            }
        }

        closeProducer(Duration.ofSeconds(30));

        if (admin != null) {
            try {
                admin.close(Duration.ofSeconds(30));
            } catch (Throwable t) {
                log.warn("Failed to close admin client on time", t);
            }
        }
        Utils.closeQuietly(transformationChain, "transformation chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
    }

    @Override
    public void removeMetrics() {
        try {
            sourceTaskMetricsGroup.close();
        } finally {
            super.removeMetrics();
        }
    }

    @Override
    public void cancel() {
        super.cancel();
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
    protected void initializeAndStart() {
        // If we try to start the task at all by invoking initialize, then count this as
        // "started" and expect a subsequent call to the task's stop() method
        // to properly clean up any resources allocated by its initialize() or
        // start() methods. If the task throws an exception during stop(),
        // the worst thing that happens is another exception gets logged for an already-
        // failed task
        started = true;
        task.initialize(new WorkerSourceTaskContext(offsetReader, this, configState));
        task.start(taskConfig);
        log.info("{} Source task finished initialization and start", this);
    }

    @Override
    public void execute() {
        try {
            log.info("{} Executing source task", this);
            while (!isStopping()) {
                updateCommittableOffsets();

                if (shouldPause()) {
                    onPause();
                    if (awaitUnpause()) {
                        onResume();
                    }
                    continue;
                }

                maybeThrowProducerSendException();
                if (toSend == null) {
                    log.trace("{} Nothing to send to Kafka. Polling source for additional records", this);
                    long start = time.milliseconds();
                    toSend = poll();
                    if (toSend != null) {
                        recordPollReturned(toSend.size(), time.milliseconds() - start);
                    }
                }

                if (toSend == null)
                    continue;
                log.trace("{} About to send {} records to Kafka", this, toSend.size());
                if (!sendRecords())
                    stopRequestedLatch.await(SEND_FAILED_BACKOFF_MS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            // Ignore and allow to exit.
        } finally {
            submittedRecords.awaitAllMessages(
                    workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG),
                    TimeUnit.MILLISECONDS
            );
            // It should still be safe to commit offsets since any exception would have
            // simply resulted in not getting more records but all the existing records should be ok to flush
            // and commit offsets. Worst case, task.flush() will also throw an exception causing the offset commit
            // to fail.
            updateCommittableOffsets();
            commitOffsets();
        }
    }

    private void closeProducer(Duration duration) {
        if (producer != null) {
            try {
                producer.close(duration);
            } catch (Throwable t) {
                log.warn("Could not close producer for {}", id, t);
            }
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

    private void updateCommittableOffsets() {
        CommittableOffsets newOffsets = submittedRecords.committableOffsets();
        synchronized (this) {
            this.committableOffsets = this.committableOffsets.updatedWith(newOffsets);
        }
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
    private ProducerRecord<byte[], byte[]> convertTransformedRecord(SourceRecord record) {
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

    /**
     * Try to send a batch of records. If a send fails and is retriable, this saves the remainder of the batch so it can
     * be retried after backing off. If a send fails and is not retriable, this will throw a ConnectException.
     * @return true if all messages were sent, false if some need to be retried
     */
    private boolean sendRecords() {
        int processed = 0;
        recordBatch(toSend.size());
        final SourceRecordWriteCounter counter =
                toSend.size() > 0 ? new SourceRecordWriteCounter(toSend.size(), sourceTaskMetricsGroup) : null;
        for (final SourceRecord preTransformRecord : toSend) {
            maybeThrowProducerSendException();

            retryWithToleranceOperator.sourceRecord(preTransformRecord);
            final SourceRecord record = transformationChain.apply(preTransformRecord);
            final ProducerRecord<byte[], byte[]> producerRecord = convertTransformedRecord(record);
            if (producerRecord == null || retryWithToleranceOperator.failed()) {
                counter.skipRecord();
                commitTaskRecord(preTransformRecord, null);
                continue;
            }

            log.trace("{} Appending record to the topic {} with key {}, value {}", this, record.topic(), record.key(), record.value());
            SubmittedRecord submittedRecord = submittedRecords.submit(record);
            try {
                maybeCreateTopic(record.topic());
                final String topic = producerRecord.topic();
                producer.send(
                    producerRecord,
                    (recordMetadata, e) -> {
                        if (e != null) {
                            if (retryWithToleranceOperator.getErrorToleranceType() == ToleranceType.ALL) {
                                log.trace("Ignoring failed record send: {} failed to send record to {}: ",
                                        WorkerSourceTask.this, topic, e);
                                // executeFailed here allows the use of existing logging infrastructure/configuration
                                retryWithToleranceOperator.executeFailed(Stage.KAFKA_PRODUCE, WorkerSourceTask.class,
                                        preTransformRecord, e);
                                commitTaskRecord(preTransformRecord, null);
                            } else {
                                log.error("{} failed to send record to {}: ", WorkerSourceTask.this, topic, e);
                                log.trace("{} Failed record: {}", WorkerSourceTask.this, preTransformRecord);
                                producerSendException.compareAndSet(null, e);
                            }
                        } else {
                            submittedRecord.ack();
                            counter.completeRecord();
                            log.trace("{} Wrote record successfully: topic {} partition {} offset {}",
                                    WorkerSourceTask.this,
                                    recordMetadata.topic(), recordMetadata.partition(),
                                    recordMetadata.offset());
                            commitTaskRecord(preTransformRecord, recordMetadata);
                            if (isTopicTrackingEnabled) {
                                recordActiveTopic(producerRecord.topic());
                            }
                        }
                    });
            } catch (RetriableException | org.apache.kafka.common.errors.RetriableException e) {
                log.warn("{} Failed to send record to topic '{}' and partition '{}'. Backing off before retrying: ",
                        this, producerRecord.topic(), producerRecord.partition(), e);
                toSend = toSend.subList(processed, toSend.size());
                submittedRecords.removeLastOccurrence(submittedRecord);
                counter.retryRemaining();
                return false;
            } catch (ConnectException e) {
                log.warn("{} Failed to send record to topic '{}' and partition '{}' due to an unrecoverable exception: ",
                        this, producerRecord.topic(), producerRecord.partition(), e);
                log.trace("{} Failed to send {} with unrecoverable exception: ", this, producerRecord, e);
                throw e;
            } catch (KafkaException e) {
                throw new ConnectException("Unrecoverable exception trying to send", e);
            }
            processed++;
        }
        toSend = null;
        return true;
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

    private RecordHeaders convertHeaderFor(SourceRecord record) {
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

    private void commitTaskRecord(SourceRecord record, RecordMetadata metadata) {
        try {
            task.commitRecord(record, metadata);
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commitRecord()", this, t);
        }
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

        if (committableOffsets.isEmpty()) {
            log.debug("{} Either no records were produced by the task since the last offset commit, " 
                    + "or every record has been filtered out by a transformation " 
                    + "or dropped due to transformation or conversion errors.",
                    this
            );
            // We continue with the offset commit process here instead of simply returning immediately
            // in order to invoke SourceTask::commit and record metrics for a successful offset commit
        } else {
            log.info("{} Committing offsets for {} acknowledged messages", this, committableOffsets.numCommittableMessages());
            if (committableOffsets.hasPending()) {
                log.debug("{} There are currently {} pending messages spread across {} source partitions whose offsets will not be committed. "
                                + "The source partition with the most pending messages is {}, with {} pending messages",
                        this,
                        committableOffsets.numUncommittableMessages(),
                        committableOffsets.numDeques(),
                        committableOffsets.largestDequePartition(),
                        committableOffsets.largestDequeSize()
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

        if (!offsetWriter.beginFlush()) {
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
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started, e);
            return false;
        } catch (ExecutionException e) {
            log.error("{} Flush of offsets threw an unexpected exception: ", this, e);
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started, e);
            return false;
        } catch (TimeoutException e) {
            log.error("{} Timed out waiting to flush offsets to storage; will try again on next flush interval with latest offsets", this);
            offsetWriter.cancelFlush();
            recordCommitFailure(time.milliseconds() - started, null);
            return false;
        }

        long durationMillis = time.milliseconds() - started;
        recordCommitSuccess(durationMillis);
        log.debug("{} Finished commitOffsets successfully in {} ms",
                this, durationMillis);

        commitSourceTask();

        return true;
    }

    private void commitSourceTask() {
        try {
            this.task.commit();
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commit()", this, t);
        }
    }

    @Override
    public String toString() {
        return "WorkerSourceTask{" +
                "id=" + id +
                '}';
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
        public SourceRecordWriteCounter(int batchSize, SourceTaskMetricsGroup metricsGroup) {
            assert batchSize > 0;
            assert metricsGroup != null;
            this.batchSize = batchSize;
            counter = batchSize;
            this.metricsGroup = metricsGroup;
        }
        public void skipRecord() {
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
                metricsGroup.recordWrite(batchSize - counter);
                completed = true;
            }
        }
    }

    static class SourceTaskMetricsGroup {
        private final MetricGroup metricGroup;
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

        void close() {
            metricGroup.close();
        }

        void recordPoll(int batchSize, long duration) {
            sourceRecordPoll.record(batchSize);
            pollTime.record(duration);
            activeRecordCount += batchSize;
            sourceRecordActiveCount.record(activeRecordCount);
        }

        void recordWrite(int recordCount) {
            sourceRecordWrite.record(recordCount);
            activeRecordCount -= recordCount;
            activeRecordCount = Math.max(0, activeRecordCount);
            sourceRecordActiveCount.record(activeRecordCount);
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}
