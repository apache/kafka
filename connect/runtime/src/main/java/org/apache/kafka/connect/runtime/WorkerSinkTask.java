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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.Utils.UncheckedCloseable;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;

/**
 * {@link WorkerTask} that uses a {@link SinkTask} to export data from Kafka.
 */
class WorkerSinkTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSinkTask.class);

    private final WorkerConfig workerConfig;
    private final SinkTask task;
    private final ClusterConfigState configState;
    private Map<String, String> taskConfig;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final HeaderConverter headerConverter;
    private final TransformationChain<SinkRecord> transformationChain;
    private final SinkTaskMetricsGroup sinkTaskMetricsGroup;
    private final boolean isTopicTrackingEnabled;
    private final Consumer<byte[], byte[]> consumer;
    private WorkerSinkTaskContext context;
    private final List<SinkRecord> messageBatch;
    private final Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private final Map<TopicPartition, OffsetAndMetadata> origOffsets;
    private RuntimeException rebalanceException;
    private long nextCommit;
    private int commitSeqno;
    private long commitStarted;
    private int commitFailures;
    private boolean pausedForRedelivery;
    private boolean committing;
    private boolean taskStopped;
    private final WorkerErrantRecordReporter workerErrantRecordReporter;
    private final Supplier<List<ErrorReporter>> errorReportersSupplier;

    public WorkerSinkTask(ConnectorTaskId id,
                          SinkTask task,
                          TaskStatus.Listener statusListener,
                          TargetState initialState,
                          WorkerConfig workerConfig,
                          ClusterConfigState configState,
                          ConnectMetrics connectMetrics,
                          Converter keyConverter,
                          Converter valueConverter,
                          ErrorHandlingMetrics errorMetrics,
                          HeaderConverter headerConverter,
                          TransformationChain<SinkRecord> transformationChain,
                          Consumer<byte[], byte[]> consumer,
                          ClassLoader loader,
                          Time time,
                          RetryWithToleranceOperator retryWithToleranceOperator,
                          WorkerErrantRecordReporter workerErrantRecordReporter,
                          StatusBackingStore statusBackingStore,
                          Supplier<List<ErrorReporter>> errorReportersSupplier) {
        super(id, statusListener, initialState, loader, connectMetrics, errorMetrics,
                retryWithToleranceOperator, time, statusBackingStore);

        this.workerConfig = workerConfig;
        this.task = task;
        this.configState = configState;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
        this.messageBatch = new ArrayList<>();
        this.lastCommittedOffsets = new HashMap<>();
        this.currentOffsets = new HashMap<>();
        this.origOffsets = new HashMap<>();
        this.pausedForRedelivery = false;
        this.rebalanceException = null;
        this.nextCommit = time.milliseconds() +
                workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        this.commitFailures = 0;
        this.sinkTaskMetricsGroup = new SinkTaskMetricsGroup(id, connectMetrics);
        this.sinkTaskMetricsGroup.recordOffsetSequenceNumber(commitSeqno);
        this.consumer = consumer;
        this.isTopicTrackingEnabled = workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG);
        this.taskStopped = false;
        this.workerErrantRecordReporter = workerErrantRecordReporter;
        this.errorReportersSupplier = errorReportersSupplier;
    }

    @Override
    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
            this.context = new WorkerSinkTaskContext(consumer, this, configState);
        } catch (Throwable t) {
            log.error("{} Task failed initialization and will not be started.", this, t);
            onFailure(t);
        }
    }

    @Override
    public void stop() {
        // Offset commit is handled upon exit in work thread
        super.stop();
        consumer.wakeup();
    }

    @Override
    protected void close() {
        // FIXME Kafka needs to add a timeout parameter here for us to properly obey the timeout
        // passed in
        try {
            task.stop();
        } catch (Throwable t) {
            log.warn("Could not stop task", t);
        }
        taskStopped = true;
        Utils.closeQuietly(consumer, "consumer");
        Utils.closeQuietly(transformationChain, "transformation chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
        Utils.closeQuietly(headerConverter, "header converter");
        /*
            Setting partition count explicitly to 0 to handle the case,
            when the task fails, which would cause its consumer to leave the group.
            This would cause onPartitionsRevoked to be invoked in the rebalance listener, but not onPartitionsAssigned,
            so the metrics for the task (which are still available for failed tasks until they are explicitly revoked
            from the worker) would become inaccurate.
        */
        sinkTaskMetricsGroup.recordPartitionCount(0);
    }

    @Override
    public void removeMetrics() {
        try {
            sinkTaskMetricsGroup.close();
        } finally {
            super.removeMetrics();
        }
    }

    @Override
    public void transitionTo(TargetState state) {
        super.transitionTo(state);
        consumer.wakeup();
    }

    @Override
    public void execute() {
        log.info("{} Executing sink task", this);
        // Make sure any uncommitted data has been committed and the task has
        // a chance to clean up its state
        try (UncheckedCloseable suppressible = this::closeAllPartitions) {
            while (!isStopping())
                iteration();
        } catch (WakeupException e) {
            log.trace("Consumer woken up during initial offset commit attempt, " 
                + "but succeeded during a later attempt");
        }
    }

    protected void iteration() {
        final long offsetCommitIntervalMs = workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);

        try {
            long now = time.milliseconds();

            // Maybe commit
            if (!committing && (context.isCommitRequested() || now >= nextCommit)) {
                commitOffsets(now, false);
                nextCommit = now + offsetCommitIntervalMs;
                context.clearCommitRequest();
            }

            final long commitTimeoutMs = commitStarted + workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);

            // Check for timed out commits
            if (committing && now >= commitTimeoutMs) {
                log.warn("{} Commit of offsets timed out", this);
                commitFailures++;
                committing = false;
            }

            // And process messages
            long timeoutMs = Math.max(nextCommit - now, 0);
            poll(timeoutMs);
        } catch (WakeupException we) {
            log.trace("{} Consumer woken up", this);

            if (isStopping())
                return;

            if (shouldPause()) {
                pauseAll();
                onPause();
                context.requestCommit();
            } else if (!pausedForRedelivery) {
                resumeAll();
                onResume();
            }
        }
    }

    /**
     * Respond to a previous commit attempt that may or may not have succeeded. Note that due to our use of async commits,
     * these invocations may come out of order and thus the need for the commit sequence number.
     *
     * @param error            the error resulting from the commit, or null if the commit succeeded without error
     * @param seqno            the sequence number at the time the commit was requested
     * @param committedOffsets the offsets that were committed; may be null if the commit did not complete successfully
     *                         or if no new offsets were committed
     */
    private void onCommitCompleted(Throwable error, long seqno, Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        if (commitSeqno != seqno) {
            log.debug("{} Received out of order commit callback for sequence number {}, but most recent sequence number is {}",
                    this, seqno, commitSeqno);
            sinkTaskMetricsGroup.recordOffsetCommitSkip();
        } else {
            long durationMillis = time.milliseconds() - commitStarted;
            if (error != null) {
                log.error("{} Commit of offsets threw an unexpected exception for sequence number {}: {}",
                        this, seqno, committedOffsets, error);
                commitFailures++;
                recordCommitFailure(durationMillis, error);
            } else {
                log.debug("{} Finished offset commit successfully in {} ms for sequence number {}: {}",
                        this, durationMillis, seqno, committedOffsets);
                if (committedOffsets != null) {
                    log.trace("{} Adding to last committed offsets: {}", this, committedOffsets);
                    lastCommittedOffsets.putAll(committedOffsets);
                    log.debug("{} Last committed offsets are now {}", this, committedOffsets);
                    sinkTaskMetricsGroup.recordCommittedOffsets(committedOffsets);
                }
                commitFailures = 0;
                recordCommitSuccess(durationMillis);
            }
            committing = false;
        }
    }

    public int commitFailures() {
        return commitFailures;
    }

    /**
     * Initializes and starts the SinkTask.
     */
    @Override
    protected void initializeAndStart() {
        SinkConnectorConfig.validate(taskConfig);
        retryWithToleranceOperator.reporters(errorReportersSupplier.get());

        if (SinkConnectorConfig.hasTopicsConfig(taskConfig)) {
            List<String> topics = SinkConnectorConfig.parseTopicsList(taskConfig);
            consumer.subscribe(topics, new HandleRebalance());
            log.debug("{} Initializing and starting task for topics {}", this, Utils.join(topics, ", "));
        } else {
            String topicsRegexStr = taskConfig.get(SinkTask.TOPICS_REGEX_CONFIG);
            Pattern pattern = Pattern.compile(topicsRegexStr);
            consumer.subscribe(pattern, new HandleRebalance());
            log.debug("{} Initializing and starting task for topics regex {}", this, topicsRegexStr);
        }

        task.initialize(context);
        task.start(taskConfig);
        log.info("{} Sink task finished initialization and start", this);
    }

    /**
     * Poll for new messages with the given timeout. Should only be invoked by the worker thread.
     */
    protected void poll(long timeoutMs) {
        rewind();
        long retryTimeout = context.timeout();
        if (retryTimeout > 0) {
            timeoutMs = Math.min(timeoutMs, retryTimeout);
            context.timeout(-1L);
        }

        log.trace("{} Polling consumer with timeout {} ms", this, timeoutMs);
        ConsumerRecords<byte[], byte[]> msgs = pollConsumer(timeoutMs);
        assert messageBatch.isEmpty() || msgs.isEmpty();
        log.trace("{} Polling returned {} messages", this, msgs.count());

        convertMessages(msgs);
        deliverMessages();
    }

    // Visible for testing
    boolean isCommitting() {
        return committing;
    }

    private void doCommitSync(Map<TopicPartition, OffsetAndMetadata> offsets, int seqno) {
        log.debug("{} Committing offsets synchronously using sequence number {}: {}", this, seqno, offsets);
        try {
            consumer.commitSync(offsets);
            onCommitCompleted(null, seqno, offsets);
        } catch (WakeupException e) {
            // retry the commit to ensure offsets get pushed, then propagate the wakeup up to poll
            doCommitSync(offsets, seqno);
            throw e;
        } catch (KafkaException e) {
            onCommitCompleted(e, seqno, offsets);
        }
    }

    private void doCommitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, final int seqno) {
        log.debug("{} Committing offsets asynchronously using sequence number {}: {}", this, seqno, offsets);
        OffsetCommitCallback cb = (tpOffsets, error) -> onCommitCompleted(error, seqno, tpOffsets);
        consumer.commitAsync(offsets, cb);
    }

    /**
     * Starts an offset commit by flushing outstanding messages from the task and then starting
     * the write commit.
     */
    private void doCommit(Map<TopicPartition, OffsetAndMetadata> offsets, boolean closing, int seqno) {
        if (isCancelled()) {
            log.debug("Skipping final offset commit as task has been cancelled");
            return;
        }
        if (closing) {
            doCommitSync(offsets, seqno);
        } else {
            doCommitAsync(offsets, seqno);
        }
    }

    private void commitOffsets(long now, boolean closing) {
        commitOffsets(now, closing, consumer.assignment());
    }

    private void commitOffsets(long now, boolean closing, Collection<TopicPartition> topicPartitions) {
        log.trace("Committing offsets for partitions {}", topicPartitions);
        if (workerErrantRecordReporter != null) {
            log.trace("Awaiting reported errors to be completed");
            workerErrantRecordReporter.awaitFutures(topicPartitions);
            log.trace("Completed reported errors");
        }

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = currentOffsets.entrySet().stream()
            .filter(e -> topicPartitions.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (offsetsToCommit.isEmpty())
            return;

        committing = true;
        commitSeqno += 1;
        commitStarted = now;
        sinkTaskMetricsGroup.recordOffsetSequenceNumber(commitSeqno);

        Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsetsForPartitions = this.lastCommittedOffsets.entrySet().stream()
            .filter(e -> offsetsToCommit.containsKey(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final Map<TopicPartition, OffsetAndMetadata> taskProvidedOffsets;
        try {
            log.trace("{} Calling task.preCommit with current offsets: {}", this, offsetsToCommit);
            taskProvidedOffsets = task.preCommit(new HashMap<>(offsetsToCommit));
        } catch (Throwable t) {
            if (closing) {
                log.warn("{} Offset commit failed during close", this);
            } else {
                log.error("{} Offset commit failed, rewinding to last committed offsets", this, t);
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : lastCommittedOffsetsForPartitions.entrySet()) {
                    log.debug("{} Rewinding topic partition {} to offset {}", this, entry.getKey(), entry.getValue().offset());
                    consumer.seek(entry.getKey(), entry.getValue().offset());
                }
                currentOffsets.putAll(lastCommittedOffsetsForPartitions);
            }
            onCommitCompleted(t, commitSeqno, null);
            return;
        } finally {
            if (closing) {
                log.trace("{} Closing the task before committing the offsets: {}", this, offsetsToCommit);
                task.close(topicPartitions);
            }
        }

        if (taskProvidedOffsets.isEmpty()) {
            log.debug("{} Skipping offset commit, task opted-out by returning no offsets from preCommit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }

        Collection<TopicPartition> allAssignedTopicPartitions = consumer.assignment();
        final Map<TopicPartition, OffsetAndMetadata> committableOffsets = new HashMap<>(lastCommittedOffsetsForPartitions);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> taskProvidedOffsetEntry : taskProvidedOffsets.entrySet()) {
            final TopicPartition partition = taskProvidedOffsetEntry.getKey();
            final OffsetAndMetadata taskProvidedOffset = taskProvidedOffsetEntry.getValue();
            if (committableOffsets.containsKey(partition)) {
                long taskOffset = taskProvidedOffset.offset();
                long currentOffset = offsetsToCommit.get(partition).offset();
                if (taskOffset <= currentOffset) {
                    committableOffsets.put(partition, taskProvidedOffset);
                } else {
                    log.warn("{} Ignoring invalid task provided offset {}/{} -- not yet consumed, taskOffset={} currentOffset={}",
                        this, partition, taskProvidedOffset, taskOffset, currentOffset);
                }
            } else if (!allAssignedTopicPartitions.contains(partition)) {
                log.warn("{} Ignoring invalid task provided offset {}/{} -- partition not assigned, assignment={}",
                        this, partition, taskProvidedOffset, allAssignedTopicPartitions);
            } else {
                log.debug("{} Ignoring task provided offset {}/{} -- partition not requested, requested={}",
                        this, partition, taskProvidedOffset, committableOffsets.keySet());
            }
        }

        if (committableOffsets.equals(lastCommittedOffsetsForPartitions)) {
            log.debug("{} Skipping offset commit, no change since last commit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }

        doCommit(committableOffsets, closing, commitSeqno);
    }


    @Override
    public String toString() {
        return "WorkerSinkTask{" +
                "id=" + id +
                '}';
    }

    private ConsumerRecords<byte[], byte[]> pollConsumer(long timeoutMs) {
        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(Duration.ofMillis(timeoutMs));

        // Exceptions raised from the task during a rebalance should be rethrown to stop the task and mark it as failed
        if (rebalanceException != null) {
            RuntimeException e = rebalanceException;
            rebalanceException = null;
            throw e;
        }

        sinkTaskMetricsGroup.recordRead(msgs.count());
        return msgs;
    }

    private void convertMessages(ConsumerRecords<byte[], byte[]> msgs) {
        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
            log.trace("{} Consuming and converting message in topic '{}' partition {} at offset {} and timestamp {}",
                    this, msg.topic(), msg.partition(), msg.offset(), msg.timestamp());

            retryWithToleranceOperator.consumerRecord(msg);

            SinkRecord transRecord = convertAndTransformRecord(msg);

            origOffsets.put(
                    new TopicPartition(msg.topic(), msg.partition()),
                    new OffsetAndMetadata(msg.offset() + 1)
            );
            if (transRecord != null) {
                messageBatch.add(transRecord);
            } else {
                log.trace(
                        "{} Converters and transformations returned null, possibly because of too many retries, so " +
                                "dropping record in topic '{}' partition {} at offset {}",
                        this, msg.topic(), msg.partition(), msg.offset()
                );
            }
        }
        sinkTaskMetricsGroup.recordConsumedOffsets(origOffsets);
    }

    private SinkRecord convertAndTransformRecord(final ConsumerRecord<byte[], byte[]> msg) {
        SchemaAndValue keyAndSchema = retryWithToleranceOperator.execute(() -> keyConverter.toConnectData(msg.topic(), msg.headers(), msg.key()),
                Stage.KEY_CONVERTER, keyConverter.getClass());

        SchemaAndValue valueAndSchema = retryWithToleranceOperator.execute(() -> valueConverter.toConnectData(msg.topic(), msg.headers(), msg.value()),
                Stage.VALUE_CONVERTER, valueConverter.getClass());

        Headers headers = retryWithToleranceOperator.execute(() -> convertHeadersFor(msg), Stage.HEADER_CONVERTER, headerConverter.getClass());

        if (retryWithToleranceOperator.failed()) {
            return null;
        }

        Long timestamp = ConnectUtils.checkAndConvertTimestamp(msg.timestamp());
        SinkRecord origRecord = new SinkRecord(msg.topic(), msg.partition(),
                keyAndSchema.schema(), keyAndSchema.value(),
                valueAndSchema.schema(), valueAndSchema.value(),
                msg.offset(),
                timestamp,
                msg.timestampType(),
                headers);
        log.trace("{} Applying transformations to record in topic '{}' partition {} at offset {} and timestamp {} with key {} and value {}",
                this, msg.topic(), msg.partition(), msg.offset(), timestamp, keyAndSchema.value(), valueAndSchema.value());
        if (isTopicTrackingEnabled) {
            recordActiveTopic(origRecord.topic());
        }

        // Apply the transformations
        SinkRecord transformedRecord = transformationChain.apply(origRecord);
        if (transformedRecord == null) {
            return null;
        }
        // Error reporting will need to correlate each sink record with the original consumer record
        return new InternalSinkRecord(msg, transformedRecord);
    }

    private Headers convertHeadersFor(ConsumerRecord<byte[], byte[]> record) {
        Headers result = new ConnectHeaders();
        org.apache.kafka.common.header.Headers recordHeaders = record.headers();
        if (recordHeaders != null) {
            String topic = record.topic();
            for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                SchemaAndValue schemaAndValue = headerConverter.toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                result.add(recordHeader.key(), schemaAndValue);
            }
        }
        return result;
    }

    protected WorkerErrantRecordReporter workerErrantRecordReporter() {
        return workerErrantRecordReporter;
    }

    private void resumeAll() {
        for (TopicPartition tp : consumer.assignment())
            if (!context.pausedPartitions().contains(tp))
                consumer.resume(singleton(tp));
    }

    private void pauseAll() {
        consumer.pause(consumer.assignment());
    }

    private void deliverMessages() {
        // Finally, deliver this batch to the sink
        try {
            // Since we reuse the messageBatch buffer, ensure we give the task its own copy
            log.trace("{} Delivering batch of {} messages to task", this, messageBatch.size());
            long start = time.milliseconds();
            task.put(new ArrayList<>(messageBatch));
            // if errors raised from the operator were swallowed by the task implementation, an
            // exception needs to be thrown to kill the task indicating the tolerance was exceeded
            if (retryWithToleranceOperator.failed() && !retryWithToleranceOperator.withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler",
                    retryWithToleranceOperator.error());
            }
            recordBatch(messageBatch.size());
            sinkTaskMetricsGroup.recordPut(time.milliseconds() - start);
            currentOffsets.putAll(origOffsets);
            origOffsets.clear();
            messageBatch.clear();
            // If we had paused all consumer topic partitions to try to redeliver data, then we should resume any that
            // the task had not explicitly paused
            if (pausedForRedelivery) {
                if (!shouldPause())
                    resumeAll();
                pausedForRedelivery = false;
            }
        } catch (RetriableException e) {
            log.error("{} RetriableException from SinkTask:", this, e);
            if (!pausedForRedelivery) {
                // If we're retrying a previous batch, make sure we've paused all topic partitions so we don't get new data,
                // but will still be able to poll in order to handle user-requested timeouts, keep group membership, etc.
                pausedForRedelivery = true;
                pauseAll();
            }
            // Let this exit normally, the batch will be reprocessed on the next loop.
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not "
                    + "recover until manually restarted. Error: {}", this, t.getMessage(), t);
            throw new ConnectException("Exiting WorkerSinkTask due to unrecoverable exception.", t);
        }
    }

    private void rewind() {
        Map<TopicPartition, Long> offsets = context.offsets();
        if (offsets.isEmpty()) {
            return;
        }
        for (Map.Entry<TopicPartition, Long> entry: offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            Long offset = entry.getValue();
            if (offset != null) {
                log.trace("{} Rewind {} to offset {}", this, tp, offset);
                consumer.seek(tp, offset);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(offset));
                currentOffsets.put(tp, new OffsetAndMetadata(offset));
            } else {
                log.warn("{} Cannot rewind {} to null offset", this, tp);
            }
        }
        context.clearOffsets();
    }

    private void openPartitions(Collection<TopicPartition> partitions) {
        task.open(partitions);
    }

    private void closeAllPartitions() {
        closePartitions(currentOffsets.keySet(), false);
    }

    private void closePartitions(Collection<TopicPartition> topicPartitions, boolean lost) {
        if (!lost) {
            commitOffsets(time.milliseconds(), true, topicPartitions);
        } else {
            log.trace("{} Closing the task as partitions have been lost: {}", this, topicPartitions);
            task.close(topicPartitions);
            if (workerErrantRecordReporter != null) {
                log.trace("Cancelling reported errors for {}", topicPartitions);
                workerErrantRecordReporter.cancelFutures(topicPartitions);
                log.trace("Cancelled all reported errors for {}", topicPartitions);
            }
            origOffsets.keySet().removeAll(topicPartitions);
            currentOffsets.keySet().removeAll(topicPartitions);
        }
        lastCommittedOffsets.keySet().removeAll(topicPartitions);
    }

    private void updatePartitionCount() {
        sinkTaskMetricsGroup.recordPartitionCount(consumer.assignment().size());
    }

    @Override
    protected void recordBatch(int size) {
        super.recordBatch(size);
        sinkTaskMetricsGroup.recordSend(size);
    }

    @Override
    protected void recordCommitSuccess(long duration) {
        super.recordCommitSuccess(duration);
        sinkTaskMetricsGroup.recordOffsetCommitSuccess();
    }

    SinkTaskMetricsGroup sinkTaskMetricsGroup() {
        return sinkTaskMetricsGroup;
    }

    // Visible for testing
    long getNextCommit() {
        return nextCommit;
    }

    private class HandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.debug("{} Partitions assigned {}", WorkerSinkTask.this, partitions);

            for (TopicPartition tp : partitions) {
                long pos = consumer.position(tp);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(pos));
                currentOffsets.put(tp, new OffsetAndMetadata(pos));
                log.debug("{} Assigned topic partition {} with offset {}", WorkerSinkTask.this, tp, pos);
            }
            sinkTaskMetricsGroup.assignedOffsets(currentOffsets);

            boolean wasPausedForRedelivery = pausedForRedelivery;
            pausedForRedelivery = wasPausedForRedelivery && !messageBatch.isEmpty();
            if (pausedForRedelivery) {
                // Re-pause here in case we picked up new partitions in the rebalance
                pauseAll();
            } else {
                // If we paused everything for redelivery and all partitions for the failed deliveries have been revoked, make
                // sure anything we paused that the task didn't request to be paused *and* which we still own is resumed.
                // Also make sure our tracking of paused partitions is updated to remove any partitions we no longer own.
                if (wasPausedForRedelivery) {
                    resumeAll();
                }
                // Ensure that the paused partitions contains only assigned partitions and repause as necessary
                context.pausedPartitions().retainAll(consumer.assignment());
                if (shouldPause())
                    pauseAll();
                else if (!context.pausedPartitions().isEmpty())
                    consumer.pause(context.pausedPartitions());
            }
            updatePartitionCount();
            if (partitions.isEmpty()) {
                return;
            }

            // Instead of invoking the assignment callback on initialization, we guarantee the consumer is ready upon
            // task start. Since this callback gets invoked during that initial setup before we've started the task, we
            // need to guard against invoking the user's callback method during that period.
            if (rebalanceException == null || rebalanceException instanceof WakeupException) {
                try {
                    openPartitions(partitions);
                    // Rewind should be applied only if openPartitions succeeds.
                    rewind();
                } catch (RuntimeException e) {
                    // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                    // exceptions and rethrow when poll() returns.
                    rebalanceException = e;
                }
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            onPartitionsRemoved(partitions, false);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            onPartitionsRemoved(partitions, true);
        }

        private void onPartitionsRemoved(Collection<TopicPartition> partitions, boolean lost) {
            if (taskStopped) {
                log.trace("Skipping partition revocation callback as task has already been stopped");
                return;
            }
            log.debug("{} Partitions {}: {}", WorkerSinkTask.this, lost ? "lost" : "revoked", partitions);

            if (partitions.isEmpty())
                return;

            try {
                closePartitions(partitions, lost);
                sinkTaskMetricsGroup.clearOffsets(partitions);
            } catch (RuntimeException e) {
                // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                // exceptions and rethrow when poll() returns.
                rebalanceException = e;
            }

            // Make sure we don't have any leftover data since offsets for these partitions will be reset to committed positions
            messageBatch.removeIf(record -> partitions.contains(new TopicPartition(record.topic(), record.kafkaPartition())));
        }
    }

    static class SinkTaskMetricsGroup {
        private final ConnectorTaskId id;
        private final ConnectMetrics metrics;
        private final MetricGroup metricGroup;
        private final Sensor sinkRecordRead;
        private final Sensor sinkRecordSend;
        private final Sensor partitionCount;
        private final Sensor offsetSeqNum;
        private final Sensor offsetCompletion;
        private final Sensor offsetCompletionSkip;
        private final Sensor putBatchTime;
        private final Sensor sinkRecordActiveCount;
        private long activeRecords;
        private Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new HashMap<>();
        private Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

        public SinkTaskMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
            this.metrics = connectMetrics;
            this.id = id;

            ConnectMetricsRegistry registry = connectMetrics.registry();
            metricGroup = connectMetrics
                                  .group(registry.sinkTaskGroupName(), registry.connectorTagName(), id.connector(), registry.taskTagName(),
                                         Integer.toString(id.task()));
            // prevent collisions by removing any previously created metrics in this group.
            metricGroup.close();

            sinkRecordRead = metricGroup.sensor("sink-record-read");
            sinkRecordRead.add(metricGroup.metricName(registry.sinkRecordReadRate), new Rate());
            sinkRecordRead.add(metricGroup.metricName(registry.sinkRecordReadTotal), new CumulativeSum());

            sinkRecordSend = metricGroup.sensor("sink-record-send");
            sinkRecordSend.add(metricGroup.metricName(registry.sinkRecordSendRate), new Rate());
            sinkRecordSend.add(metricGroup.metricName(registry.sinkRecordSendTotal), new CumulativeSum());

            sinkRecordActiveCount = metricGroup.sensor("sink-record-active-count");
            sinkRecordActiveCount.add(metricGroup.metricName(registry.sinkRecordActiveCount), new Value());
            sinkRecordActiveCount.add(metricGroup.metricName(registry.sinkRecordActiveCountMax), new Max());
            sinkRecordActiveCount.add(metricGroup.metricName(registry.sinkRecordActiveCountAvg), new Avg());

            partitionCount = metricGroup.sensor("partition-count");
            partitionCount.add(metricGroup.metricName(registry.sinkRecordPartitionCount), new Value());

            offsetSeqNum = metricGroup.sensor("offset-seq-number");
            offsetSeqNum.add(metricGroup.metricName(registry.sinkRecordOffsetCommitSeqNum), new Value());

            offsetCompletion = metricGroup.sensor("offset-commit-completion");
            offsetCompletion.add(metricGroup.metricName(registry.sinkRecordOffsetCommitCompletionRate), new Rate());
            offsetCompletion.add(metricGroup.metricName(registry.sinkRecordOffsetCommitCompletionTotal), new CumulativeSum());

            offsetCompletionSkip = metricGroup.sensor("offset-commit-completion-skip");
            offsetCompletionSkip.add(metricGroup.metricName(registry.sinkRecordOffsetCommitSkipRate), new Rate());
            offsetCompletionSkip.add(metricGroup.metricName(registry.sinkRecordOffsetCommitSkipTotal), new CumulativeSum());

            putBatchTime = metricGroup.sensor("put-batch-time");
            putBatchTime.add(metricGroup.metricName(registry.sinkRecordPutBatchTimeMax), new Max());
            putBatchTime.add(metricGroup.metricName(registry.sinkRecordPutBatchTimeAvg), new Avg());
        }

        void computeSinkRecordLag() {
            Map<TopicPartition, OffsetAndMetadata> consumed = this.consumedOffsets;
            Map<TopicPartition, OffsetAndMetadata> committed = this.committedOffsets;
            activeRecords = 0L;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> committedOffsetEntry : committed.entrySet()) {
                final TopicPartition partition = committedOffsetEntry.getKey();
                final OffsetAndMetadata consumedOffsetMeta = consumed.get(partition);
                if (consumedOffsetMeta != null) {
                    final OffsetAndMetadata committedOffsetMeta = committedOffsetEntry.getValue();
                    long consumedOffset = consumedOffsetMeta.offset();
                    long committedOffset = committedOffsetMeta.offset();
                    long diff = consumedOffset - committedOffset;
                    // Connector tasks can return offsets, so make sure nothing wonky happens
                    activeRecords += Math.max(diff, 0L);
                }
            }
            sinkRecordActiveCount.record(activeRecords);
        }

        void close() {
            metricGroup.close();
        }

        void recordRead(int batchSize) {
            sinkRecordRead.record(batchSize);
        }

        void recordSend(int batchSize) {
            sinkRecordSend.record(batchSize);
        }

        void recordPut(long duration) {
            putBatchTime.record(duration);
        }

        void recordPartitionCount(int assignedPartitionCount) {
            partitionCount.record(assignedPartitionCount);
        }

        void recordOffsetSequenceNumber(int seqNum) {
            offsetSeqNum.record(seqNum);
        }

        void recordConsumedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            consumedOffsets.putAll(offsets);
            computeSinkRecordLag();
        }

        void recordCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            committedOffsets = offsets;
            computeSinkRecordLag();
        }

        void assignedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            consumedOffsets = new HashMap<>(offsets);
            committedOffsets = offsets;
            computeSinkRecordLag();
        }

        void clearOffsets(Collection<TopicPartition> topicPartitions) {
            consumedOffsets.keySet().removeAll(topicPartitions);
            committedOffsets.keySet().removeAll(topicPartitions);
            computeSinkRecordLag();
        }

        void recordOffsetCommitSuccess() {
            offsetCompletion.record(1.0);
        }

        void recordOffsetCommitSkip() {
            offsetCompletionSkip.record(1.0);
        }

        protected MetricGroup metricGroup() {
            return metricGroup;
        }
    }
}
