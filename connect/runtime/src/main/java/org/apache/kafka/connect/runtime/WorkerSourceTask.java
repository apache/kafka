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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.ConnectUtils;
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
    private final HeaderConverter headerConverter;
    private final TransformationChain<SourceRecord> transformationChain;
    private KafkaProducer<byte[], byte[]> producer;
    private final OffsetStorageReader offsetReader;
    private final OffsetStorageWriter offsetWriter;
    private final Time time;
    private final SourceTaskMetricsGroup sourceTaskMetricsGroup;

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
    private boolean stopped = false;

    public WorkerSourceTask(ConnectorTaskId id,
                            SourceTask task,
                            TaskStatus.Listener statusListener,
                            TargetState initialState,
                            Converter keyConverter,
                            Converter valueConverter,
                            HeaderConverter headerConverter,
                            TransformationChain<SourceRecord> transformationChain,
                            KafkaProducer<byte[], byte[]> producer,
                            OffsetStorageReader offsetReader,
                            OffsetStorageWriter offsetWriter,
                            WorkerConfig workerConfig,
                            ConnectMetrics connectMetrics,
                            ClassLoader loader,
                            Time time,
                            RetryWithToleranceOperator retryWithToleranceOperator) {

        super(id, statusListener, initialState, loader, connectMetrics, retryWithToleranceOperator);

        this.workerConfig = workerConfig;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
        this.transformationChain = transformationChain;
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
        this.sourceTaskMetricsGroup = new SourceTaskMetricsGroup(id, connectMetrics);
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
        if (!shouldPause()) {
            tryStop();
        }
        if (producer != null) {
            try {
                producer.close(30, TimeUnit.SECONDS);
            } catch (Throwable t) {
                log.warn("Could not close producer", t);
            }
        }
        try {
            transformationChain.close();
        } catch (Throwable t) {
            log.warn("Could not close transformation chain", t);
        }
    }

    @Override
    protected void releaseResources() {
        sourceTaskMetricsGroup.close();
    }

    @Override
    public void stop() {
        super.stop();
        stopRequestedLatch.countDown();
        synchronized (this) {
            if (finishedStart)
                tryStop();
            else
                startedShutdownBeforeStartCompleted = true;
        }
    }

    private synchronized void tryStop() {
        if (!stopped) {
            try {
                task.stop();
                stopped = true;
            } catch (Throwable t) {
                log.warn("Could not stop task", t);
            }
        }
    }

    @Override
    public void execute() {
        try {
            task.initialize(new WorkerSourceTaskContext(offsetReader));
            task.start(taskConfig);
            log.info("{} Source task finished initialization and start", this);
            synchronized (this) {
                if (startedShutdownBeforeStartCompleted) {
                    tryStop();
                    return;
                }
                finishedStart = true;
            }

            while (!isStopping()) {
                if (shouldPause()) {
                    onPause();
                    if (awaitUnpause()) {
                        onResume();
                    }
                    continue;
                }

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
                log.debug("{} About to send " + toSend.size() + " records to Kafka", this);
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

    protected List<SourceRecord> poll() throws InterruptedException {
        try {
            return task.poll();
        } catch (RetriableException e) {
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

        byte[] key = retryWithToleranceOperator.execute(() -> keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key()),
                Stage.KEY_CONVERTER, keyConverter.getClass());

        byte[] value = retryWithToleranceOperator.execute(() -> valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value()),
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
        final SourceRecordWriteCounter counter = new SourceRecordWriteCounter(toSend.size(), sourceTaskMetricsGroup);
        for (final SourceRecord preTransformRecord : toSend) {

            retryWithToleranceOperator.sourceRecord(preTransformRecord);
            final SourceRecord record = transformationChain.apply(preTransformRecord);
            final ProducerRecord<byte[], byte[]> producerRecord = convertTransformedRecord(record);
            if (producerRecord == null || retryWithToleranceOperator.failed()) {
                counter.skipRecord();
                commitTaskRecord(preTransformRecord);
                continue;
            }

            log.trace("{} Appending record with key {}, value {}", this, record.key(), record.value());
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
                final String topic = producerRecord.topic();
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
                                    log.error("{} failed to send record to {}: {}", this, topic, e);
                                    log.debug("{} Failed record: {}", this, preTransformRecord);
                                } else {
                                    log.trace("{} Wrote record successfully: topic {} partition {} offset {}",
                                            this,
                                            recordMetadata.topic(), recordMetadata.partition(),
                                            recordMetadata.offset());
                                    commitTaskRecord(preTransformRecord);
                                }
                                recordSent(producerRecord);
                                counter.completeRecord();
                            }
                        });
                lastSendFailed = false;
            } catch (RetriableException e) {
                log.warn("{} Failed to send {}, backing off before retrying:", this, producerRecord, e);
                toSend = toSend.subList(processed, toSend.size());
                lastSendFailed = true;
                counter.retryRemaining();
                return false;
            } catch (KafkaException e) {
                throw new ConnectException("Unrecoverable exception trying to send", e);
            }
            processed++;
        }
        toSend = null;
        return true;
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

    private void commitTaskRecord(SourceRecord record) {
        try {
            task.commitRecord(record);
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commitRecord()", this, t);
        }
    }

    private synchronized void recordSent(final ProducerRecord<byte[], byte[]> record) {
        ProducerRecord<byte[], byte[]> removed = outstandingMessages.remove(record);
        // While flushing, we may also see callbacks for items in the backlog
        if (removed == null && flushing)
            removed = outstandingMessagesBacklog.remove(record);
        // But if neither one had it, something is very wrong
        if (removed == null) {
            log.error("{} CRITICAL Saw callback for record that was not present in the outstanding message set: {}", this, record);
        } else if (flushing && outstandingMessages.isEmpty()) {
            // flush thread may be waiting on the outstanding messages to clear
            this.notifyAll();
        }
    }

    public boolean commitOffsets() {
        long commitTimeoutMs = workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);

        log.info("{} Committing offsets", this);

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
                    if (timeoutMs <= 0) {
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
        Future<Void> flushFuture = offsetWriter.doFlush(new org.apache.kafka.connect.util.Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                if (error != null) {
                    log.error("{} Failed to flush offsets to storage: ", this, error);
                } else {
                    log.trace("{} Finished flushing offsets to storage", this);
                }
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
        log.info("{} Finished commitOffsets successfully in {} ms",
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
            sourceRecordPoll.add(metricGroup.metricName(registry.sourceRecordPollTotal), new Total());

            sourceRecordWrite = metricGroup.sensor("source-record-write");
            sourceRecordWrite.add(metricGroup.metricName(registry.sourceRecordWriteRate), new Rate());
            sourceRecordWrite.add(metricGroup.metricName(registry.sourceRecordWriteTotal), new Total());

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
