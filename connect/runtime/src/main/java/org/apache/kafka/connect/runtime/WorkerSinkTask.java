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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.SinkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;

/**
 * WorkerTask that uses a SinkTask to export data from Kafka.
 */
class WorkerSinkTask extends WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSinkTask.class);

    private final WorkerConfig workerConfig;
    private final SinkTask task;
    private Map<String, String> taskConfig;
    private final Time time;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final TransformationChain<SinkRecord> transformationChain;
    private KafkaConsumer<byte[], byte[]> consumer;
    private WorkerSinkTaskContext context;
    private final List<SinkRecord> messageBatch;
    private Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private RuntimeException rebalanceException;
    private long nextCommit;
    private int commitSeqno;
    private long commitStarted;
    private int commitFailures;
    private boolean pausedForRedelivery;
    private boolean committing;

    public WorkerSinkTask(ConnectorTaskId id,
                          SinkTask task,
                          TaskStatus.Listener statusListener,
                          TargetState initialState,
                          WorkerConfig workerConfig,
                          Converter keyConverter,
                          Converter valueConverter,
                          TransformationChain<SinkRecord> transformationChain,
                          Time time) {
        super(id, statusListener, initialState);

        this.workerConfig = workerConfig;
        this.task = task;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.transformationChain = transformationChain;
        this.time = time;
        this.messageBatch = new ArrayList<>();
        this.currentOffsets = new HashMap<>();
        this.pausedForRedelivery = false;
        this.rebalanceException = null;
        this.nextCommit = time.milliseconds() +
                workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        this.commitFailures = 0;
    }

    @Override
    public void initialize(TaskConfig taskConfig) {
        try {
            this.taskConfig = taskConfig.originalsStrings();
            this.consumer = createConsumer();
            this.context = new WorkerSinkTaskContext(consumer);
        } catch (Throwable t) {
            log.error("Task {} failed initialization and will not be started.", t);
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
        task.stop();
        if (consumer != null)
            consumer.close();
        transformationChain.close();
    }

    @Override
    public void transitionTo(TargetState state) {
        super.transitionTo(state);
        consumer.wakeup();
    }

    @Override
    public void execute() {
        initializeAndStart();
        try {
            while (!isStopping())
                iteration();
        } finally {
            // Make sure any uncommitted data has been committed and the task has
            // a chance to clean up its state
            closePartitions();
        }
    }

    protected void iteration() {
        final long offsetCommitIntervalMs = workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);
        final long commitTimeoutMs = commitStarted + workerConfig.getLong(WorkerConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);

        try {
            long now = time.milliseconds();

            // Maybe commit
            if (!committing && (context.isCommitRequested() || now >= nextCommit)) {
                commitOffsets(now, false);
                nextCommit += offsetCommitIntervalMs;
                context.clearCommitRequest();
            }

            // Check for timed out commits
            if (committing && now >= commitTimeoutMs) {
                log.warn("Commit of {} offsets timed out", this);
                commitFailures++;
                committing = false;
            }

            // And process messages
            long timeoutMs = Math.max(nextCommit - now, 0);
            poll(timeoutMs);
        } catch (WakeupException we) {
            log.trace("{} consumer woken up", id);

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

    private void onCommitCompleted(Throwable error, long seqno) {
        if (commitSeqno != seqno) {
            log.debug("Got callback for timed out commit {}: {}, but most recent commit is {}",
                    this,
                    seqno, commitSeqno);
        } else {
            if (error != null) {
                log.error("Commit of {} offsets threw an unexpected exception: ", this, error);
                commitFailures++;
            } else {
                log.debug("Finished {} offset commit successfully in {} ms",
                        this, time.milliseconds() - commitStarted);
                commitFailures = 0;
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
    protected void initializeAndStart() {
        log.debug("Initializing task {} ", id);
        String topicsStr = taskConfig.get(SinkTask.TOPICS_CONFIG);
        if (topicsStr == null || topicsStr.isEmpty())
            throw new ConnectException("Sink tasks require a list of topics.");
        String[] topics = topicsStr.split(",");
        log.debug("Task {} subscribing to topics {}", id, topics);
        consumer.subscribe(Arrays.asList(topics), new HandleRebalance());
        task.initialize(context);
        task.start(taskConfig);
        log.info("Sink task {} finished initialization and start", this);
    }

    /** Poll for new messages with the given timeout. Should only be invoked by the worker thread. */
    protected void poll(long timeoutMs) {
        rewind();
        long retryTimeout = context.timeout();
        if (retryTimeout > 0) {
            timeoutMs = Math.min(timeoutMs, retryTimeout);
            context.timeout(-1L);
        }

        log.trace("{} polling consumer with timeout {} ms", id, timeoutMs);
        ConsumerRecords<byte[], byte[]> msgs = pollConsumer(timeoutMs);
        assert messageBatch.isEmpty() || msgs.isEmpty();
        log.trace("{} polling returned {} messages", id, msgs.count());

        convertMessages(msgs);
        deliverMessages();
    }

    private void doCommitSync(Map<TopicPartition, OffsetAndMetadata> offsets, int seqno) {
        try {
            consumer.commitSync(offsets);
            lastCommittedOffsets = offsets;
            onCommitCompleted(null, seqno);
        } catch (WakeupException e) {
            // retry the commit to ensure offsets get pushed, then propagate the wakeup up to poll
            doCommitSync(offsets, seqno);
            throw e;
        } catch (KafkaException e) {
            onCommitCompleted(e, seqno);
        }
    }

    /**
     * Starts an offset commit by flushing outstanding messages from the task and then starting
     * the write commit.
     **/
    private void doCommit(Map<TopicPartition, OffsetAndMetadata> offsets, boolean closing, final int seqno) {
        log.info("{} Committing offsets", this);
        if (closing) {
            doCommitSync(offsets, seqno);
        } else {
            OffsetCommitCallback cb = new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception error) {
                    if (error == null) {
                        lastCommittedOffsets = offsets;
                    }
                    onCommitCompleted(error, seqno);
                }
            };
            consumer.commitAsync(offsets, cb);
        }
    }

    private void commitOffsets(long now, boolean closing) {
        if (currentOffsets.isEmpty())
            return;

        committing = true;
        commitSeqno += 1;
        commitStarted = now;

        final Map<TopicPartition, OffsetAndMetadata> taskProvidedOffsets;
        try {
            taskProvidedOffsets = task.preCommit(new HashMap<>(currentOffsets));
        } catch (Throwable t) {
            if (closing) {
                log.warn("{} Offset commit failed during close");
                onCommitCompleted(t, commitSeqno);
            } else {
                log.error("{} Offset commit failed, rewinding to last committed offsets", this, t);
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : lastCommittedOffsets.entrySet()) {
                    log.debug("{} Rewinding topic partition {} to offset {}", id, entry.getKey(), entry.getValue().offset());
                    consumer.seek(entry.getKey(), entry.getValue().offset());
                }
                currentOffsets = new HashMap<>(lastCommittedOffsets);
                onCommitCompleted(t, commitSeqno);
            }
            return;
        } finally {
            // Close the task if needed before committing the offsets.
            if (closing)
                task.close(currentOffsets.keySet());
        }

        if (taskProvidedOffsets.isEmpty()) {
            log.debug("{} Skipping offset commit, task opted-out", this);
            onCommitCompleted(null, commitSeqno);
            return;
        }

        final Map<TopicPartition, OffsetAndMetadata> commitableOffsets = new HashMap<>(lastCommittedOffsets);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> taskProvidedOffsetEntry : taskProvidedOffsets.entrySet()) {
            final TopicPartition partition = taskProvidedOffsetEntry.getKey();
            final OffsetAndMetadata taskProvidedOffset = taskProvidedOffsetEntry.getValue();
            if (commitableOffsets.containsKey(partition)) {
                if (taskProvidedOffset.offset() <= currentOffsets.get(partition).offset()) {
                    commitableOffsets.put(partition, taskProvidedOffset);
                } else {
                    log.warn("Ignoring invalid task provided offset {}/{} -- not yet consumed", partition, taskProvidedOffset);
                }
            } else {
                log.warn("Ignoring invalid task provided offset {}/{} -- partition not assigned", partition, taskProvidedOffset);
            }
        }

        if (commitableOffsets.equals(lastCommittedOffsets)) {
            log.debug("{} Skipping offset commit, no change since last commit", this);
            onCommitCompleted(null, commitSeqno);
            return;
        }

        log.trace("{} Offsets to commit: {}", this, commitableOffsets);
        doCommit(commitableOffsets, closing, commitSeqno);
    }


    @Override
    public String toString() {
        return "WorkerSinkTask{" +
                "id=" + id +
                '}';
    }

    private ConsumerRecords<byte[], byte[]> pollConsumer(long timeoutMs) {
        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(timeoutMs);

        // Exceptions raised from the task during a rebalance should be rethrown to stop the worker
        if (rebalanceException != null) {
            RuntimeException e = rebalanceException;
            rebalanceException = null;
            throw e;
        }

        return msgs;
    }

    private KafkaConsumer<byte[], byte[]> createConsumer() {
        // Include any unknown worker configs so consumer configs can be set globally on the worker
        // and through to the task
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, SinkUtils.consumerGroupId(id.connector()));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utils.join(workerConfig.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        props.putAll(workerConfig.originalsWithPrefix("consumer."));

        KafkaConsumer<byte[], byte[]> newConsumer;
        try {
            newConsumer = new KafkaConsumer<>(props);
        } catch (Throwable t) {
            throw new ConnectException("Failed to create consumer", t);
        }

        return newConsumer;
    }

    private void convertMessages(ConsumerRecords<byte[], byte[]> msgs) {
        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
            log.trace("Consuming message with key {}, value {}", msg.key(), msg.value());
            SchemaAndValue keyAndSchema = keyConverter.toConnectData(msg.topic(), msg.key());
            SchemaAndValue valueAndSchema = valueConverter.toConnectData(msg.topic(), msg.value());
            SinkRecord record = new SinkRecord(msg.topic(), msg.partition(),
                    keyAndSchema.schema(), keyAndSchema.value(),
                    valueAndSchema.schema(), valueAndSchema.value(),
                    msg.offset(),
                    (msg.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) ? null : msg.timestamp(),
                    msg.timestampType());
            record = transformationChain.apply(record);
            if (record != null) {
                messageBatch.add(record);
            }
        }
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
            task.put(new ArrayList<>(messageBatch));
            for (SinkRecord record : messageBatch)
                currentOffsets.put(new TopicPartition(record.topic(), record.kafkaPartition()),
                        new OffsetAndMetadata(record.kafkaOffset() + 1));
            messageBatch.clear();
            // If we had paused all consumer topic partitions to try to redeliver data, then we should resume any that
            // the task had not explicitly paused
            if (pausedForRedelivery) {
                if (!shouldPause())
                    resumeAll();
                pausedForRedelivery = false;
            }
        } catch (RetriableException e) {
            log.error("RetriableException from SinkTask {}:", id, e);
            // If we're retrying a previous batch, make sure we've paused all topic partitions so we don't get new data,
            // but will still be able to poll in order to handle user-requested timeouts, keep group membership, etc.
            pausedForRedelivery = true;
            pauseAll();
            // Let this exit normally, the batch will be reprocessed on the next loop.
        } catch (Throwable t) {
            log.error("Task {} threw an uncaught and unrecoverable exception", id, t);
            log.error("Task is being killed and will not recover until manually restarted");
            throw new ConnectException("Exiting WorkerSinkTask due to unrecoverable exception.");
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
                log.trace("Rewind {} to offset {}.", tp, offset);
                consumer.seek(tp, offset);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(offset));
                currentOffsets.put(tp, new OffsetAndMetadata(offset));
            } else {
                log.warn("Cannot rewind {} to null offset.", tp);
            }
        }
        context.clearOffsets();
    }

    private void openPartitions(Collection<TopicPartition> partitions) {
        task.open(partitions);
    }

    private void closePartitions() {
        commitOffsets(time.milliseconds(), true);
    }

    private class HandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            lastCommittedOffsets = new HashMap<>();
            currentOffsets = new HashMap<>();
            for (TopicPartition tp : partitions) {
                long pos = consumer.position(tp);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(pos));
                currentOffsets.put(tp, new OffsetAndMetadata(pos));
                log.debug("{} assigned topic partition {} with offset {}", id, tp, pos);
            }

            // If we paused everything for redelivery (which is no longer relevant since we discarded the data), make
            // sure anything we paused that the task didn't request to be paused *and* which we still own is resumed.
            // Also make sure our tracking of paused partitions is updated to remove any partitions we no longer own.
            pausedForRedelivery = false;

            // Ensure that the paused partitions contains only assigned partitions and repause as necessary
            context.pausedPartitions().retainAll(partitions);
            if (shouldPause())
                pauseAll();
            else if (!context.pausedPartitions().isEmpty())
                consumer.pause(context.pausedPartitions());

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
            try {
                closePartitions();
            } catch (RuntimeException e) {
                // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                // exceptions and rethrow when poll() returns.
                rebalanceException = e;
            }

            // Make sure we don't have any leftover data since offsets will be reset to committed positions
            messageBatch.clear();
        }
    }
}
