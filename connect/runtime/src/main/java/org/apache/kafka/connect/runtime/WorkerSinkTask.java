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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * WorkerTask that uses a SinkTask to export data from Kafka.
 */
class WorkerSinkTask implements WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSinkTask.class);

    private final ConnectorTaskId id;
    private final SinkTask task;
    private final WorkerConfig workerConfig;
    private final Time time;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private WorkerSinkTaskThread workThread;
    private Map<String, String> taskProps;
    private KafkaConsumer<byte[], byte[]> consumer;
    private WorkerSinkTaskContext context;
    private boolean started;
    private final List<SinkRecord> messageBatch;
    private Map<TopicPartition, OffsetAndMetadata> lastCommittedOffsets;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private boolean pausedForRedelivery;
    private RuntimeException rebalanceException;

    public WorkerSinkTask(ConnectorTaskId id, SinkTask task, WorkerConfig workerConfig,
                          Converter keyConverter, Converter valueConverter, Time time) {
        this.id = id;
        this.task = task;
        this.workerConfig = workerConfig;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.time = time;
        this.started = false;
        this.messageBatch = new ArrayList<>();
        this.currentOffsets = new HashMap<>();
        this.pausedForRedelivery = false;
        this.rebalanceException = null;
    }

    @Override
    public void start(Map<String, String> props) {
        taskProps = props;
        consumer = createConsumer();
        context = new WorkerSinkTaskContext(consumer);

        workThread = createWorkerThread();
        workThread.start();
    }

    @Override
    public void stop() {
        // Offset commit is handled upon exit in work thread
        if (workThread != null)
            workThread.startGracefulShutdown();
        consumer.wakeup();
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
        task.stop();
        return success;
    }

    @Override
    public void close() {
        // FIXME Kafka needs to add a timeout parameter here for us to properly obey the timeout
        // passed in
        if (consumer != null)
            consumer.close();
    }

    /**
     * Performs initial join process for consumer group, ensures we have an assignment, and initializes + starts the
     * SinkTask.
     *
     * @returns true if successful, false if joining the consumer group was interrupted
     */
    public boolean joinConsumerGroupAndStart() {
        String topicsStr = taskProps.get(SinkTask.TOPICS_CONFIG);
        if (topicsStr == null || topicsStr.isEmpty())
            throw new ConnectException("Sink tasks require a list of topics.");
        String[] topics = topicsStr.split(",");
        log.debug("Task {} subscribing to topics {}", id, topics);
        consumer.subscribe(Arrays.asList(topics), new HandleRebalance());

        // Ensure we're in the group so that if start() wants to rewind offsets, it will have an assignment of partitions
        // to work with. Any rewinding will be handled immediately when polling starts.
        try {
            pollConsumer(0);
        } catch (WakeupException e) {
            log.error("Sink task {} was stopped before completing join group. Task initialization and start is being skipped", this);
            return false;
        }
        task.initialize(context);
        task.start(taskProps);
        log.info("Sink task {} finished initialization and start", this);
        started = true;
        return true;
    }

    /** Poll for new messages with the given timeout. Should only be invoked by the worker thread. */
    public void poll(long timeoutMs) {
        try {
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
        } catch (WakeupException we) {
            log.trace("{} consumer woken up", id);
        }
    }

    /**
     * Starts an offset commit by flushing outstanding messages from the task and then starting
     * the write commit. This should only be invoked by the WorkerSinkTaskThread.
     **/
    public void commitOffsets(boolean sync, final int seqno) {
        log.info("{} Committing offsets", this);

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(currentOffsets);

        try {
            task.flush(offsets);
        } catch (Throwable t) {
            log.error("Commit of {} offsets failed due to exception while flushing:", this, t);
            log.error("Rewinding offsets to last committed offsets");
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : lastCommittedOffsets.entrySet()) {
                log.debug("{} Rewinding topic partition {} to offset {}", id, entry.getKey(), entry.getValue().offset());
                consumer.seek(entry.getKey(), entry.getValue().offset());
            }
            currentOffsets = new HashMap<>(lastCommittedOffsets);
            workThread.onCommitCompleted(t, seqno);
            return;
        }

        if (sync) {
            try {
                consumer.commitSync(offsets);
                lastCommittedOffsets = offsets;
                workThread.onCommitCompleted(null, seqno);
            } catch (KafkaException e) {
                workThread.onCommitCompleted(e, seqno);
            }
        } else {
            OffsetCommitCallback cb = new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception error) {
                    lastCommittedOffsets = offsets;
                    workThread.onCommitCompleted(error, seqno);
                }
            };
            consumer.commitAsync(offsets, cb);
        }
    }

    public Time time() {
        return time;
    }

    public WorkerConfig workerConfig() {
        return workerConfig;
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

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "connect-" + id.connector());
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

    private WorkerSinkTaskThread createWorkerThread() {
        return new WorkerSinkTaskThread(this, "WorkerSinkTask-" + id, time, workerConfig);
    }

    private void convertMessages(ConsumerRecords<byte[], byte[]> msgs) {
        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
            log.trace("Consuming message with key {}, value {}", msg.key(), msg.value());
            SchemaAndValue keyAndSchema = keyConverter.toConnectData(msg.topic(), msg.key());
            SchemaAndValue valueAndSchema = valueConverter.toConnectData(msg.topic(), msg.value());
            messageBatch.add(
                    new SinkRecord(msg.topic(), msg.partition(),
                            keyAndSchema.schema(), keyAndSchema.value(),
                            valueAndSchema.schema(), valueAndSchema.value(),
                            msg.offset())
            );
        }
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
                for (TopicPartition tp : consumer.assignment())
                    if (!context.pausedPartitions().contains(tp))
                        consumer.resume(tp);
                pausedForRedelivery = false;
            }
        } catch (RetriableException e) {
            log.error("RetriableException from SinkTask {}:", id, e);
            // If we're retrying a previous batch, make sure we've paused all topic partitions so we don't get new data,
            // but will still be able to poll in order to handle user-requested timeouts, keep group membership, etc.
            pausedForRedelivery = true;
            for (TopicPartition tp : consumer.assignment())
                consumer.pause(tp);
            // Let this exit normally, the batch will be reprocessed on the next loop.
        } catch (Throwable t) {
            log.error("Task {} threw an uncaught and unrecoverable exception", id);
            log.error("Task is being killed and will not recover until manually restarted:", t);
            throw new ConnectException("Exiting WorkerSinkTask due to unrecoverable exception.");
        }
    }

    private void rewind() {
        Map<TopicPartition, Long> offsets = context.offsets();
        if (offsets.isEmpty()) {
            return;
        }
        for (TopicPartition tp: offsets.keySet()) {
            Long offset = offsets.get(tp);
            if (offset != null) {
                log.trace("Rewind {} to offset {}.", tp, offset);
                consumer.seek(tp, offset);
                lastCommittedOffsets.put(tp, new OffsetAndMetadata(offset));
                currentOffsets.put(tp, new OffsetAndMetadata(offset));
            }
        }
        context.clearOffsets();
    }

    protected void abort(Throwable t) {
        // Shutdown the task now and close resources. This should only be called from the worker's task thread
        // TODO: Report the exception to the Worker so that it can be propagated or the task can be restarted

        log.error("{} stopping due to unexpected exception", id, t);
        try {
            task.stop();
        } finally {
            close();
        }
    }

    private class HandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (rebalanceException != null)
                return;

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
            if (pausedForRedelivery) {
                pausedForRedelivery = false;
                Set<TopicPartition> assigned = new HashSet<>(partitions);
                Set<TopicPartition> taskPaused = context.pausedPartitions();

                for (TopicPartition tp : partitions) {
                    if (!taskPaused.contains(tp))
                        consumer.resume(tp);
                }

                Iterator<TopicPartition> tpIter = taskPaused.iterator();
                while (tpIter.hasNext()) {
                    TopicPartition tp = tpIter.next();
                    if (assigned.contains(tp))
                        tpIter.remove();
                }
            }

            // Instead of invoking the assignment callback on initialization, we guarantee the consumer is ready upon
            // task start. Since this callback gets invoked during that initial setup before we've started the task, we
            // need to guard against invoking the user's callback method during that period.
            if (started) {
                try {
                    task.onPartitionsAssigned(partitions);
                } catch (RuntimeException e) {
                    // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                    // exceptions and rethrow when poll() returns.
                    rebalanceException = e;
                }
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (started) {
                try {
                    task.onPartitionsRevoked(partitions);
                    commitOffsets(true, -1);
                } catch (RuntimeException e) {
                    // The consumer swallows exceptions raised in the rebalance listener, so we need to store
                    // exceptions and rethrow when poll() returns.
                    rebalanceException = e;
                }
            }

            // Make sure we don't have any leftover data since offsets will be reset to committed positions
            messageBatch.clear();
        }
    }
}
