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
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.TopologyConfig.TaskConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.GlobalStateManager;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamsProducer;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the multi-partition task graph and record routing for {@link TopologyTestDriver}. It builds
 * one {@link StreamTask} per {@code (subtopologyId, partition)} pair from a
 * {@link MultiPartitionTopologyPlan} and drives processing, punctuation, state-store access and
 * shutdown of those tasks. It collaborates with the driver through the {@link Host} callbacks for
 * the work that remains the driver's responsibility (transactional commit, global-state updates,
 * global-partition lookups and recording output records).
 */
final class MultiPartitionRuntime {

    private static final Logger log = LoggerFactory.getLogger(MultiPartitionRuntime.class);

    private final MultiPartitionTopologyPlan plan;
    private final InternalTopologyBuilder internalTopologyBuilder;
    private final MockConsumer<byte[], byte[]> consumer;
    private final MockProducer<byte[], byte[]> producer;
    private final StreamsProducer testDriverProducer;
    private final GlobalStateManager globalStateManager;
    private final StreamsConfig streamsConfig;
    private final TaskConfig taskConfig;
    private final StreamsMetricsImpl streamsMetrics;
    private final ThreadCache cache;
    private final StateDirectory stateDirectory;
    private final LogContext logContext;
    private final Time wallClockTime;
    private final Host host;

    private final TreeMap<TaskId, StreamTask> tasks = new TreeMap<>();
    private final Map<TopicPartition, TaskId> taskByTopicPartition = new HashMap<>();
    private final Map<String, Map<Integer, Queue<ProducerRecord<byte[], byte[]>>>> outputByTopicPartition = new HashMap<>();
    private final Map<String, Integer> nullKeyRoundRobinByTopic = new HashMap<>();
    private final Map<TopicPartition, AtomicLong> offsets = new HashMap<>();

    interface Host {
        void commit(Map<TopicPartition, OffsetAndMetadata> offsets);
        void processGlobalRecord(TopicPartition partition, long timestamp, byte[] key, byte[] value, Headers headers);
        TopicPartition globalPartitionOrNull(String topic);
        void recordOutput(String topic, ProducerRecord<byte[], byte[]> record);
    }

    MultiPartitionRuntime(final MultiPartitionTopologyPlan plan,
                          final InternalTopologyBuilder internalTopologyBuilder,
                          final MockConsumer<byte[], byte[]> consumer,
                          final MockProducer<byte[], byte[]> producer,
                          final StreamsProducer testDriverProducer,
                          final GlobalStateManager globalStateManager,
                          final StreamsConfig streamsConfig,
                          final TaskConfig taskConfig,
                          final StreamsMetricsImpl streamsMetrics,
                          final ThreadCache cache,
                          final StateDirectory stateDirectory,
                          final LogContext logContext,
                          final Time wallClockTime,
                          final Host host) {
        this.plan = plan;
        this.internalTopologyBuilder = internalTopologyBuilder;
        this.consumer = consumer;
        this.producer = producer;
        this.testDriverProducer = testDriverProducer;
        this.globalStateManager = globalStateManager;
        this.streamsConfig = streamsConfig;
        this.taskConfig = taskConfig;
        this.streamsMetrics = streamsMetrics;
        this.cache = cache;
        this.stateDirectory = stateDirectory;
        this.logContext = logContext;
        this.wallClockTime = wallClockTime;
        this.host = host;
    }

    /**
     * Build one {@link StreamTask} per {@code (subtopologyId, partition)} pair using the structures
     * computed by the plan. All tasks share the driver's single {@link #consumer} and the
     * {@link #testDriverProducer} as their record collector's producer.
     */
    void build() {
        final List<TopicPartition> allSourcePartitions = new ArrayList<>();
        final String threadId = Thread.currentThread().getName();

        for (final int sid : plan.subtopologyIds()) {
            final ProcessorTopology pt = plan.subtopology(sid);
            if (pt.sourceTopics().isEmpty()) {
                continue;
            }
            final int numPartitions = plan.partitionsOfSubtopology(sid);

            // Register an offset counter for every (source-topic, partition) the sub-topology consumes.
            for (final String src : pt.sourceTopics()) {
                final int n = plan.partitionsOfTopic(src);
                for (int p = 0; p < n; p++) {
                    final TopicPartition tp = new TopicPartition(src, p);
                    offsets.putIfAbsent(tp, new AtomicLong());
                    allSourcePartitions.add(tp);
                }
            }

            for (int p = 0; p < numPartitions; p++) {
                // Build a fresh ProcessorTopology per task: ProcessorNode state (sources, processors,
                // store handles) is single-init and would otherwise throw "The processor is not closed"
                // when the second task tries to initialize the same instance.
                final ProcessorTopology freshPt = internalTopologyBuilder.buildSubtopology(sid);
                buildOneTask(sid, p, freshPt, threadId);
            }
        }

        if (!allSourcePartitions.isEmpty()) {
            consumer.assign(allSourcePartitions);
            final Map<TopicPartition, Long> startOffsets = new HashMap<>();
            for (final TopicPartition tp : allSourcePartitions) {
                startOffsets.put(tp, 0L);
            }
            consumer.updateBeginningOffsets(startOffsets);
            consumer.updateEndOffsets(startOffsets);
        }
    }

    private void buildOneTask(final int sid,
                              final int partition,
                              final ProcessorTopology pt,
                              final String threadId) {
        final TaskId taskId = new TaskId(sid, partition);
        TaskMetrics.droppedRecordsSensor(threadId, taskId.toString(), streamsMetrics);

        // This task owns partition {@code p} of each source topic that has at least p+1 partitions.
        final Set<TopicPartition> inputPartitions = new HashSet<>();
        for (final String src : pt.sourceTopics()) {
            final int n = plan.partitionsOfTopic(src);
            if (partition < n) {
                final TopicPartition tp = new TopicPartition(src, partition);
                inputPartitions.add(tp);
                taskByTopicPartition.put(tp, taskId);
            }
        }
        if (inputPartitions.isEmpty()) {
            return;
        }

        final ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Task.TaskType.ACTIVE,
            StreamsConfig.EXACTLY_ONCE_V2.equals(streamsConfig.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
            streamsConfig.getBoolean(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG),
            logContext,
            stateDirectory,
            pt.storeToChangelogTopic(),
            new HashSet<>(inputPartitions));
        final RecordCollector recordCollector = new RecordCollectorImpl(
            logContext,
            taskId,
            testDriverProducer,
            streamsConfig.productionExceptionHandler(),
            streamsMetrics,
            pt
        );
        final InternalProcessorContext<?, ?> context = new ProcessorContextImpl(
            taskId,
            streamsConfig,
            stateManager,
            streamsMetrics,
            cache
        );
        final StreamTask task = new StreamTask(
            taskId,
            new HashSet<>(inputPartitions),
            pt,
            consumer,
            taskConfig,
            streamsMetrics,
            stateDirectory,
            cache,
            wallClockTime,
            stateManager,
            recordCollector,
            context,
            logContext,
            false
        );
        task.initializeIfNeeded();
        task.completeRestoration(noOpResetter -> { });
        task.processorContext().setRecordContext(null);
        tasks.put(taskId, task);
    }

    /**
     * Resolve the partition a record routes to.
     * Explicit partition wins; otherwise {@code Utils.toPositive(Utils.murmur2(keyBytes)) % n} matches
     * {@code BuiltInPartitioner.partitionForKey}; null key or n == 1 routes to partition 0.
     */
    private int resolvePartition(final String topic, final byte[] keyBytes, final int explicit) {
        final int n = Math.max(1, plan.partitionsOfTopic(topic));
        // A negative explicit partition is the "unset" sentinel (TestRecord default): route by key instead.
        if (explicit >= 0) {
            if (explicit >= n) {
                throw new IllegalArgumentException(
                    "Partition " + explicit + " is out of range for topic '" + topic
                        + "' (has " + n + " partitions). Declare a higher count via declareTopic() if needed.");
            }
            return explicit;
        }
        if (n == 1) {
            return 0;
        }
        if (keyBytes == null) {
            // Distribute null-key records round-robin across the topic's partitions.
            final int count = nullKeyRoundRobinByTopic.merge(topic, 1, Integer::sum);
            return (count - 1) % n;
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % n;
    }

    /**
     * Multi-sub-topology pipe path. Routes the record to the task owning the resolved
     * (topic, partition) and drains every task to quiescence before returning.
     */
    void pipeRecord(final String topicName,
                    final long timestamp,
                    final byte[] key,
                    final byte[] value,
                    final Headers headers,
                    final int explicitPartition) {
        final boolean isTaskInput = (plan.subtopologyForInputTopic(topicName) != null);
        final TopicPartition globalPartition = host.globalPartitionOrNull(topicName);
        final boolean isGlobal = globalPartition != null;
        if (!isTaskInput && !isGlobal) {
            throw new IllegalArgumentException("Unknown topic: " + topicName);
        }
        if (isTaskInput) {
            final int partition = resolvePartition(topicName, key, explicitPartition);
            enqueueTaskRecord(topicName, new TopicPartition(topicName, partition),
                timestamp, key, value, headers);
            completeAllProcessableWork();
        }
        if (isGlobal) {
            host.processGlobalRecord(globalPartition, timestamp, key, value, headers);
        }
    }

    private void enqueueTaskRecord(final String topic,
                                   final TopicPartition tp,
                                   final long timestamp,
                                   final byte[] key,
                                   final byte[] value,
                                   final Headers headers) {
        final TaskId taskId = taskByTopicPartition.get(tp);
        if (taskId == null) {
            throw new IllegalStateException(
                "No task owns " + tp + ". This typically means init() was not called or the topic "
                    + "was not declared with enough partitions.");
        }
        final StreamTask owner = tasks.get(taskId);
        if (owner == null) {
            throw new IllegalStateException("Task " + taskId + " is registered but no StreamTask exists for it.");
        }
        final long offset = offsets
            .computeIfAbsent(tp, k -> new AtomicLong())
            .getAndIncrement();
        owner.addRecords(tp, Collections.singleton(new ConsumerRecord<>(
            topic, tp.partition(), offset, timestamp, TimestampType.CREATE_TIME,
            key == null ? ConsumerRecord.NULL_SIZE : key.length,
            value == null ? ConsumerRecord.NULL_SIZE : value.length,
            key, value, headers, Optional.empty())));
    }

    /**
     * Drain every multi-sub-topology task to quiescence, picking the processable task with the lowest
     * current stream time on each iteration to mirror {@code PartitionGroup} ordering across tasks.
     */
    void completeAllProcessableWork() {
        captureOutputs();
        if (tasks.isEmpty()) {
            return;
        }
        StreamTask next;
        while ((next = pickNextProcessableTask()) != null) {
            next.resumePollingForPartitionsWithAvailableSpace();
            next.updateLags();
            next.process(wallClockTime.milliseconds());
            next.maybePunctuateStreamTime();
            host.commit(next.prepareCommit(true));
            next.postCommit(true);
            captureOutputs();
        }
        for (final StreamTask t : tasks.values()) {
            if (t.hasRecordsQueued()) {
                log.info("Multi-sub task {} has records that cannot be processed right now; advance "
                    + "wall-clock time or pipe records on co-partitioned topics (see {}).",
                    t.id(), StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            }
        }
    }

    private StreamTask pickNextProcessableTask() {
        StreamTask best = null;
        long bestTime = Long.MAX_VALUE;
        final long now = wallClockTime.milliseconds();
        for (final StreamTask t : tasks.values()) {
            if (!t.hasRecordsQueued() || !t.isProcessable(now)) {
                continue;
            }
            final long streamTime = ((ProcessorContextImpl) t.processorContext()).currentStreamTimeMs();
            if (streamTime < bestTime) {
                bestTime = streamTime;
                best = t;
            }
        }
        return best;
    }

    /**
     * Capture all records emitted by the shared producer this round, partition them in
     * {@link #outputByTopicPartition} (and hand them to the host for back-compat with the existing
     * read accessors), and loop back into any sub-topology that consumes the topic.
     * Honours an explicit producer partition when set (custom {@link org.apache.kafka.streams.processor.StreamPartitioner}
     * on a sink); otherwise resolves by key.
     */
    private void captureOutputs() {
        final List<ProducerRecord<byte[], byte[]>> output = producer.history();
        producer.clear();
        for (final ProducerRecord<byte[], byte[]> record : output) {
            final String topic = record.topic();
            final Integer producedPartition = record.partition();
            // MockProducer leaves partition() null when the upstream code did not pin one. Resolve it
            // ourselves so the output record reflects the partition the test driver actually routes to.
            final int capturedPartition = producedPartition != null
                ? producedPartition
                : resolvePartition(topic, record.key(), -1);
            final ProducerRecord<byte[], byte[]> stamped = producedPartition != null
                ? record
                : new ProducerRecord<>(topic, capturedPartition, record.timestamp(),
                    record.key(), record.value(), record.headers());

            host.recordOutput(topic, stamped);
            outputByTopicPartition
                .computeIfAbsent(topic, k -> new HashMap<>())
                .computeIfAbsent(capturedPartition, k -> new LinkedList<>())
                .add(stamped);

            if (plan.subtopologyForInputTopic(topic) != null) {
                enqueueTaskRecord(topic, new TopicPartition(topic, capturedPartition),
                    record.timestamp(), record.key(), record.value(), record.headers());
            }
            final TopicPartition globalPartition = host.globalPartitionOrNull(topic);
            if (globalPartition != null) {
                host.processGlobalRecord(globalPartition,
                    record.timestamp(), record.key(), record.value(), record.headers());
            }
        }
    }

    /**
     * Multi-sub-topology lookup. A global store match wins. Otherwise, the no-argument
     * accessors are only valid for a store registered in a sub-topology that has exactly one
     * partition <em>in the declared plan</em> -- not merely "happens to have one task built right
     * now". A store registered in a sub-topology whose declared partition count is &gt; 1 always
     * throws {@link IllegalStateException}, even if only one of its tasks has been instantiated
     * (e.g. because one of its source topics has fewer partitions than the sub-topology's max): no
     * single partition can be inferred, and silently resolving to whichever task happens to exist
     * would be surprising.
     */
    StateStore getStateStore(final String name, final boolean throwForBuiltInStores) {
        if (globalStateManager != null) {
            final StateStore gs = globalStateManager.store(name);
            if (gs != null) {
                if (throwForBuiltInStores) {
                    TopologyTestDriver.throwIfBuiltInStore(gs);
                }
                return gs;
            }
        }
        final Integer sid = subtopologyOwningStore(name);
        if (sid == null) {
            return null;
        }
        final int declaredPartitions = plan.partitionsOfSubtopology(sid);
        if (declaredPartitions > 1) {
            throw new IllegalStateException(
                    "Store '" + name + "' is registered in sub-topology " + sid + ", which is declared "
                            + "with " + declaredPartitions + " partitions; no single partition can be inferred. "
                            + "Use getStateStore(name, partition) to access a specific partition.");
        }
        // declaredPartitions == 1: exactly one task exists for this sub-topology (partition 0).
        final StreamTask only = tasks.get(new TaskId(sid, 0));
        if (only == null) {
            return null;
        }
        only.processorContext().setRecordContext(
                new ProcessorRecordContext(0L, -1L, -1, null, new RecordHeaders()));
        final StateStore stateStore = ((ProcessorContextImpl) only.processorContext()).stateManager().store(name);
        if (throwForBuiltInStores && stateStore != null) {
            TopologyTestDriver.throwIfBuiltInStore(stateStore);
        }
        return stateStore;
    }

    private Integer subtopologyOwningStore(final String name) {
        Integer found = null;
        for (final StreamTask t : tasks.values()) {
            final StateStore s = ((ProcessorContextImpl) t.processorContext()).stateManager().store(name);
            if (s == null) {
                continue;
            }
            final int sid = t.id().subtopology();
            if (found != null && found != sid) {
                throw new IllegalStateException(
                    "Store '" + name + "' is registered in more than one sub-topology ("
                        + found + " and " + sid + ").");
            }
            found = sid;
        }
        return found;
    }

    /**
     * Return the {@link StateStore} for the task owning {@code partition} of the sub-topology that
     * registers a store named {@code name}. If the store name appears in multiple
     * sub-topologies, throws {@link IllegalStateException}.
     *
     * @param name the store name
     * @param partition the partition whose owning task should be queried
     * @return the {@link StateStore}, or {@code null} if no sub-topology registers a store with this name
     */
    StateStore getStateStore(final String name, final int partition) {
        if (globalStateManager != null) {
            final StateStore gs = globalStateManager.store(name);
            if (gs != null) {
                return gs;
            }
        }
        final Integer sid = subtopologyOwningStore(name);
        if (sid == null) {
            return null;
        }
        return getStateStore(name, sid, partition);
    }

    /**
     * Internal fully-qualified {@link StateStore} accessor: resolves a store to the task owning
     * {@code (subtopologyId, partition)}.
     *
     * @param name the store name
     * @param subtopologyId the sub-topology id
     * @param partition the partition whose owning task should be queried
     * @return the {@link StateStore}, or {@code null} if the task does not register a store with this name
     * @throws IllegalArgumentException if no task exists for {@code (subtopologyId, partition)}
     */
    StateStore getStateStore(final String name, final int subtopologyId, final int partition) {
        final TaskId taskId = new TaskId(subtopologyId, partition);
        final StreamTask owner = tasks.get(taskId);
        if (owner == null) {
            throw new IllegalArgumentException(
                "No task exists for " + taskId + " (sub-topology " + subtopologyId + " has "
                    + plan.partitionsOfSubtopology(subtopologyId) + " partition(s)).");
        }
        owner.processorContext().setRecordContext(
            new ProcessorRecordContext(0L, -1L, -1, null, new RecordHeaders()));
        return ((ProcessorContextImpl) owner.processorContext()).stateManager().store(name);
    }

    /**
     * @return the number of partitions of the sub-topology that registers {@code storeName}, or 0
     *         if no sub-topology registers it (or 1 for a global store).
     */
    int partitionsOf(final String storeName) {
        if (globalStateManager != null && globalStateManager.store(storeName) != null) {
            return 1;
        }
        final Integer sid = subtopologyOwningStore(storeName);
        return sid == null ? 0 : plan.partitionsOfSubtopology(sid);
    }

    /**
     * @return the number of partitions of the given sub-topology, or 0 if the id is unknown.
     */
    int partitionsOfSubtopology(final int subtopologyId) {
        return plan.partitionsOfSubtopology(subtopologyId);
    }

    /**
     * @return the list of the sub-topology ids in this runtime.
     */
    List<Integer> subtopologies() {
        return plan.subtopologyIds();
    }

    /**
     * Advance wall-clock time across every multi-sub-topology task, firing system-time punctuators,
     * committing and then draining processable work.
     */
    void advanceWallClockTime() {
        for (final StreamTask t : tasks.values()) {
            t.maybePunctuateSystemTime();
            host.commit(t.prepareCommit(true));
            t.postCommit(true);
        }
        completeAllProcessableWork();
    }

    /**
     * Suspend, commit and close every multi-sub-topology task, swallowing per-task close failures.
     */
    void closeTasks() {
        for (final StreamTask t : tasks.values()) {
            try {
                t.suspend();
                t.prepareCommit(true);
                t.postCommit(true);
                t.closeClean();
            } catch (final RuntimeException e) {
                log.warn("Error closing multi-sub task {}: {}", t.id(), e.toString());
            }
        }
    }
}
