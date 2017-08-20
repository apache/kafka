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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTask implements Task {
    private static final Logger log = LoggerFactory.getLogger(AbstractTask.class);

    final TaskId id;
    final String applicationId;
    final ProcessorTopology topology;
    final ProcessorStateManager stateMgr;
    final Set<TopicPartition> partitions;
    final Consumer consumer;
    final String logPrefix;
    final boolean eosEnabled;

    InternalProcessorContext processorContext;

    /**
     * @throws ProcessorStateException if the state manager cannot be created
     */
    AbstractTask(final TaskId id,
                 final String applicationId,
                 final Collection<TopicPartition> partitions,
                 final ProcessorTopology topology,
                 final Consumer<byte[], byte[]> consumer,
                 final ChangelogReader changelogReader,
                 final boolean isStandby,
                 final StateDirectory stateDirectory,
                 final StreamsConfig config) {
        this.id = id;
        this.applicationId = applicationId;
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;
        this.eosEnabled = StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));

        logPrefix = String.format("%s [%s]", isStandby ? "standby-task" : "task", id());

        // create the processor state manager
        try {
            stateMgr = new ProcessorStateManager(
                id,
                partitions,
                isStandby,
                stateDirectory,
                topology.storeToChangelogTopic(),
                changelogReader,
                eosEnabled);
        } catch (final IOException e) {
            throw new ProcessorStateException(String.format("%s Error while creating the state manager", logPrefix), e);
        }
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public final String applicationId() {
        return applicationId;
    }

    @Override
    public final Set<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public final ProcessorTopology topology() {
        return topology;
    }

    @Override
    public final ProcessorContext context() {
        return processorContext;
    }

    @Override
    public StateStore getStore(final String name) {
        return stateMgr.getStore(name);
    }

    /**
     * Produces a string representation containing useful information about a StreamTask.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamTask instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a StreamTask starting with the given indent.
     * This is useful in debugging scenarios.
     * @return A string representation of the StreamTask instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indent);
        sb.append("StreamsTask taskId: ");
        sb.append(id);
        sb.append("\n");

        // print topology
        if (topology != null) {
            sb.append(indent).append(topology.toString(indent + "\t"));
        }

        // print assigned partitions
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(indent).append("Partitions [");
            for (final TopicPartition topicPartition : partitions) {
                sb.append(topicPartition.toString()).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]\n");
        }
        return sb.toString();
    }

    protected Map<TopicPartition, Long> recordCollectorOffsets() {
        return Collections.emptyMap();
    }

    protected void updateOffsetLimits() {
        for (final TopicPartition partition : partitions) {
            try {
                final OffsetAndMetadata metadata = consumer.committed(partition); // TODO: batch API?
                final long offset = metadata != null ? metadata.offset() : 0L;
                stateMgr.putOffsetLimit(partition, offset);

                if (log.isTraceEnabled()) {
                    log.trace("{} Updating store offset limits {} for changelog {}", logPrefix, offset, partition);
                }
            } catch (final AuthorizationException e) {
                throw new ProcessorStateException(String.format("task [%s] AuthorizationException when initializing offsets for %s", id, partition), e);
            } catch (final WakeupException e) {
                throw e;
            } catch (final KafkaException e) {
                throw new ProcessorStateException(String.format("task [%s] Failed to initialize offsets for %s", id, partition), e);
            }
        }
    }

    /**
     * Flush all state stores owned by this task
     */
    void flushState() {
        stateMgr.flush();
    }

    void initializeStateStores() {
        log.trace("{} Initializing state stores", logPrefix);

        // set initial offset limits
        updateOffsetLimits();

        for (final StateStore store : topology.stateStores()) {
            log.trace("{} Initializing store {}", logPrefix, store.name());
            store.init(processorContext, store);
        }
    }

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     * @param writeCheckpoint boolean indicating if a checkpoint file should be written
     */
    void closeStateManager(final boolean writeCheckpoint) throws ProcessorStateException {
        log.trace("{} Closing state manager", logPrefix);
        stateMgr.close(writeCheckpoint ? recordCollectorOffsets() : null);
    }

}
