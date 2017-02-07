/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTask {
    private static final Logger log = LoggerFactory.getLogger(AbstractTask.class);

    protected final TaskId id;
    protected final String applicationId;
    protected final ProcessorTopology topology;
    protected final Consumer consumer;
    protected final ProcessorStateManager stateMgr;
    protected final Set<TopicPartition> partitions;
    protected InternalProcessorContext processorContext;
    protected final ThreadCache cache;
    /**
     * @throws ProcessorStateException if the state manager cannot be created
     */
    protected AbstractTask(TaskId id,
                           String applicationId,
                           Collection<TopicPartition> partitions,
                           ProcessorTopology topology,
                           Consumer<byte[], byte[]> consumer,
                           Consumer<byte[], byte[]> restoreConsumer,
                           boolean isStandby,
                           StateDirectory stateDirectory,
                           final ThreadCache cache) {
        this.id = id;
        this.applicationId = applicationId;
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;
        this.cache = cache;

        // create the processor state manager
        try {
            this.stateMgr = new ProcessorStateManager(id, partitions, restoreConsumer, isStandby, stateDirectory, topology.storeToChangelogTopic());
        } catch (IOException e) {
            throw new ProcessorStateException(String.format("task [%s] Error while creating the state manager", id), e);
        }
    }

    protected void initializeStateStores() {
        // set initial offset limits
        initializeOffsetLimits();

        for (StateStore store : this.topology.stateStores()) {
            log.trace("task [{}] Initializing store {}", id(), store.name());
            store.init(this.processorContext, store);
        }

    }

    public final TaskId id() {
        return id;
    }

    public final String applicationId() {
        return applicationId;
    }

    public final Set<TopicPartition> partitions() {
        return this.partitions;
    }

    public final ProcessorTopology topology() {
        return topology;
    }

    public final ProcessorContext context() {
        return processorContext;
    }

    public final ThreadCache cache() {
        return cache;
    }

    public abstract void commit();

    public abstract void close();

    public abstract void initTopology();
    public abstract void closeTopology();

    public abstract void commitOffsets();

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     * @param writeCheckpoint boolean indicating if a checkpoint file should be written
     */
    void closeStateManager(final boolean writeCheckpoint) {
        log.trace("task [{}] Closing", id());
        try {
            stateMgr.close(writeCheckpoint ? recordCollectorOffsets() : null);
        } catch (IOException e) {
            throw new ProcessorStateException("Error while closing the state manager", e);
        }
    }

    protected Map<TopicPartition, Long> recordCollectorOffsets() {
        return Collections.emptyMap();
    }

    protected void initializeOffsetLimits() {
        for (TopicPartition partition : partitions) {
            try {
                OffsetAndMetadata metadata = consumer.committed(partition); // TODO: batch API?
                stateMgr.putOffsetLimit(partition, metadata != null ? metadata.offset() : 0L);
            } catch (AuthorizationException e) {
                throw new ProcessorStateException(String.format("task [%s] AuthorizationException when initializing offsets for %s", id, partition), e);
            } catch (WakeupException e) {
                throw e;
            } catch (KafkaException e) {
                throw new ProcessorStateException(String.format("task [%s] Failed to initialize offsets for %s", id, partition), e);
            }
        }
    }

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
        final StringBuilder sb = new StringBuilder(indent + "StreamsTask taskId: " + this.id() + "\n");

        // print topology
        if (topology != null) {
            sb.append(indent).append(topology.toString(indent + "\t"));
        }

        // print assigned partitions
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(indent).append("Partitions [");
            for (TopicPartition topicPartition : partitions) {
                sb.append(topicPartition.toString()).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]\n");
        }
        return sb.toString();
    }

    /**
     * Flush all state stores owned by this task
     */
    public void flushState() {
        stateMgr.flush((InternalProcessorContext) this.context());
    }
}
