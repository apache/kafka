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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TaskId;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTask {
    protected final TaskId id;
    protected final String jobId;
    protected final ProcessorTopology topology;
    protected final Consumer consumer;
    protected final ProcessorStateManager stateMgr;
    protected final Set<TopicPartition> partitions;
    protected ProcessorContext processorContext;

    protected AbstractTask(TaskId id,
                           String jobId,
                           Collection<TopicPartition> partitions,
                           ProcessorTopology topology,
                           Consumer<byte[], byte[]> consumer,
                           Consumer<byte[], byte[]> restoreConsumer,
                           StreamsConfig config,
                           boolean isStandby) {
        this.id = id;
        this.jobId = jobId;
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;

        // create the processor state manager
        try {
            File jobStateDir = StreamThread.makeStateDir(jobId, config.getString(StreamsConfig.STATE_DIR_CONFIG));
            File stateFile = new File(jobStateDir.getCanonicalPath(), id.toString());
            // if partitions is null, this is a standby task
            this.stateMgr = new ProcessorStateManager(jobId, id.partition, partitions, stateFile, restoreConsumer, isStandby);
        } catch (IOException e) {
            throw new ProcessorStateException("Error while creating the state manager", e);
        }
    }

    protected void initializeStateStores() {
        for (StateStoreSupplier stateStoreSupplier : this.topology.stateStoreSuppliers()) {
            StateStore store = stateStoreSupplier.get();
            store.init(this.processorContext, store);
        }
    }

    public final TaskId id() {
        return id;
    }

    public final String jobId() {
        return jobId;
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

    public abstract void commit();

    public void close() {
        try {
            stateMgr.close(recordCollectorOffsets());
        } catch (IOException e) {
            throw new ProcessorStateException("Error while closing the state manager", e);
        }
    }

    protected Map<TopicPartition, Long> recordCollectorOffsets() {
        return Collections.emptyMap();
    }

}
