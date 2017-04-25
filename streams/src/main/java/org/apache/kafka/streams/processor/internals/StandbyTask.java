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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A StandbyTask
 */
public class StandbyTask extends AbstractTask {

    private static final Logger log = LoggerFactory.getLogger(StandbyTask.class);
    private final Map<TopicPartition, Long> checkpointedOffsets;

    /**
     * Create {@link StandbyTask} with its assigned partitions
     * @param id                    the ID of this task
     * @param applicationId         the ID of the stream processing application
     * @param partitions            the collection of assigned {@link TopicPartition}
     * @param topology              the instance of {@link ProcessorTopology}
     * @param consumer              the instance of {@link Consumer}
     * @param config                the {@link StreamsConfig} specified by the user
     * @param metrics               the {@link StreamsMetrics} created by the thread
     * @param stateDirectory        the {@link StateDirectory} created by the thread
     */
    StandbyTask(final TaskId id,
                final String applicationId,
                final Collection<TopicPartition> partitions,
                final ProcessorTopology topology,
                final Consumer<byte[], byte[]> consumer,
                final ChangelogReader changelogReader,
                final StreamsConfig config,
                final StreamsMetrics metrics,
                final StateDirectory stateDirectory) {
        super(id, applicationId, partitions, topology, consumer, changelogReader, true, stateDirectory, null);

        // initialize the topology with its own context
        processorContext = new StandbyContextImpl(id, applicationId, config, stateMgr, metrics);

        log.info("standby-task [{}] Initializing state stores", id());
        initializeStateStores();

        processorContext.initialized();

        checkpointedOffsets = Collections.unmodifiableMap(stateMgr.checkpointed());
    }

    Map<TopicPartition, Long> checkpointedOffsets() {
        return checkpointedOffsets;
    }

    /**
     * Updates a state store using records from one change log partition
     * @return a list of records not consumed
     */
    public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition, final List<ConsumerRecord<byte[], byte[]>> records) {
        log.debug("standby-task [{}] Updating standby replicas of its state store for partition [{}]", id(), partition);
        return stateMgr.updateStandbyStates(partition, records);
    }

    @Override
    public void commit() {
        log.debug("standby-task [{}] Committing its state", id());
        stateMgr.flush();
        stateMgr.checkpoint(Collections.<TopicPartition, Long>emptyMap());
        // reinitialize offset limits
        initializeOffsetLimits();
    }

    @Override
    public void suspend() {
        commit();
    }

    @Override
    public void close() {
        try {
            commit();
            closeStateManager(true);
        } catch (final RuntimeException e) {
            closeStateManager(false);
            throw e;
        }
    }

}
