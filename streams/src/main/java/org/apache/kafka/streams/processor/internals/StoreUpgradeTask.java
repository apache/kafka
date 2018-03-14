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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.RecordConverter;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Collection;
import java.util.List;

class StoreUpgradeTask extends StandbyTask {
    private final RecordCollector recordCollector;
    private RecordConverter recordConverter;

    /**
     * Create {@link StoreUpgradeTask} with its assigned partitions
     *
     * @param id             the ID of this task
     * @param partitions     the collection of assigned {@link TopicPartition}
     * @param topology       the instance of {@link ProcessorTopology}
     * @param consumer       the instance of {@link Consumer}
     * @param config         the {@link StreamsConfig} specified by the user
     * @param metrics        the {@link StreamsMetrics} created by the thread
     * @param stateDirectory the {@link StateDirectory} created by the thread
     */
    StoreUpgradeTask(final TaskId id,
                     final Collection<TopicPartition> partitions,
                     final ProcessorTopology topology,
                     final Consumer<byte[], byte[]> consumer,
                     final ChangelogReader changelogReader,
                     final StreamsConfig config,
                     final StreamsMetricsImpl metrics,
                     final StateDirectory stateDirectory,
                     final Producer<byte[], byte[]> producer) {
        super(id, partitions, topology, consumer, changelogReader, true, config, metrics, stateDirectory);

        this.recordCollector = new RecordCollectorImpl(
            producer,
            id.toString(),
            logContext,
            config.defaultProductionExceptionHandler(), // TODO: check if we should always use Fail-Handler for upgrading?
            metrics.skippedRecordsSensor());

        processorContext = new UpgradeContextImpl(id, config, this.recordCollector, stateMgr, metrics, null);
    }

    @Override
    protected void flushState() {
        log.trace("Flushing state and producer");
        super.flushState();
        try {
            recordCollector.flush();
        } catch (final ProducerFencedException fatal) {
            throw new TaskMigratedException(this, fatal);
        }
    }

    @Override
    public void closeSuspended(final boolean clean,
                               final boolean isZombie,
                               final RuntimeException firstException) {
        super.closeSuspended(clean, isZombie, firstException);

        try {
            if (!isZombie) {
                recordCollector.close();
            }
        } catch (final Throwable e) {
            log.error("Failed to close producer due to the following error:", e);
        }
    }

    /**
     * Updates a state store using records from one change log partition
     *
     * @return a list of records not consumed
     */
    @Override
    public List<ConsumerRecord<byte[], byte[]>> update(final TopicPartition partition,
                                                       final List<ConsumerRecord<byte[], byte[]>> records) {
        recordConverter = stateMgr.recordConverters.get(partition.topic());
        return super.update(partition, records);
    }

    KeyValue<byte[], byte[]> mkKeyValuePair(final ConsumerRecord<byte[], byte[]> record) {
        return recordConverter.convert(record);
    }

}