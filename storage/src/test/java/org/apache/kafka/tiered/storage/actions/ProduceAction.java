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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorage;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageCondition;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.tiered.storage.specs.OffloadedSegmentSpec;
import org.apache.kafka.tiered.storage.specs.TopicSpec;
import org.apache.kafka.tiered.storage.utils.BrokerLocalStorage;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageCondition.expectEvent;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.COPY_SEGMENT;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.tieredStorageRecords;
import static org.apache.kafka.tiered.storage.utils.RecordsKeyValueMatcher.correspondTo;
import static org.hamcrest.MatcherAssert.assertThat;

public final class ProduceAction implements TieredStorageTestAction {

    /**
     * How much time to wait for all remote log segments of a topic-partition to be offloaded to the second-tier storage.
     * This timeout should exceed the {@link org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils#STORAGE_WAIT_TIMEOUT_SEC}
     * so that the test can verify that the active segment gets rolled and offloaded to the remote storage.
     */
    private static final int OFFLOAD_WAIT_TIMEOUT_SEC = 40;

    private final TopicPartition topicPartition;
    private final List<OffloadedSegmentSpec> offloadedSegmentSpecs;
    private final List<ProducerRecord<String, String>> recordsToProduce;
    private final Integer batchSize;
    private final Long expectedEarliestLocalOffset;
    private final Serde<String> serde = Serdes.String();

    public ProduceAction(TopicPartition topicPartition,
                         List<OffloadedSegmentSpec> offloadedSegmentSpecs,
                         List<ProducerRecord<String, String>> recordsToProduce,
                         Integer batchSize,
                         Long expectedEarliestLocalOffset) {
        this.topicPartition = topicPartition;
        this.offloadedSegmentSpecs = offloadedSegmentSpecs;
        this.recordsToProduce = recordsToProduce;
        this.batchSize = batchSize;
        this.expectedEarliestLocalOffset = expectedEarliestLocalOffset;
    }

    @Override
    public void doExecute(TieredStorageTestContext context)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<LocalTieredStorage> tieredStorages = context.remoteStorageManagers();
        List<BrokerLocalStorage> localStorages = context.localStorages();

        List<LocalTieredStorageCondition> tieredStorageConditions = offloadedSegmentSpecs.stream()
                .map(spec -> expectEvent(
                        tieredStorages,
                        COPY_SEGMENT,
                        spec.getSourceBrokerId(),
                        spec.getTopicPartition(),
                        spec.getBaseOffset(),
                        false))
                .collect(Collectors.toList());

        // Retrieve the offset of the next record which would be consumed from the topic-partition
        // before records are produced. This allows consuming only the newly produced records afterwards.
        long startOffset = context.nextOffset(topicPartition);
        long beginOffset = context.beginOffset(topicPartition);

        // Records are produced here.
        context.produce(recordsToProduce, batchSize);

        if (!tieredStorageConditions.isEmpty()) {
            tieredStorageConditions.stream()
                    .reduce(LocalTieredStorageCondition::and)
                    .get()
                    .waitUntilTrue(OFFLOAD_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
        }

        // At this stage, records were produced, and the expected remote log segments found in the second-tier storage.
        // Further steps are:
        // 1) Verify the local (first-tier) storages contain only the expected log segments - that is to say,
        //    in the special case of these integration tests, only the active segment.
        // 2) Consume the records and verify they match the produced records.
        TopicSpec topicSpec = context.topicSpec(topicPartition.topic());
        long earliestLocalOffset = expectedEarliestLocalOffset != -1L ? expectedEarliestLocalOffset
                : startOffset + recordsToProduce.size()
                - (recordsToProduce.size() % topicSpec.getMaxBatchCountPerSegment()) - 1;

        for (BrokerLocalStorage localStorage : localStorages) {
            // Select brokers which are assigned a replica of the topic-partition
            boolean isAssignedReplica = context.isAssignedReplica(topicPartition, localStorage.getBrokerId());
            if (isAssignedReplica) {
                // Filter out inactive brokers, which may still contain log segments we would expect
                // to be deleted based on the retention configuration.
                boolean isActive = context.isActive(localStorage.getBrokerId());
                if (isActive) {
                    // Wait until the brokers local storage has been cleared from the inactive log segments.
                    localStorage.waitForEarliestLocalOffset(topicPartition, earliestLocalOffset);
                }
            }
        }

        // Verify that the produced records can be consumed from the topic-partition.
        List<ConsumerRecord<String, String>> consumedRecords =
                context.consume(topicPartition, recordsToProduce.size(), startOffset);
        assertThat(consumedRecords, correspondTo(recordsToProduce, topicPartition, serde, serde));

        // Take a physical snapshot of the second-tier storage, and compare the records found with
        // those of the expected log segments.
        List<Record> tieredStorageRecords = tieredStorageRecords(context, topicPartition);
        // Don't include the records which were stored before our records were produced.
        List<Record> discoveredRecords =
                tieredStorageRecords.subList((int) (startOffset - beginOffset), tieredStorageRecords.size());

        List<ProducerRecord<String, String>> producerRecords = offloadedSegmentSpecs.stream()
                .flatMap(spec -> spec.getRecords().stream())
                .collect(Collectors.toList());
        compareRecords(discoveredRecords, producerRecords, topicPartition);
    }

    @Override
    public void describe(PrintStream output) {
        output.println("produce-records: " + topicPartition);
        recordsToProduce.forEach(record -> output.println("    " + record));
        offloadedSegmentSpecs.forEach(spec -> output.println("    " + spec));
    }

    /**
     * Compare the records found in the local tiered storage with the expected records.
     *
     * @param discoveredRecords The records found in the local tiered storage.
     * @param producerRecords   The records expected to be found, based on what was sent by the producer.
     * @param topicPartition    The topic-partition of the records.
     */
    private void compareRecords(List<Record> discoveredRecords,
                                List<ProducerRecord<String, String>> producerRecords,
                                TopicPartition topicPartition) {
        assertThat(discoveredRecords, correspondTo(producerRecords, topicPartition, serde, serde));
    }
}