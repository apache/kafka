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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageHistory;
import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.tiered.storage.specs.FetchCountAndOp;
import org.apache.kafka.tiered.storage.specs.RemoteFetchCount;
import org.apache.kafka.tiered.storage.specs.RemoteFetchSpec;

import java.io.PrintStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_OFFSET_INDEX;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_SEGMENT;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_TIME_INDEX;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.FETCH_TRANSACTION_INDEX;
import static org.apache.kafka.tiered.storage.utils.RecordsKeyValueMatcher.correspondTo;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.tieredStorageRecords;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public final class ConsumeAction implements TieredStorageTestAction {

    private final TopicPartition topicPartition;
    private final Long fetchOffset;
    private final Integer expectedTotalCount;
    private final Integer expectedFromSecondTierCount;
    private final RemoteFetchSpec remoteFetchSpec;
    private final Serde<String> serde = Serdes.String();

    public ConsumeAction(TopicPartition topicPartition,
                         Long fetchOffset,
                         Integer expectedTotalCount,
                         Integer expectedFromSecondTierCount,
                         RemoteFetchSpec remoteFetchSpec) {
        this.topicPartition = topicPartition;
        this.fetchOffset = fetchOffset;
        this.expectedTotalCount = expectedTotalCount;
        this.expectedFromSecondTierCount = expectedFromSecondTierCount;
        this.remoteFetchSpec = remoteFetchSpec;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        // Retrieve the history (which stores the chronological sequence of interactions with the second-tier
        // storage) for the expected broker. Note that while the second-tier storage is unique, each broker
        // maintains a local instance of LocalTieredStorage, which is the server-side plug-in interface which
        // allows Kafka to interact with that storage. These instances record the interactions (or events)
        // between the broker which they belong to and the second-tier storage.
        //
        // The latest event at the time of invocation for the interaction of type "FETCH_SEGMENT" between the
        // given broker and the second-tier storage is retrieved. It can be empty if an interaction of this
        // type has yet to happen.
        LocalTieredStorageHistory history = context.tieredStorageHistory(remoteFetchSpec.sourceBrokerId());
        Optional<LocalTieredStorageEvent> latestEventSoFar = history.latestEvent(FETCH_SEGMENT, topicPartition);
        Optional<LocalTieredStorageEvent> latestOffsetIdxEventSoFar = history.latestEvent(FETCH_OFFSET_INDEX, topicPartition);
        Optional<LocalTieredStorageEvent> latestTimeIdxEventSoFar = history.latestEvent(FETCH_TIME_INDEX, topicPartition);
        Optional<LocalTieredStorageEvent> latestTxnIdxEventSoFar = history.latestEvent(FETCH_TRANSACTION_INDEX, topicPartition);

        // Records are consumed here
        List<ConsumerRecord<String, String>> consumedRecords =
                context.consume(topicPartition, expectedTotalCount, fetchOffset);

        // (A) Comparison of records consumed with records in the second-tier storage.
        // Reads all records physically found in the second-tier storage for the given topic-partition.
        // The resulting sequence is sorted by records offset, as there is no guarantee on ordering from
        // the LocalTieredStorageSnapshot.
        List<Record> tieredStorageRecords = tieredStorageRecords(context, topicPartition);

        Optional<Record> firstExpectedRecordOpt = tieredStorageRecords
                .stream()
                .filter(record -> record.offset() >= fetchOffset)
                .findFirst();

        if (firstExpectedRecordOpt.isEmpty()) {
            // If no records could be found in the second-tier storage, no record would be consumed from that storage.
            if (expectedFromSecondTierCount > 0) {
                fail("Could not find any record with offset >= " + fetchOffset + " from tier storage.");
            }
            return;
        }

        int indexOfFetchOffsetInTieredStorage = tieredStorageRecords.indexOf(firstExpectedRecordOpt.get());
        int recordsCountFromFirstIndex = tieredStorageRecords.size() - indexOfFetchOffsetInTieredStorage;

        assertFalse(expectedFromSecondTierCount > recordsCountFromFirstIndex,
                "Not enough records found in tiered storage from offset " + fetchOffset + " for "
                        + topicPartition + ". Expected: " + expectedFromSecondTierCount
                        + ", Was: " + recordsCountFromFirstIndex);

        assertFalse(expectedFromSecondTierCount < recordsCountFromFirstIndex,
                "Too many records found in tiered storage from offset " + fetchOffset + " for "
                        + topicPartition + ". Expected: " + expectedFromSecondTierCount
                        + ", Was: " + recordsCountFromFirstIndex);

        List<Record> storedRecords =
                tieredStorageRecords.subList(indexOfFetchOffsetInTieredStorage, tieredStorageRecords.size());
        List<ConsumerRecord<String, String>> readRecords = consumedRecords.subList(0, expectedFromSecondTierCount);

        assertThat(storedRecords, correspondTo(readRecords, topicPartition, serde, serde));

        // (B) Assessment of the interactions between the source broker and the second-tier storage.
        for (LocalTieredStorageEvent.EventType eventType : List.of(FETCH_SEGMENT, FETCH_OFFSET_INDEX, FETCH_TIME_INDEX, FETCH_TRANSACTION_INDEX)) {
            Optional<LocalTieredStorageEvent> latestEvent = switch (eventType) {
                case FETCH_SEGMENT -> latestEventSoFar;
                case FETCH_OFFSET_INDEX -> latestOffsetIdxEventSoFar;
                case FETCH_TIME_INDEX -> latestTimeIdxEventSoFar;
                case FETCH_TRANSACTION_INDEX -> latestTxnIdxEventSoFar;
                default -> Optional.empty();
            };

            List<LocalTieredStorageEvent> events = history.getEvents(eventType, topicPartition);
            List<LocalTieredStorageEvent> eventsInScope = latestEvent
                    .map(e -> events.stream().filter(event -> event.isAfter(e)).toList())
                    .orElse(events);

            RemoteFetchCount remoteFetchCount = remoteFetchSpec.remoteFetchCount();
            FetchCountAndOp expectedCountAndOp = switch (eventType) {
                case FETCH_SEGMENT -> remoteFetchCount.getSegmentFetchCountAndOp();
                case FETCH_OFFSET_INDEX -> remoteFetchCount.getOffsetIdxFetchCountAndOp();
                case FETCH_TIME_INDEX -> remoteFetchCount.getTimeIdxFetchCountAndOp();
                case FETCH_TRANSACTION_INDEX -> remoteFetchCount.getTxnIdxFetchCountAndOp();
                default -> new FetchCountAndOp(-1, RemoteFetchCount.OperationType.EQUALS_TO);
            };

            RemoteFetchCount.OperationType exceptedOperationType = expectedCountAndOp.operationType();
            int exceptedCount = expectedCountAndOp.count();
            int actualCount = eventsInScope.size();
            String message = errorMessage(eventType, actualCount, exceptedOperationType, exceptedCount);
            if (exceptedCount != -1) {
                if (exceptedOperationType == RemoteFetchCount.OperationType.EQUALS_TO) {
                    assertEquals(exceptedCount, actualCount, message);
                } else if (exceptedOperationType == RemoteFetchCount.OperationType.LESS_THAN_OR_EQUALS_TO) {
                    assertTrue(actualCount <= exceptedCount, message);
                } else {
                    assertTrue(actualCount >= exceptedCount, message);
                }
            }
        }
    }

    private String errorMessage(
        LocalTieredStorageEvent.EventType eventType,
        int actualCount,
        RemoteFetchCount.OperationType exceptedOperationType,
        int exceptedCount
    ) {
        return String.format(
            "Expected %s requests count from broker %d to tiered storage for topic-partition %s to be %s %d, " +
                    "but actual count was %d.",
            eventType,
            remoteFetchSpec.sourceBrokerId(),
            remoteFetchSpec.topicPartition(),
            operationTypeToString(exceptedOperationType),
            exceptedCount,
            actualCount
        );
    }

    private String operationTypeToString(RemoteFetchCount.OperationType operationType) {
        return switch (operationType) {
            case EQUALS_TO -> "equal to";
            case LESS_THAN_OR_EQUALS_TO -> "less than or equal to";
            case GREATER_THAN_OR_EQUALS_TO -> "greater than or equal to";
        };
    }

    @Override
    public void describe(PrintStream output) {
        output.println("consume-action:");
        output.println("  topic-partition = " + topicPartition);
        output.println("  fetch-offset = " + fetchOffset);
        output.println("  expected-record-count = " + expectedTotalCount);
        output.println("  expected-record-from-tiered-storage = " + expectedFromSecondTierCount);
        output.println("  remote-fetch-spec = " + remoteFetchSpec);
    }
}
