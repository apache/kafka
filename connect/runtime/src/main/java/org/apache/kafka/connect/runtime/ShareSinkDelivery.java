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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

// Complete on the plugin lane; acknowledge its receipt on the consumer owner.
final class ShareSinkDelivery {
    private final List<ConsumerRecord<byte[], byte[]>> originals;
    private final List<SinkRecord> prepared;
    private final Set<TopicPartition> partitions;
    private CompletedDelivery completed;

    ShareSinkDelivery(ConsumerRecords<byte[], byte[]> records,
                      Function<ConsumerRecord<byte[], byte[]>, ConversionResult> converter) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(converter, "converter");
        List<ConsumerRecord<byte[], byte[]>> originals = new ArrayList<>(records.count());
        List<SinkRecord> prepared = new ArrayList<>(records.count());
        Set<TopicPartition> partitions = new LinkedHashSet<>();
        for (TopicPartition partition : records.partitions()) {
            if (!records.records(partition).isEmpty()) {
                partitions.add(partition);
            }
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
            originals.add(record);
            ConversionResult result = converter.apply(record);
            if (result == null) {
                throw new ConnectException("Share sink conversion returned no classified outcome");
            }
            if (result.failure != null) {
                throw result.failure;
            }
            if (result.record != null) {
                prepared.add(result.record);
            }
        }
        this.originals = List.copyOf(originals);
        this.prepared = List.copyOf(prepared);
        this.partitions = Set.copyOf(partitions);
    }

    CompletedDelivery complete(ShareSinkTask task) {
        if (completed == null) {
            Objects.requireNonNull(task, "task");
            if (!prepared.isEmpty()) {
                task.put(new ArrayList<>(prepared));
            }
            completed = new CompletedDelivery(originals, partitions);
        }
        return completed;
    }

    static final class ConversionResult {
        private static final ConversionResult FILTERED = new ConversionResult(null, null);
        private final SinkRecord record;
        private final RuntimeException failure;

        private ConversionResult(SinkRecord record, RuntimeException failure) {
            this.record = record;
            this.failure = failure;
        }

        static ConversionResult deliver(SinkRecord record) {
            return new ConversionResult(Objects.requireNonNull(record, "record"), null);
        }

        static ConversionResult filtered() {
            return FILTERED;
        }

        static ConversionResult failed(RuntimeException failure) {
            return new ConversionResult(null, Objects.requireNonNull(failure, "failure"));
        }
    }

    static final class CompletedDelivery {
        private final List<ConsumerRecord<byte[], byte[]>> originals;
        private final Set<TopicPartition> partitions;
        private AcknowledgementResult acknowledgementResult;

        private CompletedDelivery(List<ConsumerRecord<byte[], byte[]>> originals, Set<TopicPartition> partitions) {
            this.originals = originals;
            this.partitions = partitions;
        }

        boolean matchesOriginals(List<ConsumerRecord<byte[], byte[]>> expected) {
            if (originals.size() != expected.size()) {
                return false;
            }
            for (int i = 0; i < originals.size(); i++) {
                if (originals.get(i) != expected.get(i)) {
                    return false;
                }
            }
            return true;
        }

        Optional<AcknowledgementResult> acknowledgementResult() {
            return Optional.ofNullable(acknowledgementResult);
        }

        AcknowledgementResult acknowledge(ShareConsumer<byte[], byte[]> consumer, Duration timeout) {
            if (acknowledgementResult != null) {
                return acknowledgementResult;
            }
            Objects.requireNonNull(consumer, "consumer");
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isNegative() || timeout.toMillis() < 0) {
                throw new IllegalArgumentException("Acknowledgement timeout must not be negative");
            }
            return acknowledge(consumer, original -> consumer.acknowledge(original, AcknowledgeType.ACCEPT), () -> timeout);
        }

        AcknowledgementResult acknowledge(ShareConsumer<byte[], byte[]> consumer,
                                          Consumer<ConsumerRecord<byte[], byte[]>> stage,
                                          Supplier<Duration> commitTimeout) {
            if (acknowledgementResult != null) {
                return acknowledgementResult;
            }
            Objects.requireNonNull(consumer, "consumer");
            Objects.requireNonNull(stage, "stage");
            Objects.requireNonNull(commitTimeout, "commitTimeout");
            try {
                if (originals.isEmpty()) {
                    acknowledgementResult = AcknowledgementResult.fromResponses(partitions, Map.of());
                } else {
                    for (ConsumerRecord<byte[], byte[]> original : originals) {
                        stage.accept(original);
                    }
                    acknowledgementResult = AcknowledgementResult.fromResponses(partitions, consumer.commitSync(commitTimeout.get()));
                }
            } catch (RuntimeException failure) {
                acknowledgementResult = AcknowledgementResult.unknown(partitions, failure);
                if (failure instanceof WakeupException || failure instanceof InterruptException
                        || failure instanceof ConcurrentModificationException) {
                    throw failure;
                }
            }
            return acknowledgementResult;
        }
    }

    enum AcknowledgementStatus {
        CONFIRMED,
        REPORTED_ERROR,
        UNKNOWN
    }

    static final class AcknowledgementResult {
        private final Map<TopicPartition, AcknowledgementStatus> statuses;
        private final Map<TopicPartition, RuntimeException> causes;
        private final Map<TopicIdPartition, Optional<KafkaException>> responses;
        private final boolean allConfirmed;

        private AcknowledgementResult(Map<TopicPartition, AcknowledgementStatus> statuses,
                                      Map<TopicPartition, RuntimeException> causes,
                                      Map<TopicIdPartition, Optional<KafkaException>> responses) {
            this.statuses = Map.copyOf(statuses);
            this.causes = Map.copyOf(causes);
            this.responses = Map.copyOf(responses);
            this.allConfirmed = !statuses.containsValue(AcknowledgementStatus.REPORTED_ERROR)
                    && !statuses.containsValue(AcknowledgementStatus.UNKNOWN);
        }

        private static AcknowledgementResult fromResponses(Set<TopicPartition> partitions,
                                                            Map<TopicIdPartition, Optional<KafkaException>> responses) {
            Map<TopicIdPartition, Optional<KafkaException>> snapshot = Map.copyOf(responses);
            Map<TopicPartition, Optional<KafkaException>> byPartition = new HashMap<>(partitions.size());
            Set<TopicPartition> ambiguous = new HashSet<>();
            for (Map.Entry<TopicIdPartition, Optional<KafkaException>> entry : snapshot.entrySet()) {
                TopicPartition partition = entry.getKey().topicPartition();
                if (partitions.contains(partition) && byPartition.putIfAbsent(partition, entry.getValue()) != null) {
                    ambiguous.add(partition);
                }
            }
            Map<TopicPartition, AcknowledgementStatus> statuses = new HashMap<>(partitions.size());
            Map<TopicPartition, RuntimeException> causes = new HashMap<>();
            for (TopicPartition partition : partitions) {
                Optional<KafkaException> error = byPartition.get(partition);
                if (ambiguous.contains(partition)) {
                    statuses.put(partition, AcknowledgementStatus.UNKNOWN);
                    causes.put(partition, new ConnectException("Ambiguous acknowledgement response for " + partition));
                } else if (error == null) {
                    statuses.put(partition, AcknowledgementStatus.UNKNOWN);
                    causes.put(partition, new ConnectException("Missing acknowledgement response for " + partition));
                } else if (error.isPresent()) {
                    statuses.put(partition, AcknowledgementStatus.REPORTED_ERROR);
                    causes.put(partition, error.get());
                } else {
                    statuses.put(partition, AcknowledgementStatus.CONFIRMED);
                }
            }
            return new AcknowledgementResult(statuses, causes, snapshot);
        }

        private static AcknowledgementResult unknown(Set<TopicPartition> partitions, RuntimeException failure) {
            Map<TopicPartition, AcknowledgementStatus> statuses = new HashMap<>(partitions.size());
            Map<TopicPartition, RuntimeException> causes = new HashMap<>(partitions.size());
            for (TopicPartition partition : partitions) {
                statuses.put(partition, AcknowledgementStatus.UNKNOWN);
                causes.put(partition, failure);
            }
            return new AcknowledgementResult(statuses, causes, Map.of());
        }

        Map<TopicPartition, AcknowledgementStatus> statuses() {
            return statuses;
        }

        AcknowledgementResult withExpectedTopicIds(Map<TopicPartition, Uuid> expected) {
            Map<TopicPartition, AcknowledgementStatus> checkedStatuses = new HashMap<>(statuses);
            Map<TopicPartition, RuntimeException> checkedCauses = new HashMap<>(causes);
            expected.forEach((partition, topicId) -> {
                TopicIdPartition identity = new TopicIdPartition(topicId, partition);
                if (!responses.containsKey(identity)) {
                    checkedStatuses.put(partition, AcknowledgementStatus.UNKNOWN);
                    checkedCauses.computeIfAbsent(partition,
                            ignored -> new ConnectException("Share acknowledgement identity was not confirmed for " + partition));
                }
            });
            return new AcknowledgementResult(checkedStatuses, checkedCauses, responses);
        }

        Optional<RuntimeException> cause(TopicPartition partition) {
            return Optional.ofNullable(causes.get(partition));
        }

        Map<TopicIdPartition, Optional<KafkaException>> responses() {
            return responses;
        }

        boolean allConfirmed() {
            return allConfirmed;
        }
    }
}
