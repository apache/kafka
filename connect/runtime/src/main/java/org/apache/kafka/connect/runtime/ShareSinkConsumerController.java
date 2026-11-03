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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

final class ShareSinkConsumerController implements Runnable {
    private final Supplier<ShareConsumer<byte[], byte[]>> consumerFactory;
    private final List<String> topics;
    private final long incarnation;
    private final Settings settings;
    private final Time time;
    private final Object monitor = new Object();
    private final CountDownLatch terminated = new CountDownLatch(1);
    private volatile ShareConsumer<byte[], byte[]> consumer;
    private volatile boolean stopRequested;
    private boolean started;
    private boolean signalled;
    private boolean cleanupComplete;
    private RuntimeException failure;
    private ActiveUnit active;
    private Unit handoff;
    private Result result;
    private ShareSinkDelivery.AcknowledgementResult lastAcknowledgement;
    private long sequence;

    ShareSinkConsumerController(Supplier<ShareConsumer<byte[], byte[]>> consumerFactory,
                                List<String> topics,
                                long incarnation,
                                Settings settings,
                                Time time) {
        this.consumerFactory = Objects.requireNonNull(consumerFactory);
        this.topics = List.copyOf(topics);
        if (topics.isEmpty() || incarnation <= 0) {
            throw new IllegalArgumentException("Topics and a positive task incarnation are required");
        }
        this.incarnation = incarnation;
        this.settings = Objects.requireNonNull(settings);
        this.time = Objects.requireNonNull(time);
    }

    // The worker invokes run only after serial plugin initialization succeeds.
    @Override
    public void run() {
        synchronized (monitor) {
            if (started) {
                throw new IllegalStateException("Share consumer controller is already started");
            }
            started = true;
        }
        try {
            if (stopRequested) {
                return;
            }
            consumer = Objects.requireNonNull(consumerFactory.get());
            consumer.subscribe(topics);
            while (!stopRequested) {
                if (active == null) {
                    acquireUnit();
                } else {
                    progressUnit();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(new ConnectException("Share consumer controller was interrupted", e));
        } catch (RuntimeException e) {
            if (!stopRequested) {
                fail(e);
            }
        } catch (Error e) {
            fail(new ConnectException("Share consumer controller failed", e));
            throw e;
        } finally {
            finish();
        }
    }

    Unit awaitUnit() throws InterruptedException {
        synchronized (monitor) {
            while (true) {
                if (failure != null) {
                    throw new ConnectException("Share consumer controller failed", failure);
                }
                if (stopRequested || terminated.getCount() == 0) {
                    return null;
                }
                if (handoff != null) {
                    Unit unit = handoff;
                    handoff = null;
                    return unit;
                }
                monitor.wait();
            }
        }
    }

    boolean publishResult(Unit unit, ShareSinkDelivery.CompletedDelivery completed) {
        Objects.requireNonNull(completed);
        synchronized (monitor) {
            if (!acceptsResult(unit) || !completed.matchesOriginals(unit.originals)) {
                return false;
            }
            if (result != null) {
                return result.completed == completed;
            }
            result = new Result(completed, null);
            monitor.notifyAll();
            return true;
        }
    }

    boolean publishFailure(Unit unit, RuntimeException error) {
        Objects.requireNonNull(error);
        synchronized (monitor) {
            if (!acceptsResult(unit) || result != null) {
                return false;
            }
            result = new Result(null, error);
            monitor.notifyAll();
            return true;
        }
    }

    private boolean acceptsResult(Unit unit) {
        return !stopRequested && failure == null && active != null && active.unit == unit && handoff == null;
    }

    void requestStop() {
        ShareConsumer<byte[], byte[]> current;
        synchronized (monitor) {
            stopRequested = true;
            current = consumer;
            monitor.notifyAll();
        }
        if (current != null) {
            current.wakeup();
        }
    }

    void signal() {
        synchronized (monitor) {
            signalled = true;
            monitor.notifyAll();
        }
    }

    boolean awaitStopped(Duration timeout) throws InterruptedException {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("Stop timeout must not be negative");
        }
        return terminated.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    boolean cleanupComplete() {
        synchronized (monitor) {
            return cleanupComplete;
        }
    }

    Optional<RuntimeException> failure() {
        synchronized (monitor) {
            return Optional.ofNullable(failure);
        }
    }

    Optional<ShareSinkDelivery.AcknowledgementResult> lastAcknowledgement() {
        synchronized (monitor) {
            return Optional.ofNullable(lastAcknowledgement);
        }
    }

    private void acquireUnit() {
        long beforePoll = now();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(settings.pollTimeoutMs));
        if (records.isEmpty() || stopRequested) {
            return;
        }
        Unit unit = new Unit(new AttemptId(incarnation, ++sequence, 1), records, beforePoll, settings);
        ActiveUnit acquired = new ActiveUnit(unit, lockDuration(), beforePoll);
        synchronized (monitor) {
            active = acquired;
        }
        renew(acquired);
        synchronized (monitor) {
            if (!stopRequested) {
                handoff = unit;
                monitor.notifyAll();
            }
        }
    }

    private void progressUnit() throws InterruptedException {
        ActiveUnit current = active;
        Result completed;
        synchronized (monitor) {
            completed = result;
        }
        requireBudget(current);
        if (completed != null) {
            if (completed.failure != null) {
                throw completed.failure;
            }
            acknowledge(current, completed.completed);
        } else if (renewalDelay(current) <= 0) {
            renew(current);
        } else {
            synchronized (monitor) {
                if (result == null && !stopRequested && !signalled) {
                    monitor.wait(Math.max(1, renewalDelay(current)));
                }
                signalled = false;
            }
        }
    }

    private void renew(ActiveUnit current) {
        requireBudget(current);
        long beforeRenew = now();
        for (ConsumerRecord<byte[], byte[]> original : current.unit.originals) {
            stage(current, original, AcknowledgeType.RENEW);
        }
        Map<TopicIdPartition, Optional<KafkaException>> responses = consumer.commitSync(budget(current, settings.requestTimeoutMs, true));
        Map<TopicPartition, Uuid> topicIds = verifyRenewal(current, responses);
        requireBudget(current);
        long beforePoll = now();
        ConsumerRecords<byte[], byte[]> reattached = consumer.poll(budget(current, settings.pollTimeoutMs, false));
        current.unit.verifyReattachment(reattached);
        current.lockDurationMs = Math.min(current.lockDurationMs, lockDuration());
        current.leaseStartedMs = beforeRenew;
        current.pollStartedMs = beforePoll;
        current.topicIds = topicIds;
        requireBudget(current);
    }

    private Map<TopicPartition, Uuid> verifyRenewal(ActiveUnit current,
                                                 Map<TopicIdPartition, Optional<KafkaException>> responses) {
        Map<TopicPartition, Uuid> topicIds = new HashMap<>();
        for (Map.Entry<TopicIdPartition, Optional<KafkaException>> entry : responses.entrySet()) {
            TopicPartition partition = entry.getKey().topicPartition();
            if (!current.unit.records.partitions().contains(partition)) {
                continue;
            }
            Uuid topicId = entry.getKey().topicId();
            if (topicId.equals(Uuid.ZERO_UUID) || topicIds.putIfAbsent(partition, topicId) != null) {
                throw new ConnectException("Ambiguous share renewal identity for " + partition);
            }
            if (entry.getValue().isPresent()) {
                throw new ConnectException("Share renewal failed for " + partition, entry.getValue().get());
            }
        }
        if (!topicIds.keySet().equals(current.unit.records.partitions())) {
            throw new ConnectException("Share renewal did not confirm every input partition");
        }
        if (!current.topicIds.isEmpty() && !current.topicIds.equals(topicIds)) {
            throw new ConnectException("Share renewal topic identity changed");
        }
        return Map.copyOf(topicIds);
    }

    private void acknowledge(ActiveUnit current, ShareSinkDelivery.CompletedDelivery completed) {
        ShareSinkDelivery.AcknowledgementResult acknowledgement;
        try {
            acknowledgement = completed.acknowledge(consumer,
                    original -> stage(current, original, AcknowledgeType.ACCEPT),
                    () -> budget(current, settings.requestTimeoutMs, false))
                    .withExpectedTopicIds(current.topicIds);
        } finally {
            completed.acknowledgementResult().ifPresent(value -> {
                synchronized (monitor) {
                    lastAcknowledgement = value.withExpectedTopicIds(current.topicIds);
                }
            });
        }
        if (!acknowledgement.allConfirmed()) {
            throw new ConnectException("Share input acknowledgement was not fully confirmed");
        }
        synchronized (monitor) {
            result = null;
            active = null;
        }
    }

    private Duration budget(ActiveUnit current, long configuredMs, boolean reservePoll) {
        synchronized (monitor) {
            requireAuthority(current);
            long remaining = requireBudget(current);
            long timeout = Math.min(configuredMs, reservePoll ? remaining / 2 : remaining);
            if (timeout <= 0) {
                throw new ConnectException("Share processing has no safe control-call budget");
            }
            return Duration.ofMillis(timeout);
        }
    }

    private void stage(ActiveUnit current, ConsumerRecord<byte[], byte[]> original, AcknowledgeType type) {
        synchronized (monitor) {
            requireAuthority(current);
            requireBudget(current);
            consumer.acknowledge(original, type);
        }
    }

    private void requireAuthority(ActiveUnit current) {
        if (stopRequested || failure != null || active != current) {
            throw new ConnectException("Share input authority has been retired");
        }
    }

    private long requireBudget(ActiveUnit current) {
        long remaining = Math.min(remaining(current.unit.startedMs, settings.maxAgeMs),
                Math.min(remaining(current.leaseStartedMs, usable(current.lockDurationMs)),
                        remaining(current.pollStartedMs, usable(settings.maxPollIntervalMs))));
        if (remaining <= 0) {
            throw new ConnectException("Share processing age, lease or poll interval budget expired");
        }
        return remaining;
    }

    private long renewalDelay(ActiveUnit current) {
        return Math.min(remaining(current.unit.startedMs, settings.maxAgeMs),
                Math.min(remaining(current.leaseStartedMs, usable(current.lockDurationMs) / 2),
                        remaining(current.pollStartedMs, usable(settings.maxPollIntervalMs) / 2)));
    }

    private long remaining(long startedMs, long durationMs) {
        return durationMs - Math.max(0, now() - startedMs);
    }

    private static long usable(long durationMs) {
        return durationMs - Math.max(1, durationMs / 4);
    }

    private int lockDuration() {
        int duration = consumer.acquisitionLockTimeoutMs()
                .orElseThrow(() -> new ConnectException("The share acquisition lock duration is unavailable"));
        if (duration < 4) {
            throw new ConnectException("The share acquisition lock duration is too short");
        }
        return duration;
    }

    private long now() {
        return time.hiResClockMs();
    }

    private void fail(RuntimeException error) {
        synchronized (monitor) {
            if (failure == null) {
                failure = error;
            } else if (failure != error) {
                failure.addSuppressed(error);
            }
            handoff = null;
            monitor.notifyAll();
        }
    }

    private void finish() {
        signal();
        boolean interrupted = Thread.interrupted();
        boolean cleaned = false;
        try {
            ShareConsumer<byte[], byte[]> current = consumer;
            if (current != null) {
                current.close(Duration.ofMillis(settings.requestTimeoutMs));
            }
            cleaned = true;
        } catch (RuntimeException e) {
            fail(e);
        } catch (Error e) {
            fail(new ConnectException("Share consumer cleanup failed", e));
            throw e;
        } finally {
            consumer = null;
            synchronized (monitor) {
                cleanupComplete = cleaned;
                active = null;
                handoff = null;
                result = null;
                terminated.countDown();
                monitor.notifyAll();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    record Settings(long maxAgeMs, long pollTimeoutMs, long requestTimeoutMs,
                    long maxPollIntervalMs, int maxRecords, long maxBytes) {
        Settings {
            if (maxAgeMs <= 0 || pollTimeoutMs <= 0 || requestTimeoutMs <= 0
                    || maxPollIntervalMs < 4 || maxRecords <= 0 || maxBytes <= 0) {
                throw new IllegalArgumentException("Share controller limits must be positive and permit bounded polling");
            }
        }
    }

    record AttemptId(long incarnation, long unit, long attempt) {
    }

    static final class Unit {
        private final AttemptId id;
        private final ConsumerRecords<byte[], byte[]> records;
        private final List<ConsumerRecord<byte[], byte[]>> originals;
        private final long startedMs;
        private final long retainedBytes;

        private Unit(AttemptId id, ConsumerRecords<byte[], byte[]> records, long startedMs, Settings settings) {
            this.id = id;
            this.startedMs = startedMs;
            Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> snapshot = new HashMap<>();
            long bytes = 0;
            long count = 0;
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                if (partitionRecords.isEmpty()) {
                    continue;
                }
                Set<Long> offsets = new HashSet<>();
                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                    if (!partition.topic().equals(record.topic()) || partition.partition() != record.partition()
                            || !offsets.add(record.offset())) {
                        throw new ConnectException("Ambiguous original share record occurrence");
                    }
                    bytes = Math.addExact(bytes, recordBytes(record));
                }
                count += partitionRecords.size();
                snapshot.put(partition, List.copyOf(partitionRecords));
            }
            if (count == 0 || count > settings.maxRecords || bytes > settings.maxBytes) {
                throw new ConnectException("Share input unit exceeds configured record or byte capacity");
            }
            this.records = new ConsumerRecords<>(Map.copyOf(snapshot), Map.of());
            List<ConsumerRecord<byte[], byte[]>> originals = new ArrayList<>((int) count);
            this.records.forEach(originals::add);
            this.originals = List.copyOf(originals);
            this.retainedBytes = bytes;
        }

        private void verifyReattachment(ConsumerRecords<byte[], byte[]> returned) {
            if (!records.partitions().equals(returned.partitions()) || records.count() != returned.count()) {
                throw new ConnectException("Share renewal returned a different input unit");
            }
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<byte[], byte[]>> expected = records.records(partition);
                List<ConsumerRecord<byte[], byte[]>> actual = returned.records(partition);
                if (expected.size() != actual.size()) {
                    throw new ConnectException("Share renewal returned different input occurrences");
                }
                for (int i = 0; i < expected.size(); i++) {
                    if (expected.get(i) != actual.get(i)) {
                        throw new ConnectException("Share renewal did not reattach the original input occurrence");
                    }
                }
            }
        }

        private static long recordBytes(ConsumerRecord<byte[], byte[]> record) {
            long bytes = record.key() == null ? 0 : record.key().length;
            bytes += record.value() == null ? 0 : record.value().length;
            for (Header header : record.headers()) {
                bytes += Utils.utf8Length(header.key());
                bytes += header.value() == null ? 0 : header.value().length;
            }
            return bytes;
        }

        AttemptId id() {
            return id;
        }

        ConsumerRecords<byte[], byte[]> records() {
            return records;
        }

        long startedMs() {
            return startedMs;
        }

        long retainedBytes() {
            return retainedBytes;
        }
    }

    private static final class ActiveUnit {
        private final Unit unit;
        private int lockDurationMs;
        private long leaseStartedMs;
        private long pollStartedMs;
        private Map<TopicPartition, Uuid> topicIds = Map.of();

        private ActiveUnit(Unit unit, int lockDurationMs, long startedMs) {
            this.unit = unit;
            this.lockDurationMs = lockDurationMs;
            this.leaseStartedMs = startedMs;
            this.pollStartedMs = startedMs;
        }
    }

    private record Result(ShareSinkDelivery.CompletedDelivery completed, RuntimeException failure) {
    }
}
