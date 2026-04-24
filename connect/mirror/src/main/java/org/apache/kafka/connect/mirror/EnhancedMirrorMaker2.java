package com.kafka.mirror;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;     
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EnhancedMirrorMaker2
 *
 * Replicates messages from a source Kafka cluster (primary) to a destination
 * Kafka cluster (DR/standby) with two fault-tolerance enhancements over
 * vanilla MirrorMaker 2:
 *
 *  1. Log Truncation Detection (Task 2)
 *     Detects when Kafka's retention policy purges messages that have not yet
 *     been replicated, causing a silent offset gap.  On detection the replicator
 *     logs a detailed error and throws a RuntimeException (fail-fast).
 *
 *  2. Graceful Topic Reset Handling (Task 3)
 *     Detects when the source topic is deleted and recreated (offset resets to 0
 *     while the consumer is mid-stream).  Instead of crashing, the replicator
 *     logs the event, resubscribes from the beginning offset, and continues
 *     replication automatically.
 */
public class EnhancedMirrorMaker2 {

    private static final Logger log = LoggerFactory.getLogger(EnhancedMirrorMaker2.class);

    // ── Environment-driven configuration ─────────────────────────────────────

    private static final String SOURCE_BOOTSTRAP  =
        System.getenv().getOrDefault("SOURCE_BOOTSTRAP_SERVERS",  "localhost:9092");
    private static final String DEST_BOOTSTRAP    =
        System.getenv().getOrDefault("DEST_BOOTSTRAP_SERVERS",    "localhost:9093");
    private static final String SOURCE_TOPIC      =
        System.getenv().getOrDefault("SOURCE_TOPIC",              "commit-log");
    private static final String DEST_TOPIC        =
        System.getenv().getOrDefault("DEST_TOPIC",                "primary.commit-log");
    private static final String CONSUMER_GROUP    =
        System.getenv().getOrDefault("CONSUMER_GROUP",            "mirror-maker-group");

    // Poll / retry tuning
    private static final Duration POLL_DURATION       = Duration.ofMillis(500);
    private static final int      MAX_RESET_RETRIES   = 10;
    private static final long     RESET_RETRY_DELAY_MS = 3_000L;

    private static final AtomicBoolean running = new AtomicBoolean(true);

    // ── State tracking ────────────────────────────────────────────────────────

    /** Last offset successfully replicated from the source topic. */
    private long lastReplicatedOffset = -1L;

    /** Topic ID (UUID) used to detect deletion + recreation. */
    private Uuid lastKnownTopicId = null;

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private AdminClient                   sourceAdmin;

    // ─────────────────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        // Graceful shutdown on SIGTERM / Ctrl-C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("[MirrorMaker] Shutdown signal received — stopping.");
            running.set(false);
        }));

        new EnhancedMirrorMaker2().run();
    }

    public void run() {
        log.info("[MirrorMaker] Starting — source={} dest={} topic={}→{}",
            SOURCE_BOOTSTRAP, DEST_BOOTSTRAP, SOURCE_TOPIC, DEST_TOPIC);

        try {
            sourceAdmin = buildAdminClient(SOURCE_BOOTSTRAP);
            producer    = buildProducer(DEST_BOOTSTRAP);
            consumer    = buildConsumer(SOURCE_BOOTSTRAP);

            // Record the topic ID before we start consuming
            lastKnownTopicId = fetchTopicId(SOURCE_TOPIC);
            log.info("[MirrorMaker] Source topic ID at startup: {}", lastKnownTopicId);

            TopicPartition tp = new TopicPartition(SOURCE_TOPIC, 0);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            replicate();

        } catch (Exception e) {
            log.error("[MirrorMaker] Fatal error — exiting.", e);
            System.exit(1);
        } finally {
            closeAll();
        }
    }

    // ── Main replication loop ─────────────────────────────────────────────────

    private void replicate() {
        TopicPartition tp = new TopicPartition(SOURCE_TOPIC, 0);

        while (running.get()) {
            ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION);

            if (records.isEmpty()) {
            // Check for truncation even when no records arrive
                long earliest = fetchEarliestOffset(tp);
                if (lastReplicatedOffset >= 0 && earliest > lastReplicatedOffset + 1) {
                    String msg = String.format(
                        "[MirrorMaker] LOG TRUNCATION DETECTED on topic='%s' partition=%d — " +
                        "expected offset=%d but earliest available=%d. Failing fast.",
                        SOURCE_TOPIC, tp.partition(), lastReplicatedOffset + 1, earliest);
                    log.error(msg);
                    throw new RuntimeException(msg);
                }
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {
                if (lastReplicatedOffset < 0) lastReplicatedOffset = record.offset() - 1;

                // ── TASK 2: Log Truncation Detection ──────────────────────────
                checkForLogTruncation(record, tp);

                // ── TASK 3: Topic Reset Detection ─────────────────────────────
                if (isTopicReset()) {
                    handleTopicReset(tp);
                    // After reset, break inner loop — consumer position has changed
                    break;
                }

                // ── Normal replication ─────────────────────────────────────────
                replicateRecord(record);
                lastReplicatedOffset = record.offset();
            }

            // Commit offsets after each successful batch
        }
    }

    // ── Task 2: Log Truncation Detection ─────────────────────────────────────

    /**
     * Compares the current record's offset against the last replicated offset.
     * If there is a forward gap AND the earliest available offset on the source
     * is greater than (lastReplicatedOffset + 1), messages were purged by
     * retention before we could replicate them — silent data loss.
     *
     * On detection: logs a detailed error and throws RuntimeException (fail-fast).
     */
    private void checkForLogTruncation(ConsumerRecord<String, String> record,
                                       TopicPartition tp) {
        if (lastReplicatedOffset < 0) {
            lastReplicatedOffset = record.offset() - 1;
        }

        long expectedOffset = lastReplicatedOffset + 1;
        long actualOffset   = record.offset();

        if (actualOffset > expectedOffset) {
            // There is a gap — check whether the earliest offset explains it
            long earliestOffset = fetchEarliestOffset(tp);

            if (earliestOffset > expectedOffset) {
                // Confirmed truncation: messages were purged before replication
                String msg = String.format(
                    "[MirrorMaker] LOG TRUNCATION DETECTED on topic='%s' partition=%d — " +
                    "expected offset=%d but earliest available=%d, current record offset=%d. " +
                    "Messages in range [%d, %d) were purged by retention before replication. " +
                    "Data loss has occurred. Failing fast.",
                    SOURCE_TOPIC, tp.partition(),
                    expectedOffset, earliestOffset, actualOffset,
                    expectedOffset, earliestOffset
                );
                log.error(msg);
                throw new RuntimeException(msg);
            }
        }
    }

    // ── Task 3: Topic Reset Detection ────────────────────────────────────────

    /**
     * A topic reset (delete + recreate) is detected when:
     *   - The current record offset is 0 (or less than what we last replicated), AND
     *   - The topic's Kafka-internal UUID has changed (new topic, same name).
     *
     * Offset rollback alone is not enough — Kafka can legitimately seek to 0.
     * Comparing the topic ID is the reliable signal.
     */
    private boolean isTopicReset() {
    // Guard: only detect reset after real consumption has started
    if (lastReplicatedOffset < 0) {
        return false;
    }

    Uuid currentTopicId = fetchTopicId(SOURCE_TOPIC);

    if (currentTopicId != null &&
        lastKnownTopicId != null &&
        !currentTopicId.equals(lastKnownTopicId)) {

        log.warn(
            "[MirrorMaker] TOPIC RESET DETECTED — topic='{}' oldId={} newId={}",
            SOURCE_TOPIC,
            lastKnownTopicId,
            currentTopicId
        );

        lastKnownTopicId = currentTopicId;
        return true;
    }

    return false;
}
    /**
     * Handles a confirmed topic reset by resubscribing from offset 0.
     * Retries up to MAX_RESET_RETRIES times in case the topic is still
     * being recreated when we attempt to resubscribe.
     */
    private void handleTopicReset(TopicPartition tp) {
        log.info("[MirrorMaker] Handling topic reset — will resubscribe from beginning.");

        for (int attempt = 1; attempt <= MAX_RESET_RETRIES; attempt++) {
            try {
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));
                lastReplicatedOffset = -1L;
                log.info("[MirrorMaker] Resubscribed from beginning offset after reset " +
                         "(attempt {}/{})", attempt, MAX_RESET_RETRIES);
                return;
            } catch (Exception e) {
                log.warn("[MirrorMaker] Resubscribe attempt {}/{} failed: {} — retrying in {}ms",
                    attempt, MAX_RESET_RETRIES, e.getMessage(), RESET_RETRY_DELAY_MS);
                sleep(RESET_RETRY_DELAY_MS);
            }
        }

        throw new RuntimeException(
            "[MirrorMaker] Could not resubscribe after topic reset after " +
            MAX_RESET_RETRIES + " attempts.");
    }

    // ── Replicate a single record to the destination ──────────────────────────

    private void replicateRecord(ConsumerRecord<String, String> record) {
        ProducerRecord<String, String> outRecord =
            new ProducerRecord<>(DEST_TOPIC, record.key(), record.value());

        try {
            RecordMetadata meta = producer.send(outRecord).get();
            log.debug("[MirrorMaker] Replicated offset={} → dest partition={} offset={}",
                record.offset(), meta.partition(), meta.offset());
        } catch (InterruptedException | ExecutionException e) {
            log.error("[MirrorMaker] Failed to replicate record at offset {}: {}",
                record.offset(), e.getMessage());
            throw new RuntimeException("Replication failed", e);
        }
    }

    // ── Admin helpers ─────────────────────────────────────────────────────────

    /** Fetch the earliest available offset for the given partition. */
    private long fetchEarliestOffset(TopicPartition tp) {
        Map<TopicPartition, Long> offsets =
            consumer.beginningOffsets(Collections.singletonList(tp));
        return offsets.getOrDefault(tp, -1L);
    }

    /** Fetch the Kafka-internal topic UUID via AdminClient (null if not found). */
    private Uuid fetchTopicId(String topicName) {
        try {
            DescribeTopicsResult result =
                sourceAdmin.describeTopics(Collections.singletonList(topicName));
            TopicDescription desc = result.topicNameValues().get(topicName).get();
            return desc.topicId();
        } catch (InterruptedException | ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException) {
                log.warn("[MirrorMaker] Topic '{}' not found when fetching ID.", topicName);
                return null;
            }
            log.error("[MirrorMaker] Error fetching topic ID for '{}': {}", topicName, e.getMessage());
            return null;
        }
    }

    // ── Client builders ───────────────────────────────────────────────────────

    private static KafkaConsumer<String, String> buildConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         500);
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> buildProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(ProducerConfig.RETRIES_CONFIG,                3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);
        return new KafkaProducer<>(props);
    }

    private static AdminClient buildAdminClient(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private void closeAll() {
        try { if (consumer    != null) consumer.close();    } catch (Exception ignored) {}
        try { if (producer    != null) producer.close();    } catch (Exception ignored) {}
        try { if (sourceAdmin != null) sourceAdmin.close(); } catch (Exception ignored) {}
        log.info("[MirrorMaker] All clients closed.");
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
