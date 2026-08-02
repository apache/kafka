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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic end-to-end reproduction of the restore
 * {@code OffsetOutOfRangeException} -> {@code TaskCorruptedException} seen on the
 * {@code streams-soak-KIP-8921035-*} 4.3 stacks, and of the seek decision that
 * decides whether it happens.
 *
 * <p><b>The trick.</b> In the soak this is a race: the broker deletes the front of
 * a changelog faster than a from-scratch restore can read it. Races are not
 * reproducible, so instead the changelog is truncated <em>at the exact moment the
 * seek has happened but the first fetch has not</em>.
 * {@link StateRestoreListener#onRestoreStart} is invoked from
 * {@code StoreChangelogReader.prepareChangelogs} after {@code seekNewPartitions}
 * and before {@code pollRecordsFromRestoreConsumer}, so a listener calling
 * {@code Admin.deleteRecords} there advances log-start underneath the restore with
 * no race at all.
 *
 * <p><b>The variable under test is where the restore was seeked to.</b> All three
 * tests truncate the changelog to the same precomputed offset, and
 * {@code onRestoreStart}'s {@code startingOffset} reports the seek position — the
 * thing production logs cannot tell you, because every branch of
 * KAFKA-13499 / PR #22115 logs at {@code debug}.
 *
 * <ul>
 *   <li>{@link #nonWindowedRestoreSeeksToBeginningAndIsLappedByTruncation()} —
 *       {@code retentionPeriod} is -1, so #22115 never applies: seek to log-start,
 *       below the truncation point, lapped.</li>
 *   <li>{@link #windowedRestoreWithShortRetentionSeeksNearHeadAndSurvives()} —
 *       retention (2s) much shorter than the changelog's timestamp span (60s):
 *       seek lands near the head, above the truncation point, survives. #22115
 *       working as designed.</li>
 *   <li>{@link #windowedRestoreWithRetentionLongerThanTheLogSeeksToBeginningAndIsLapped()} —
 *       retention (1h) longer than the changelog's span, so
 *       {@code offsetsForTimes(latest - retention)} resolves to log-start and the
 *       optimisation buys nothing. This is the soak's own configuration: a windowed
 *       aggregate declaring {@code withRetention(Duration.ofHours(4))} against a
 *       changelog that physically retains ~56 minutes.</li>
 * </ul>
 *
 * <p>Records carry spread-out timestamps precisely so that "retention shorter than
 * the log" and "retention longer than the log" are both expressible.
 *
 * <p>Runs {@code exactly_once_v2}, matching the soaks. The EOS wipe is what
 * *causes* a from-scratch restore in production; that half is simulated here by
 * purging the local state directory between runs, but the guarantee itself is kept
 * faithful because it changes what the restore consumer can read (read_committed,
 * plus transaction markers occupying offsets in the changelog).
 */
@Timeout(600)
@Tag("integration")
public class RestoreLappedByRetentionIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_RECORDS = 3_000;
    private static final int NUM_KEYS = 500;
    /** Spacing between record timestamps: 3000 x 20ms = a 60s span. */
    private static final long TIMESTAMP_SPACING_MS = 20L;
    private static final int MIN_CHANGELOG_RECORDS = 1_000;
    /** Wall-clock duration over which the "live timing" fixture feeds the running app. */
    private static final long LIVE_PRODUCE_MS = 30_000L;
    private static final int LIVE_BATCHES = 60;
    /** Enough partitions to make the shared probe poll contend, as it does on the soak. */
    private static final int PROBE_PARTITIONS = 6;

    /**
     * Streams gives a KeyValue-store changelog {@code cleanup.policy=compact}, and
     * the broker rejects {@code DeleteRecords} on a compact-only topic with
     * {@code PolicyViolationException}. The soak's windowed changelogs are
     * {@code compact,delete}, which both permits truncation here and is what lets
     * retention delete the front of the log in production.
     */
    private static final Map<String, String> CHANGELOG_CONFIG =
        Map.of("cleanup.policy", "compact,delete");

    /**
     * The soak's stream-stream join changelogs are {@code cleanup.policy=delete} (measured on
     * {@code streams-soak-KIP-8921035-4-3-eos-v2}), unlike its aggregate changelogs. Matching
     * that here keeps the gate-1 test faithful to the store class it is about.
     */
    private static final Map<String, String> JOIN_CHANGELOG_CONFIG =
        Map.of("cleanup.policy", "delete");

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private String appId;
    private String inputTopic;
    private String otherTopic;
    private Properties streamsConfig;
    private Admin admin;

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        appId = safeUniqueTestName(testInfo);
        inputTopic = appId + "-input";
        otherTopic = appId + "-other";
        CLUSTER.createTopic(inputTopic, 1, 1);
        CLUSTER.createTopic(otherTopic, 1, 1);
        admin = CLUSTER.createAdminClient();
        streamsConfig = config();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (streamsConfig != null) {
            IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
        }
    }

    private Properties config() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        // EOS-v2, matching the soaks. Earlier revisions of this test used
        // at_least_once, which is NOT a faithful reproduction: under EOS the
        // changelog also carries transaction markers and the restore consumer is
        // read_committed, both of which affect what a restore can actually read.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        // No record cache: caching collapses repeated updates for the same key, so
        // with it on the changelog gets far fewer records than the input.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        return props;
    }

    private StreamsBuilder plainTopology(final String storeName) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, Integer>stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Integer()))
            .groupByKey()
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withLoggingEnabled(CHANGELOG_CONFIG));
        return builder;
    }

    private StreamsBuilder windowedTopology(final String storeName, final Duration retention) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, Integer>stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Integer()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(storeName)
                .withLoggingEnabled(CHANGELOG_CONFIG)
                .withRetention(retention));
        return builder;
    }

    /** As {@link #windowedTopology}, but reading a caller-supplied (multi-partition) topic. */
    private StreamsBuilder windowedTopologyOn(final String topic, final String storeName,
                                              final Duration retention) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, Integer>stream(topic, Consumed.with(Serdes.String(), Serdes.Integer()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(storeName)
                .withLoggingEnabled(CHANGELOG_CONFIG)
                .withRetention(retention));
        return builder;
    }

    /** {@link #produceInputPacedInRealTime} against a caller-supplied topic. */
    private void producePacedTo(final String topic) throws InterruptedException {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        final int perBatch = NUM_RECORDS / LIVE_BATCHES;
        try (final Producer<String, Integer> producer = new KafkaProducer<>(props)) {
            for (int batch = 0; batch < LIVE_BATCHES; batch++) {
                for (int i = 0; i < perBatch; i++) {
                    final int n = batch * perBatch + i;
                    producer.send(new ProducerRecord<>(topic, "k" + (n % NUM_KEYS), n));
                }
                producer.flush();
                Thread.sleep(LIVE_PRODUCE_MS / LIVE_BATCHES);
            }
        }
    }

    /**
     * A stream-stream join, which is the only topology that instantiates
     * {@link org.apache.kafka.streams.state.internals.PlainToHeadersWindowStoreAdapter} — the
     * store wrapper at the heart of gate 1. Window sizes match the soak verbatim
     * ({@code StreamsSoakTest.java:608}), giving a store retention of 2x1s + 1s = 3s against a
     * ~30s log.
     */
    private StreamsBuilder joinTopology(final String storeName) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> left =
            builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        final KStream<String, Integer> right =
            builder.stream(otherTopic, Consumed.with(Serdes.String(), Serdes.Integer()));
        left.join(right,
            Integer::sum,
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(1), Duration.ofSeconds(1)),
            StreamJoined.<String, Integer, Integer>with(
                    Serdes.String(), Serdes.Integer(), Serdes.Integer())
                .withStoreName(storeName)
                .withLoggingEnabled(JOIN_CHANGELOG_CONFIG));
        return builder;
    }

    /** Spread timestamps so "retention shorter/longer than the log" is meaningful. */
    private void produceInput() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        final long baseTimestamp =
            System.currentTimeMillis() - (NUM_RECORDS * TIMESTAMP_SPACING_MS) - 60_000L;
        try (final Producer<String, Integer> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, null,
                    baseTimestamp + i * TIMESTAMP_SPACING_MS, "k" + (i % NUM_KEYS), i));
            }
            producer.flush();
        }
    }

    /**
     * Production-like timing fixture.
     *
     * <p>{@link #populateChangelogThenWipeLocalState} produces all input first and only then
     * starts the app, so a 60s span of event time is written to the changelog in ~2s of wall
     * clock. Every transaction marker therefore carries roughly the same (current) timestamp
     * while the data around it carries event times up to 60s older — an event-time lag that does
     * not exist in the soak, where the measured lag is ~3.9s.
     *
     * <p>Here the app is started <em>first</em> and input is fed to it over
     * {@link #LIVE_PRODUCE_MS} of real time, so append time and event time advance together
     * exactly as they do in production. This is the fixture to use when the question is "does
     * this happen on the soak", rather than "is this mechanism real".
     */
    private long populateChangelogWithLiveTimestamps(final StreamsBuilder builder,
                                                     final String changelogTopic) throws Exception {
        final TopicPartition changelog = new TopicPartition(changelogTopic, 0);
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                Collections.singletonList(streams));
            produceInputPacedInRealTime();
            final AtomicLong previous = new AtomicLong(-1);
            final AtomicLong stableRounds = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                final long end = endOffsetOf(changelog);
                if (end >= MIN_CHANGELOG_RECORDS && end == previous.get()) {
                    return stableRounds.incrementAndGet() >= 3;
                }
                previous.set(end);
                stableRounds.set(0);
                return false;
            }, 120_000, "changelog " + changelogTopic + " never stabilised");
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
        return endOffsetOf(changelog);
    }

    /**
     * As {@link #populateChangelogWithLiveTimestamps}, but for a stream-stream join, whose
     * store — and therefore changelog — names are generated by the DSL. The changelog is
     * discovered at runtime rather than guessed.
     *
     * @return the name of one of the two join changelogs
     */
    private String populateJoinChangelogWithLiveTimestamps(final StreamsBuilder builder)
            throws Exception {
        final AtomicReference<String> discovered = new AtomicReference<>();
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                Collections.singletonList(streams));
            produceInputPacedInRealTime();
            TestUtils.waitForCondition(() -> {
                final String found = findJoinChangelog();
                discovered.set(found);
                return found != null;
            }, 60_000, "no join-store changelog was ever created");

            final TopicPartition changelog = new TopicPartition(discovered.get(), 0);
            final AtomicLong previous = new AtomicLong(-1);
            final AtomicLong stableRounds = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                final long end = endOffsetOf(changelog);
                if (end >= MIN_CHANGELOG_RECORDS && end == previous.get()) {
                    return stableRounds.incrementAndGet() >= 3;
                }
                previous.set(end);
                stableRounds.set(0);
                return false;
            }, 120_000, "join changelog " + discovered.get() + " never stabilised");
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
        return discovered.get();
    }

    /** The DSL names join stores itself, so find the changelog instead of assuming it. */
    private String findJoinChangelog() {
        try {
            final Set<String> topics = admin.listTopics().names().get();
            return topics.stream()
                .filter(t -> t.startsWith(appId) && t.endsWith("-changelog") && t.contains("join-store"))
                .sorted()
                .findFirst()
                .orElse(null);
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * Sends with no explicit timestamp, so the producer stamps wall clock, paced across
     * {@link #LIVE_PRODUCE_MS}. The app is already running and consumes as it goes.
     */
    private void produceInputPacedInRealTime() throws InterruptedException {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        final int perBatch = NUM_RECORDS / LIVE_BATCHES;
        try (final Producer<String, Integer> producer = new KafkaProducer<>(props)) {
            for (int batch = 0; batch < LIVE_BATCHES; batch++) {
                for (int i = 0; i < perBatch; i++) {
                    final int n = batch * perBatch + i;
                    // Both topics get the same records: the aggregate topologies read only
                    // inputTopic, while the join needs a matching record on each side.
                    producer.send(new ProducerRecord<>(inputTopic, "k" + (n % NUM_KEYS), n));
                    producer.send(new ProducerRecord<>(otherTopic, "k" + (n % NUM_KEYS), n));
                }
                producer.flush();
                Thread.sleep(LIVE_PRODUCE_MS / LIVE_BATCHES);
            }
        }
    }

    private long endOffsetOf(final TopicPartition partition) {
        try {
            return admin.listOffsets(Collections.singletonMap(partition, OffsetSpec.latest()))
                .partitionResult(partition).get().offset();
        } catch (final Exception e) {
            return -1L;
        }
    }

    /**
     * Runs the app once until the changelog has stopped growing, then stops it and
     * purges local state. Waiting for STABILITY (not merely a minimum) matters: if
     * the app is still consuming input during the restore run, the changelog grows
     * underneath the test and the truncation point is no longer comparable to the
     * seek position.
     */
    private long populateChangelogThenWipeLocalState(final StreamsBuilder builder,
                                                     final String changelogTopic) throws Exception {
        produceInput();
        final TopicPartition changelog = new TopicPartition(changelogTopic, 0);
        try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                Collections.singletonList(streams));
            final AtomicLong previous = new AtomicLong(-1);
            final AtomicLong stableRounds = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                final long end = endOffsetOf(changelog);
                if (end >= MIN_CHANGELOG_RECORDS && end == previous.get()) {
                    return stableRounds.incrementAndGet() >= 3;
                }
                previous.set(end);
                stableRounds.set(0);
                return false;
            }, 120_000, "changelog " + changelogTopic + " never stabilised");
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
        return endOffsetOf(changelog);
    }

    /** Truncates to a precomputed offset the first time any restore begins. */
    private final class TruncateOnRestoreStart implements StateRestoreListener {
        private final long truncateTo;
        private final AtomicBoolean fired = new AtomicBoolean(false);
        private final AtomicBoolean restoreEnded = new AtomicBoolean(false);
        private final AtomicLong observedStartOffset = new AtomicLong(-1);
        private final AtomicReference<String> error = new AtomicReference<>();

        private TruncateOnRestoreStart(final long truncateTo) {
            this.truncateTo = truncateTo;
        }

        @Override
        public void onRestoreStart(final TopicPartition partition, final String store,
                                   final long startingOffset, final long endingOffset) {
            observedStartOffset.compareAndSet(-1, startingOffset);
            if (!fired.compareAndSet(false, true)) {
                return;
            }
            try {
                admin.deleteRecords(Collections.singletonMap(
                        partition, RecordsToDelete.beforeOffset(truncateTo))).all().get();
            } catch (final Exception e) {
                // Never throw from the listener: Streams treats that as fatal and
                // would mask what is being measured.
                error.set("deleteRecords failed for " + partition + ": " + e);
            }
        }

        @Override
        public void onBatchRestored(final TopicPartition p, final String s, final long o, final long n) { }

        @Override
        public void onRestoreEnd(final TopicPartition p, final String s, final long total) {
            restoreEnded.set(true);
        }
    }

    private Outcome restoreWithTruncation(final StreamsBuilder builder,
                                          final long truncateTo) throws Exception {
        final TruncateOnRestoreStart listener = new TruncateOnRestoreStart(truncateTo);
        final boolean sawOoore;
        try (final LogCaptureAppender appender =
                 LogCaptureAppender.createAndRegister(StoreChangelogReader.class)) {
            try (final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig)) {
                streams.setGlobalStateRestoreListener(listener);
                streams.start();
                // Settle on either outcome rather than always burning the full timeout.
                try {
                    TestUtils.waitForCondition(
                        () -> oooreLogged(appender) || listener.restoreEnded.get(),
                        90_000,
                        "neither an OOORE nor a completed restore was observed");
                } catch (final AssertionError ignored) {
                    // fall through; treated as "no OOORE"
                }
                sawOoore = oooreLogged(appender);
            }
        }
        return new Outcome(sawOoore, listener.observedStartOffset.get(), truncateTo,
            listener.error.get());
    }

    private boolean oooreLogged(final LogCaptureAppender appender) {
        return appender.getMessages().stream().anyMatch(m -> m.contains("OffsetOutOfRangeException"));
    }

    private record Outcome(boolean sawOoore, long startOffset, long truncatedTo, String error) { }

    private long endOffsetSeenBy(final String isolationLevel, final TopicPartition tp) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        try (final Consumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.assign(Collections.singletonList(tp));
            return c.endOffsets(Collections.singletonList(tp)).get(tp);
        }
    }

    /** Exactly what #22115's probe does: seek to an offset, poll, count records. */
    private int recordsAt(final String isolationLevel, final TopicPartition tp, final long offset) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        try (final Consumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.assign(Collections.singletonList(tp));
            c.seek(tp, Math.max(0, offset));
            return c.poll(Duration.ofSeconds(5)).records(tp).size();
        }
    }

    /**
     * Settles WHY #22115's probe poll comes back empty under EOS. It seeks to
     * {@code endOffset - 1} and polls; if that returns nothing, the optimisation
     * silently falls back to seekToBeginning.
     *
     * <p>Two candidate explanations, distinguished here without changing any app
     * config:
     * <ul>
     *   <li><b>Transaction markers</b> — the tail offset is a control record, which
     *       occupies an offset but is never returned. Then HW == LSO, but a poll at
     *       {@code end-1} still yields 0.</li>
     *   <li><b>LSO</b> — an open transaction holds the last stable offset below the
     *       high watermark, so a read_committed consumer cannot see the tail at all.
     *       Then LSO &lt; HW.</li>
     * </ul>
     */
    @Test
    public void whyTheProbePollComesBackEmptyUnderEos() throws Exception {
        final String store = "windowed-store";
        final String changelog = appId + "-" + store + "-changelog";
        populateChangelogThenWipeLocalState(
            windowedTopology(store, Duration.ofSeconds(2)), changelog);

        final TopicPartition tp = new TopicPartition(changelog, 0);
        final long highWatermark = endOffsetSeenBy("read_uncommitted", tp);
        final long lastStable = endOffsetSeenBy("read_committed", tp);

        final int atLsoMinusOne = recordsAt("read_committed", tp, lastStable - 1);
        final int atHwMinusOne = recordsAt("read_uncommitted", tp, highWatermark - 1);

        System.out.printf(
            "%n=== changelog tail under EOS-v2: %s%n"
                + "    high watermark (read_uncommitted endOffsets) : %d%n"
                + "    last stable offset (read_committed endOffsets): %d%n"
                + "    LSO gap (HW - LSO)                            : %d%n"
                + "    records at LSO-1 (read_committed, what #22115 does): %d%n"
                + "    records at HW-1  (read_uncommitted)               : %d%n"
                + "    => %s%n",
            changelog, highWatermark, lastStable, highWatermark - lastStable,
            atLsoMinusOne, atHwMinusOne,
            highWatermark > lastStable
                ? "OPEN TRANSACTION: read_committed cannot see the tail"
                : (atLsoMinusOne == 0
                    ? "TAIL OFFSET IS A CONTROL RECORD (transaction marker)"
                    : "tail is readable -- neither explanation holds here"));
    }

    /**
     * Instrumented probe for the remaining gap. After the backward-probe fix,
     * #22115 stops falling back to log-start but still seeks far short of the head
     * (903 of ~3000 for a 2s-retention store on a 60s log). #22115 assumes
     * {@code offsetsForTimes(latestTimestamp - retention)} cuts near the head,
     * which only holds if the changelog is ordered by timestamp.
     *
     * <p>A windowed store's changelog record carries the WINDOW/event time, not the
     * append time, so that ordering is not guaranteed. This reads the changelog and
     * measures it directly.
     */
    @Test
    public void isTheWindowedChangelogOrderedByTimestamp() throws Exception {
        final String store = "windowed-store";
        final String changelog = appId + "-" + store + "-changelog";
        populateChangelogThenWipeLocalState(
            windowedTopology(store, Duration.ofSeconds(2)), changelog);

        final TopicPartition tp = new TopicPartition(changelog, 0);
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        long tailTs = -1;
        long maxTsOffset = -1;
        long lastTs = Long.MIN_VALUE;
        long inversions = 0;
        long dataRecords = 0;
        long prevOffset = -1;
        final StringBuilder controlOffsets = new StringBuilder();
        final long end;

        try (final Consumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.assign(Collections.singletonList(tp));
            end = c.endOffsets(Collections.singletonList(tp)).get(tp);
            c.seek(tp, c.beginningOffsets(Collections.singletonList(tp)).get(tp));
            final long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline && c.position(tp) < end) {
                final ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(500));
                if (recs.isEmpty() && c.position(tp) >= end) {
                    break;
                }
                for (final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> r : recs.records(tp)) {
                    // Offsets the consumer never delivers are transaction control
                    // records: a marker occupies an offset but is filtered out.
                    if (prevOffset >= 0) {
                        for (long missing = prevOffset + 1; missing < r.offset(); missing++) {
                            controlOffsets.append(missing).append(' ');
                        }
                    }
                    prevOffset = r.offset();
                    dataRecords++;
                    final long ts = r.timestamp();
                    if (ts < minTs) {
                        minTs = ts;
                    }
                    if (ts > maxTs) {
                        maxTs = ts;
                        maxTsOffset = r.offset();
                    }
                    if (ts < lastTs) {
                        inversions++;
                    }
                    lastTs = ts;
                    tailTs = ts;
                }
            }
        }

        // what #22115 would compute, given a 2s retention
        final long seekTarget = maxTs - Duration.ofSeconds(2).toMillis();
        final long wallClock = System.currentTimeMillis();
        long offsetForSeekTarget = -1;
        long timestampAtSeekTarget = -1;
        try (final Consumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.assign(Collections.singletonList(tp));
            final var r = c.offsetsForTimes(Collections.singletonMap(tp, seekTarget)).get(tp);
            offsetForSeekTarget = r == null ? -1 : r.offset();
            timestampAtSeekTarget = r == null ? -1 : r.timestamp();
        }

        // The honest answer: the first DATA record at or after the target. If
        // offsetsForTimes disagrees with this, it answered with something the
        // restore consumer will never be handed -- i.e. a control record.
        final long firstDataOffsetAtTarget =
            firstDataOffsetWithTimestampAtLeast(props, tp, seekTarget, end);

        System.out.printf(
            "%n=== windowed changelog timestamp shape: %s%n"
                + "    endOffset                       : %d%n"
                + "    data records read               : %d  (endOffset - data = %d control records)%n"
                + "    timestamp span                  : %d ms (min=%d max=%d)%n"
                + "    offset carrying the MAX timestamp: %d  (%.1f%% through the log)%n"
                + "    timestamp of the LAST data record: %d  (max - tail = %d ms behind the max)%n"
                + "    out-of-order pairs (ts[i] < ts[i-1]): %d of %d  (%.1f%%)%n"
                + "    control-record offsets          : %s%n"
                + "  --- the seek #22115 asks for, target = max - 2s = %d ---%n"
                + "    offsetsForTimes -> offset       : %d  (%.1f%% through the log)%n"
                + "    offsetsForTimes -> timestamp    : %d%n"
                + "        vs max DATA timestamp       : %+d ms%n"
                + "        vs wall clock now (%d)      : %+d ms%n"
                + "    first DATA record >= target     : %d  (%.1f%% through the log)%n"
                + "    => %s%n",
            changelog, end, dataRecords, end - dataRecords,
            maxTs - minTs, minTs, maxTs,
            maxTsOffset, 100.0 * maxTsOffset / Math.max(1, end),
            tailTs, maxTs - tailTs,
            inversions, dataRecords, 100.0 * inversions / Math.max(1, dataRecords),
            controlSummary(controlOffsets.toString().trim()),
            seekTarget,
            offsetForSeekTarget, 100.0 * offsetForSeekTarget / Math.max(1, end),
            timestampAtSeekTarget,
            timestampAtSeekTarget - maxTs,
            wallClock, timestampAtSeekTarget - wallClock,
            firstDataOffsetAtTarget, 100.0 * firstDataOffsetAtTarget / Math.max(1, end),
            seekVerdict(offsetForSeekTarget, firstDataOffsetAtTarget));
    }

    private static String controlSummary(final String offsets) {
        return offsets.isEmpty() ? "(none before the last data record)" : offsets;
    }

    private static String seekVerdict(final long fromOffsetsForTimes, final long fromData) {
        return fromOffsetsForTimes == fromData
            ? "offsetsForTimes agrees with the data: the seek is sound"
            : "offsetsForTimes DISAGREES with the data by "
                + (fromData - fromOffsetsForTimes) + " offsets -- it answered with a record "
                + "the restore consumer is never handed";
    }

    /**
     * Scan the changelog and return the offset of the first <em>data</em> record whose
     * timestamp is at or after {@code target}. This is the answer #22115 believes it is
     * getting from {@code offsetsForTimes}; any difference is the bug.
     */
    private long firstDataOffsetWithTimestampAtLeast(final Properties props,
                                                     final TopicPartition tp,
                                                     final long target,
                                                     final long end) {
        try (final Consumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            c.assign(Collections.singletonList(tp));
            c.seek(tp, c.beginningOffsets(Collections.singletonList(tp)).get(tp));
            final long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline && c.position(tp) < end) {
                final ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(500));
                for (final org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> r : recs.records(tp)) {
                    if (r.timestamp() >= target) {
                        return r.offset();
                    }
                }
                if (recs.isEmpty() && c.position(tp) >= end) {
                    break;
                }
            }
        }
        return -1;
    }

    // ------------------------------------------------------------------
    // 1. non-windowed: retentionPeriod -1, #22115 never applies
    // ------------------------------------------------------------------
    @Test
    public void nonWindowedRestoreSeeksToBeginningAndIsLappedByTruncation() throws Exception {
        final String store = "plain-store";
        final String changelog = appId + "-" + store + "-changelog";

        final long end = populateChangelogThenWipeLocalState(plainTopology(store), changelog);
        final Outcome outcome = restoreWithTruncation(plainTopology(store), end / 2);

        assertNull(outcome.error(), "truncation hook reported: " + outcome.error());
        assertEquals(0L, outcome.startOffset(),
            "a non-windowed store has retentionPeriod -1, so it must seek to log-start");
        assertTrue(outcome.sawOoore(),
            "restoring from log-start below the truncation point (" + outcome.truncatedTo()
                + ") must raise OffsetOutOfRangeException");
    }

    // ------------------------------------------------------------------
    // 2c. MULTI-PARTITION PROBE STARVATION.
    //
    //     Reproduces the OOORE seen on the fixed soak at 2026-07-31 03:18.
    //     The backward probe shares one poll across every unresolved
    //     partition and re-seeks all of them each round, with each partition's
    //     step-back doubling whenever it was not served. A re-seek discards a
    //     fetch that was in flight, so with several partitions competing a
    //     partition can burn the whole PROBE_MAX_ATTEMPTS budget without its
    //     own offset ever being the problem, and then falls back to log-start
    //     with zero margin.
    //
    //     Production evidence: "targets=5 attempts=10 resolved=4
    //     newFallbacks=1", then lapped 67s later; the same partition resolved
    //     on the next try with backUsed=2. Every partition here has a 2s
    //     retention against a much longer log, so a log-start seek for ANY of
    //     them is the bug.
    // ------------------------------------------------------------------
    @Timeout(600)
    @Test
    public void multiPartitionProbeMustNotStarveAnyPartitionIntoALogStartSeek() throws Exception {
        final String store = "windowed-store";
        final String multiTopic = appId + "-multi";
        CLUSTER.createTopic(multiTopic, PROBE_PARTITIONS, 1);

        // Populate with production-like timing: the app runs while input arrives.
        try (final KafkaStreams streams =
                 new KafkaStreams(windowedTopologyOn(multiTopic, store, Duration.ofSeconds(2)).build(),
                     streamsConfig)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                Collections.singletonList(streams));
            producePacedTo(multiTopic);
            final AtomicLong previous = new AtomicLong(-1);
            final AtomicLong stable = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                long total = 0;
                for (int p = 0; p < PROBE_PARTITIONS; p++) {
                    total += Math.max(0, endOffsetOf(
                        new TopicPartition(appId + "-" + store + "-changelog", p)));
                }
                if (total >= MIN_CHANGELOG_RECORDS && total == previous.get()) {
                    return stable.incrementAndGet() >= 3;
                }
                previous.set(total);
                stable.set(0);
                return false;
            }, 180_000, "multi-partition changelog never stabilised");
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);

        // Restore from scratch and record where EVERY partition was seeked to.
        final Map<TopicPartition, Long> startOffsets = new ConcurrentHashMap<>();
        final CountDownLatch seen = new CountDownLatch(PROBE_PARTITIONS);
        final StateRestoreListener listener = new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition partition, final String storeName,
                                       final long startingOffset, final long endingOffset) {
                if (startOffsets.putIfAbsent(partition, startingOffset) == null) {
                    seen.countDown();
                }
            }

            @Override
            public void onBatchRestored(final TopicPartition p, final String s, final long o, final long n) { }

            @Override
            public void onRestoreEnd(final TopicPartition p, final String s, final long t) { }
        };

        try (final KafkaStreams streams =
                 new KafkaStreams(windowedTopologyOn(multiTopic, store, Duration.ofSeconds(2)).build(),
                     streamsConfig)) {
            streams.setGlobalStateRestoreListener(listener);
            streams.start();
            assertTrue(seen.await(180, java.util.concurrent.TimeUnit.SECONDS),
                "only saw restore start for " + startOffsets.size() + " of " + PROBE_PARTITIONS
                    + " partitions: " + startOffsets);
        }

        // TopicPartition is not Comparable, so key the report by partition number.
        final Map<Integer, Long> byPartition = new java.util.TreeMap<>();
        startOffsets.forEach((tp, off) -> byPartition.put(tp.partition(), off));
        final Map<Integer, Long> starved = new java.util.TreeMap<>();
        byPartition.forEach((p, off) -> {
            if (off == 0L) {
                starved.put(p, off);
            }
        });
        System.out.printf("%n=== multi-partition probe: seek positions by partition ===%n"
                + "  %s%n  starved (seeked to log-start): %d of %d%n",
            byPartition, starved.size(), byPartition.size());

        assertTrue(starved.isEmpty(),
            "every partition has a 2s retention against a much longer log, so none should seek "
                + "to log-start; these were starved by the shared probe poll and fell back with "
                + "zero margin: " + starved);
    }

    // ------------------------------------------------------------------
    // 2b. GATE 1, end to end. A stream-stream join is the only topology that
    //     builds a PlainToHeadersWindowStoreAdapter. Without the adapter fix
    //     its retention resolves to -1, #22115 is skipped and the restore
    //     seeks log-start; with it, the 3s store retention puts the seek near
    //     the head of a ~30s log. Production-like timing, delete-only
    //     changelog, join windows copied from the soak.
    // ------------------------------------------------------------------
    @Timeout(300)
    @Test
    public void streamStreamJoinRestoreSeeksNearHeadOnceTheAdapterExposesItsRetention()
            throws Exception {
        final String changelog = populateJoinChangelogWithLiveTimestamps(joinTopology("j"));
        final long end = endOffsetOf(new TopicPartition(changelog, 0));
        final Outcome outcome = restoreWithTruncation(joinTopology("j"), end / 2);

        assertNull(outcome.error(), "truncation hook reported: " + outcome.error());
        assertTrue(outcome.startOffset() > outcome.truncatedTo(),
            "a stream-stream join store has a 3s retention against a ~30s changelog, so the "
                + "restore must seek near the head, above the truncation point; seeking "
                + "log-start means the adapter hid the store's retention (start="
                + outcome.startOffset() + " truncatedTo=" + outcome.truncatedTo() + ")");
        assertFalse(outcome.sawOoore(),
            "seeking near the head must survive the truncation");
    }

    // ------------------------------------------------------------------
    // 2a. THE DECISIVE ONE. Same as test 2, but with production-like timing:
    //     event time and append time advance together, so the ~60s artificial
    //     event-time lag is gone and transaction markers no longer outrank the
    //     data around them. This runs on a branch WITHOUT the broker-side
    //     control-batch fix, so passing here proves the two Streams gate fixes
    //     are sufficient on their own under the soak's actual timing.
    // ------------------------------------------------------------------
    @Timeout(300)
    @Test
    public void windowedRestoreWithLiveTimestampsSeeksNearHeadWithoutTheBrokerFix()
            throws Exception {
        final String store = "windowed-store";
        final String changelog = appId + "-" + store + "-changelog";

        final long end = populateChangelogWithLiveTimestamps(
            windowedTopology(store, Duration.ofSeconds(2)), changelog);
        final Outcome outcome = restoreWithTruncation(
            windowedTopology(store, Duration.ofSeconds(2)), end / 2);

        assertNull(outcome.error(), "truncation hook reported: " + outcome.error());
        assertTrue(outcome.startOffset() > outcome.truncatedTo(),
            "with production-like timestamps the two gate fixes alone must seek a 2s-retention "
                + "store above the truncation point, with no broker change (start="
                + outcome.startOffset() + " truncatedTo=" + outcome.truncatedTo() + ")");
        assertFalse(outcome.sawOoore(),
            "seeking near the head must survive the truncation that kills the plain store");
    }

    // ------------------------------------------------------------------
    // 2. windowed, retention << log span: seeks near head, SURVIVES
    // ------------------------------------------------------------------
    @Disabled("Needs the broker-side control-batch fix as well as the two gate fixes on this "
        + "branch: offsetsForTimes searches transaction control batches, which carry the append "
        + "time rather than an event time, so on this changelog (data timestamps deliberately "
        + "60s in the past) the earliest commit marker satisfies the target and the seek "
        + "collapses to log-start. Fixed on branch offsetsfortimes-skip-control-batches; with "
        + "that change plus these two, this test passes. See "
        + "isTheWindowedChangelogOrderedByTimestamp() for the measurement.")
    @Test
    public void windowedRestoreWithShortRetentionSeeksNearHeadAndSurvives() throws Exception {
        final String store = "windowed-store";
        final String changelog = appId + "-" + store + "-changelog";

        final long end = populateChangelogThenWipeLocalState(
            windowedTopology(store, Duration.ofSeconds(2)), changelog);
        final Outcome outcome = restoreWithTruncation(
            windowedTopology(store, Duration.ofSeconds(2)), end / 2);

        assertNull(outcome.error(), "truncation hook reported: " + outcome.error());
        assertTrue(outcome.startOffset() > outcome.truncatedTo(),
            "#22115 should seek a 2s-retention store near the head of a 60s log, above the "
                + "truncation point (start=" + outcome.startOffset()
                + " truncatedTo=" + outcome.truncatedTo() + ")");
        assertFalse(outcome.sawOoore(),
            "seeking near the head should survive the truncation that kills the plain store");
    }

    // ------------------------------------------------------------------
    // 3. windowed, retention >> log span: optimisation is a no-op, IS lapped.
    //    The soak's own shape: withRetention(4h) on a ~56min changelog.
    // ------------------------------------------------------------------
    @Test
    public void windowedRestoreWithRetentionLongerThanTheLogSeeksToBeginningAndIsLapped()
            throws Exception {
        final String store = "windowed-store";
        final String changelog = appId + "-" + store + "-changelog";

        final long end = populateChangelogThenWipeLocalState(
            windowedTopology(store, Duration.ofHours(1)), changelog);
        final Outcome outcome = restoreWithTruncation(
            windowedTopology(store, Duration.ofHours(1)), end / 2);

        assertNull(outcome.error(), "truncation hook reported: " + outcome.error());
        assertEquals(0L, outcome.startOffset(),
            "when retention exceeds the log's span, offsetsForTimes(latest - retention) "
                + "resolves to log-start and #22115 buys nothing");
        assertTrue(outcome.sawOoore(),
            "so a long-retention windowed store is lapped exactly like the plain store");
    }
}
