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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.StoreChangelogReader;
import org.apache.kafka.streams.state.WindowStore;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.test.TestUtils.tempDirectory;

/**
 * A long-running LOCAL soak that reproduces the KIP-892/1035 restore
 * {@code OffsetOutOfRangeException} without AWS, and shows which store families it
 * lands on.
 *
 * <p>Unlike {@code RestoreLappedByRetentionIntegrationTest}, which forces a single
 * deterministic OOORE via {@code Admin.deleteRecords}, this one reproduces the
 * NATURAL race the soak hits — retention deleting the front of a changelog faster
 * than a from-scratch restore reads it — so OOOREs can be COUNTED and compared.
 *
 * <p>It recreates the four conditions the AWS soak satisfies:
 * <ol>
 *   <li><b>EOS-v2</b>, so an unclean close wipes the task directory
 *       ({@code StateManagerUtil}: {@code !closeClean && eosEnabled}) and the next
 *       re-init has no checkpoint;</li>
 *   <li><b>injected unclean closes</b> — a processor throws periodically and the
 *       handler returns {@code REPLACE_THREAD}, mirroring the soak's hourly
 *       {@code ThreadReplaceableException};</li>
 *   <li><b>tight changelog retention</b> — {@code cleanup.policy=compact,delete}
 *       with small {@code retention.bytes}/{@code segment.bytes} and a 1s broker
 *       retention check, so log-start actually advances;</li>
 *   <li><b>a throttled restore</b>, so it can lose the race.</li>
 * </ol>
 *
 * <p>The topology deliberately contains BOTH families, because they behave
 * differently:
 * <ul>
 *   <li>a <b>stream-stream join</b> store — retention ~3s. Once
 *       {@code PlainToHeadersWindowStoreAdapter} reports its retention, #22115
 *       seeks near the head and the restore is ~170 records instead of the whole
 *       log;</li>
 *   <li>a <b>long-retention windowed count</b> ({@code withRetention(4h)}) — its
 *       seek target resolves to log-start regardless, so #22115 cannot help it.</li>
 * </ul>
 *
 * <p>Run it, and read the per-family OOORE counts and seek-decision counts:
 *
 * <pre>
 *   # default 5 minutes
 *   ./gradlew :streams:integration-tests:test --tests '*LocalOooreSoakTest' --console=plain -i
 *
 *   # a real soak
 *   ./gradlew :streams:integration-tests:test --tests '*LocalOooreSoakTest' \
 *       --console=plain -i -Dsoak.minutes=120
 * </pre>
 *
 * <p><b>To A/B the adapter fix</b>, run it twice — once as-is, once with the
 * {@code WithRetentionPeriod} change to {@code PlainToHeadersWindowStoreAdapter}
 * reverted — and compare {@code OPTIMISED} and the join-store OOORE count.
 * Without the fix, joins take {@code seekToBeginning} and are lappable; with it
 * they should seek near the head and stop failing.
 */
@Tag("integration")
@Timeout(24 * 60 * 60)
public class LocalOooreSoakTest {

    private static final int PARTITIONS = 2;
    private static final String LEFT = "soak-left";
    private static final String RIGHT = "soak-right";

    /** Tight enough that log-start genuinely advances under load. */
    private static final Map<String, String> TIGHT_CHANGELOG = Map.of(
        // `delete` is required: a compact-only changelog never advances its
        // log-start offset, so it can never lap a restore. Every OOORE on the AWS
        // soak is on a compact,delete changelog.
        "cleanup.policy", "compact,delete",
        // segment.bytes is broker-validated with a 1 MiB MINIMUM -- a smaller value
        // fails topic creation outright ("Value must be at least 1048576"), which
        // silently prevented this soak from starting at all on the first attempt.
        "segment.bytes", "1048576",
        // Keep only ~2 segments so retention deletion is continuous under load.
        "retention.bytes", "2097152",
        "retention.ms", "60000");

    private static EmbeddedKafkaCluster cluster;

    @BeforeAll
    public static void startCluster() throws IOException, InterruptedException {
        final Properties brokerConfig = new Properties();
        // Retention must actually run, and often.
        brokerConfig.put("log.retention.check.interval.ms", "1000");
        brokerConfig.put("log.initial.task.delay.ms", "100");
        cluster = new EmbeddedKafkaCluster(1, brokerConfig);
        cluster.start();
        cluster.createTopic(LEFT, PARTITIONS, 1);
        cluster.createTopic(RIGHT, PARTITIONS, 1);
    }

    @AfterAll
    public static void stopCluster() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    /** Throws periodically so the handler can REPLACE_THREAD -> unclean close -> wipe. */
    private static final class Kaboom<K, V> implements ProcessorSupplier<K, V, K, V> {
        private final AtomicLong seen = new AtomicLong();
        private final long every;

        private Kaboom(final long every) {
            this.every = every;
        }

        @Override
        public org.apache.kafka.streams.processor.api.Processor<K, V, K, V> get() {
            return new ContextualProcessor<>() {
                @Override
                public void process(final Record<K, V> record) {
                    if (seen.incrementAndGet() % every == 0) {
                        throw new IllegalStateException("kaboom: forcing an unclean close");
                    }
                    context().forward(record);
                }
            };
        }
    }

    private static Properties streamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "local-ooore-soak");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempDirectory().getAbsolutePath());
        // EOS-v2 is what makes an unclean close wipe the task directory.
        // SOAK_EOS=off switches to at_least_once. That matters for more than the
        // wipe: under EOS the changelog carries TRANSACTION MARKERS, which occupy
        // offsets but are never returned as records. #22115's probe seeks to
        // endOffset-1 and polls, so if that offset is a marker the poll comes back
        // empty and the optimisation silently falls back to seekToBeginning.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
            "off".equalsIgnoreCase(System.getenv("SOAK_EOS"))
                ? StreamsConfig.AT_LEAST_ONCE : StreamsConfig.EXACTLY_ONCE_V2);
        // Leave transactional state stores OFF: that is the default, and it is the
        // configuration in which the wipe (and therefore the OOORE) is possible.
        props.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, false);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        // Throttle the restore so it can lose the race against retention.
        // SOAK_RESTORE_THROTTLE=off lifts it -- needed to tell a genuine #22115
        // probe-poll fallback apart from this harness starving the probe, since
        // seekNewPartitions' probe uses the SAME restore consumer.
        if (!"off".equalsIgnoreCase(System.getenv("SOAK_RESTORE_THROTTLE"))) {
            props.put(StreamsConfig.restoreConsumerPrefix("max.partition.fetch.bytes"), 4096);
            props.put(StreamsConfig.restoreConsumerPrefix("fetch.max.bytes"), 8192);
            props.put(StreamsConfig.restoreConsumerPrefix("max.poll.records"), 20);
        }
        // poll.ms IS the probe poll's timeout in seekNewPartitions (pollTime).
        // Default 100ms may simply be too short for the first fetch after a seek.
        final String pollMs = System.getenv("SOAK_POLL_MS");
        if (pollMs != null) {
            props.put(StreamsConfig.POLL_MS_CONFIG, Long.parseLong(pollMs));
        }
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }

    private static StreamsBuilder topology() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Long> left =
            builder.stream(LEFT, Consumed.with(Serdes.String(), Serdes.Long()));
        final KStream<String, Long> right =
            builder.stream(RIGHT, Consumed.with(Serdes.String(), Serdes.Long()));

        // (a) stream-stream join -> ~3s retention window stores (JOINTHIS/JOINOTHER).
        //     These are the stores the adapter fix unblocks.
        left.process(new Kaboom<>(20_000))
            .join(right, (l, r) -> l + r,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(1000), Duration.ofMillis(1000)),
                StreamJoined.with(Serdes.String(), Serdes.Long(), Serdes.Long()))
            .foreach((k, v) -> { });

        // (b) long-retention windowed count -> seek resolves to log-start anyway,
        //     so #22115 cannot help it even when the gate passes.
        left.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("long-retention-count")
                .withLoggingEnabled(TIGHT_CHANGELOG)
                .withRetention(Duration.ofHours(4)))
            .toStream()
            .foreach((k, v) -> { });

        return builder;
    }

    private static Thread datagen(final AtomicBoolean running, final AtomicLong produced) {
        final Thread t = new Thread(() -> {
            final Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
            try (final Producer<String, Long> producer = new KafkaProducer<>(props)) {
                long i = 0;
                while (running.get()) {
                    final String key = "k" + (i % 5_000);
                    // Count ACKS, not send() calls. Counting sends reported 1.6
                    // BILLION "produced" in 4 minutes on the first attempt, because
                    // send() only buffers.
                    producer.send(new ProducerRecord<>(LEFT, key, i), (m, e) -> {
                        if (e == null) {
                            produced.incrementAndGet();
                        }
                    });
                    producer.send(new ProducerRecord<>(RIGHT, key, i), (m, e) -> {
                        if (e == null) {
                            produced.incrementAndGet();
                        }
                    });
                    i++;
                    if (i % 500 == 0) {
                        producer.flush();
                        // Rate limit: unthrottled, this spins the broker into the
                        // ground and starves the restore we are trying to observe.
                        Thread.sleep(2);
                    }
                }
                producer.flush();
            } catch (final Exception e) {
                if (running.get()) {
                    throw new RuntimeException(e);
                }
            }
        }, "soak-datagen");
        t.setDaemon(true);
        return t;
    }

    private static long count(final LogCaptureAppender appender, final String needle) {
        return appender.getMessages().stream().filter(m -> m.contains(needle)).count();
    }

    private static long countOooreFor(final LogCaptureAppender appender, final String storeMarker) {
        return appender.getMessages().stream()
            .filter(m -> m.contains("OffsetOutOfRangeException") && m.contains(storeMarker))
            .count();
    }

    @Test
    public void soak() throws Exception {
        // Gradle does not forward -Dsoak.minutes to the test JVM, so SOAK_MINUTES
        // (an env var, which Test tasks DO inherit) is the reliable knob.
        final String env = System.getenv("SOAK_MINUTES");
        final long minutes = env != null ? Long.parseLong(env) : Long.getLong("soak.minutes", 5L);
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicLong produced = new AtomicLong();
        final AtomicLong threadReplacements = new AtomicLong();

        final Thread producer = datagen(running, produced);
        producer.start();

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            // DEBUG on the reader is what exposes WHICH seek each store took --
            // every branch of #22115 logs at debug and is invisible otherwise.
            appender.setClassLogger(StoreChangelogReader.class, Level.DEBUG);
            appender.setClassLogger(ProcessorStateManager.class, Level.DEBUG);

            final KafkaStreams streams = new KafkaStreams(topology().build(), streamsConfig());
            streams.setUncaughtExceptionHandler(e -> {
                threadReplacements.incrementAndGet();
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            streams.start();

            final long deadline = System.currentTimeMillis() + Duration.ofMinutes(minutes).toMillis();
            long nextReport = System.currentTimeMillis();
            try {
                while (System.currentTimeMillis() < deadline) {
                    if (System.currentTimeMillis() >= nextReport) {
                        report(appender, produced, threadReplacements, minutes, deadline);
                        nextReport = System.currentTimeMillis() + 30_000;
                    }
                    Thread.sleep(500);
                }
            } finally {
                running.set(false);
                streams.close(Duration.ofSeconds(30));
                producer.join(Duration.ofSeconds(10).toMillis());
            }

            System.out.println("\n================ FINAL ================");
            report(appender, produced, threadReplacements, minutes, deadline);
            System.out.println("\nChangelog tail shape (is endOffset-1 a transaction marker?):");
            probeChangelogTail("local-ooore-soak-long-retention-count-changelog");

            System.out.println("\nPer-store-family OOORE:");
            for (final String marker : List.of("JOINTHIS", "JOINOTHER", "long-retention-count")) {
                System.out.printf("  %-24s %d%n", marker, countOooreFor(appender, marker));
            }
        } finally {
            try {
                IntegrationTestUtils.purgeLocalStreamsState(streamsConfig());
            } catch (final Exception ignored) {
                // best effort
            }
        }
    }

    /**
     * Replicates EXACTLY what {@code seekNewPartitions} does to a changelog
     * partition -- seek to {@code endOffset - 1}, poll -- and reports whether any
     * record comes back, plus where the last real DATA record actually sits.
     *
     * <p>This is an observation, not a config change: the app stays on EOS-v2 like
     * the soak. Under EOS the changelog carries transaction commit markers, which
     * consume offsets but are never returned to a consumer. If {@code endOffset-1}
     * is a marker, the probe poll returns nothing and #22115 silently falls back
     * to {@code seekToBeginning}. A non-zero gap below is that, measured.
     */
    private static void probeChangelogTail(final String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tail-probe-" + System.nanoTime());

        try (final Consumer<byte[], byte[]> c = new KafkaConsumer<>(props)) {
            for (final PartitionInfo pi : c.partitionsFor(topic)) {
                final TopicPartition tp = new TopicPartition(topic, pi.partition());
                c.assign(List.of(tp));
                final long end = c.endOffsets(List.of(tp)).get(tp);
                final long begin = c.beginningOffsets(List.of(tp)).get(tp);
                if (end <= begin) {
                    System.out.printf("   tail-probe %s-%d: empty (begin=%d end=%d)%n",
                        topic, pi.partition(), begin, end);
                    continue;
                }

                // 1. exactly what #22115 does
                c.seek(tp, end - 1);
                final ConsumerRecords<byte[], byte[]> atTail = c.poll(Duration.ofSeconds(2));
                final int gotAtTail = atTail.records(tp).size();

                // 2. where is the last DATA record really?
                long lastData = -1;
                final long scanFrom = Math.max(begin, end - 200);
                c.seek(tp, scanFrom);
                final long deadline = System.currentTimeMillis() + 4000;
                while (System.currentTimeMillis() < deadline) {
                    final ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(500));
                    for (final ConsumerRecord<byte[], byte[]> r : recs.records(tp)) {
                        lastData = Math.max(lastData, r.offset());
                    }
                    if (c.position(tp) >= end) {
                        break;
                    }
                }
                System.out.printf(
                    "   tail-probe %s-%d: begin=%d end=%d  seek(end-1)->records=%d  "
                        + "lastDataOffset=%d  gap(end-1-lastData)=%s%n",
                    topic, pi.partition(), begin, end, gotAtTail, lastData,
                    lastData < 0 ? "?" : String.valueOf((end - 1) - lastData));
            }
        } catch (final Exception e) {
            System.out.println("   tail-probe failed: " + e);
        }
    }

    private static void report(final LogCaptureAppender appender,
                              final AtomicLong produced,
                              final AtomicLong threadReplacements,
                              final long minutes,
                              final long deadline) {
        final long remaining = Math.max(0, (deadline - System.currentTimeMillis()) / 1000);
        System.out.printf(
            "%n[soak %dmin, %ds left] produced=%,d threadReplacements=%d%n"
                + "   OOORE=%d  noCheckpoint=%d  fromCheckpoint=%d%n"
                + "   seek: OPTIMISED=%d  toBeginning(nonFinite)=%d  toBeginning(fallback)=%d%n",
            minutes, remaining, produced.get(), threadReplacements.get(),
            count(appender, "OffsetOutOfRangeException"),
            count(appender, "did not find checkpoint offset"),
            count(appender, "initialized from checkpoint with offset"),
            count(appender, "from stream-time-based timestamp"),
            count(appender, "since we cannot find current offset"),
            count(appender, "from the beginning."));
    }
}
