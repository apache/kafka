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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyTestDriverBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario tests for record-header propagation through {@code suppress()}.
 *
 * <p>The suppress buffer holds a row per key containing an {@code old} and a {@code new} value,
 * plus a single record context. Each of those parts originates from a
 * <em>different</em> input record, so each must be serialized and deserialized with the headers of
 * the record it came from. Getting this wrong is invisible with header-agnostic serdes such as
 * {@link Serdes#String()}, because they discard the headers they are handed. These tests therefore
 * use serdes that observe every invocation, and assert on what the serdes actually saw rather than
 * only on what reached the output topic.
 *
 * <p>Each input record carries the headers it expects to see preserved, as {@code origin=<value>},
 * so every value should always meet its own marker; see {@link #observingSerde}.
 *
 * <h3>Invariants asserted</h3>
 * <ul>
 *   <li><b>INV-1</b> — every value (de)serialized by the suppress buffer must be handed the headers
 *       belonging to that value, i.e. {@code origin=<value>}.</li>
 *   <li><b>INV-2</b> — the record emitted on eviction must carry the headers belonging to the value
 *       being emitted.</li>
 *   <li><b>INV-3</b> — reported, not enforced: which headers the suppress buffer hands to the
 *       <em>key</em> serde. The key is shared by all parts of a row, so there is no single "correct"
 *       answer to pin down yet; the observations are printed so the behaviour is visible.</li>
 * </ul>
 *
 * <p>All checks are collected and reported together: a scenario runs to completion, prints a full
 * trace plus a check summary to stdout, and only then makes a single assertion. That keeps the whole
 * picture visible instead of stopping at the first deviation.
 *
 * <h3>Scenario coverage</h3>
 *
 * The buffer never sees {@code Windowed}: it stores serialized key bytes, and windowing reaches
 * {@code suppress()} only through the time definition and through {@code safeToDropTombstones},
 * neither of which affects header handling. Windowed and non-windowed scenarios therefore drive the
 * same code, and the matrix below is deliberately not filled in exhaustively.
 *
 * <p>In particular there is <b>no</b> windowed / eviction-triggered-by-a-different-key scenario. That
 * is omitted on purpose rather than missing: it would drive the same buffer path as
 * {@link #nonWindowedEvictionTriggeredByDifferentKey()} with nothing added. The windowed scenarios
 * that are here earn their place either by recording row-identity semantics that are easy to get
 * wrong, or by being reachable only when windowed.
 *
 * <h3>Why the same-row scenarios require the HEADERS store format</h3>
 *
 * When the evicting record updates a row that is already buffered, that row ends up holding an
 * {@code old} and a {@code new} value originating from two different records, so INV-1 requires two
 * different sets of headers to be recoverable from one row. The plain format has nowhere to put
 * them: it stores bare values and a single record context, so at most one of the two can be right.
 * INV-1 is therefore unsatisfiable in plain format for these scenarios, no matter how
 * {@code suppress()} is implemented.
 *
 * <p>{@link #nonWindowedEvictionTriggeredBySameKey()} and
 * {@link #windowedEvictionTriggeredBySameKeySameWindow()} consequently set
 * {@code dsl.store.format=headers}, where each value part carries its own headers. Running them in
 * plain format would assert something impossible and read as a permanent bug.
 *
 * <p>Scenarios where the evicted row is <em>not</em> the one being updated have only one origin per
 * row, so they are satisfiable in plain format and deliberately stay there.
 */
public class SuppressHeadersScenarioTest {

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    /** Explicit suppress name so the buffer's changelog topic is predictable and can be filtered on. */
    private static final String SUPPRESS_NAME = "suppress-under-test";
    private static final String SUPPRESS_STORE_TOPIC_PART = SUPPRESS_NAME + "-store";

    private static final String ORIGIN_HEADER = "origin";

    /** Input value whose aggregation result is {@code null}, used to stage a tombstone downstream. */
    private static final String DELETE_SENTINEL = "DELETE";

    private final List<Event> events = new ArrayList<>();
    private final Checks checks = new Checks();
    private String phase = "init";
    private String scenario = "";
    private Properties config;

    @BeforeEach
    public void setup() {
        events.clear();
        checks.clear();
        config = new Properties();
        config.setProperty(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
    }

    @AfterEach
    public void report() {
        System.out.println();
        System.out.println("================ SCENARIO: " + scenario + " ================");
        System.out.println(renderTrace());
        System.out.println("---- checks ----");
        System.out.println(checks.render());
        System.out.println("================ END: " + scenario + " ================");
        System.out.println();
    }

    // ------------------------------------------------------------------ scenarios

    @Disabled("Enabled by the fix for KAFKA-20413; see class javadoc")
    @Test
    public void nonWindowedEvictionTriggeredByDifferentKey() {
        scenario = "non-windowed / eviction triggered by a DIFFERENT key";
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(nonWindowedTopology()).withConfig(config).build()) {
            final TestInputTopic<String, String> input =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            pipe(input, "kA", "v1", 0L);
            // Advancing stream time past kA's suppress deadline. kA's row is untouched by this
            // record, so it is evicted exactly as it was buffered.
            pipe(input, "kB", "v2", 120_000L);

            verifyEmitted(driver, OUTPUT_TOPIC, "v1");
        }
        finish();
    }

    /**
     * Runs in HEADERS store format only; see the note on same-row scenarios in the class javadoc.
     * The row holds {@code new=v2 / old=v1}, each from a different input record, which plain values
     * cannot represent.
     */
    @Disabled("Enabled by the fix for KAFKA-20413; see class javadoc")
    @Test
    public void nonWindowedEvictionTriggeredBySameKey() {
        scenario = "non-windowed / SAME key, row updated then evicted (headers format)";
        config.setProperty(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(nonWindowedTopology()).withConfig(config).build()) {
            final TestInputTopic<String, String> input =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            pipe(input, "kA", "v1", 0L);
            pipe(input, "kA", "v2", 120_000L);

            verifyEmitted(driver, OUTPUT_TOPIC, "v2");
        }
        finish();
    }

    /**
     * A delete of a buffered row. {@code untilTimeLimit} reports {@code safeToDropTombstones=false},
     * so the tombstone must reach the output: dropping it would leave downstream state undeleted.
     *
     * <p>This is also the sharpest header case in this class. There is no value left to carry
     * anything, so the record's headers are all that remains, and they still have to be correct for
     * the key's sake.
     *
     * <p>Not a duplicate of {@link #nonWindowedEvictionTriggeredBySameKey()}, despite also being a
     * same-row overwrite: it is the only scenario that fails INV-2. There, the new value is the last
     * part serialized, so the headers left behind on the shared object happen to match what is
     * emitted; with a tombstone only the <em>old</em> value is serialized, which makes the leak
     * visible on the emitted record.
     */
    @Disabled("Enabled by the fix for KAFKA-20413; see class javadoc")
    @Test
    public void nonWindowedTombstoneMustBeForwarded() {
        scenario = "non-windowed / buffered row is DELETED, tombstone must be forwarded";
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(nonWindowedTopology()).withConfig(config).build()) {
            final TestInputTopic<String, String> input =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            pipe(input, "kA", "v1", 0L);
            // A null value deletes the table row. It lands on the same suppress row, so it overwrites
            // the buffered value and is then evicted as Change(new=null, old=v1).
            pipe(input, "kA", null, 120_000L);

            verifyEmitted(driver, OUTPUT_TOPIC, null);
        }
        finish();
    }

    /**
     * Drives the same buffer path as {@link #nonWindowedEvictionTriggeredByDifferentKey()} — the
     * buffer never sees {@code Windowed}, only serialized key bytes — and is kept for the semantics
     * it records: a <em>same</em> input key still produces a <em>different</em> suppress row once
     * windowed, so the evicted row is not the one the arriving record updated.
     */
    @Disabled("Enabled by the fix for KAFKA-20413; see class javadoc")
    @Test
    public void windowedEvictionTriggeredBySameKeyDifferentWindow() {
        scenario = "windowed (untilWindowCloses) / SAME key but a DIFFERENT window";
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(windowedTopology(false)).withConfig(config).build()) {
            final TestInputTopic<String, String> input =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            pipe(input, "kA", "v1", 0L);
            // Same input key, but a far-future window, so this is a different suppress row.
            pipe(input, "kA", "v2", 120_000L);

            verifyEmitted(driver, OUTPUT_TOPIC, "v1");
        }
        finish();
    }

    /**
     * Drives the same buffer path as {@link #nonWindowedEvictionTriggeredBySameKey()}, and is kept
     * for the semantics it records: same-row eviction <em>is</em> reachable while windowed, but only
     * under {@code untilTimeLimit}. It cannot happen under {@code untilWindowCloses}, where being
     * inside a window excludes being past its close.
     */
    @Disabled("Enabled by the fix for KAFKA-20413; see class javadoc")
    @Test
    public void windowedEvictionTriggeredBySameKeySameWindow() {
        scenario = "windowed (untilTimeLimit) / SAME key and SAME window (headers format)";
        config.setProperty(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(windowedTopology(true)).withConfig(config).build()) {
            final TestInputTopic<String, String> input =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            pipe(input, "kA", "v1", 0L);
            // A one-hour window with a one-second time limit: both records land in the same window,
            // so this is the same suppress row, updated and then evicted. Not reachable with
            // untilWindowCloses, where being inside a window excludes being past its close.
            pipe(input, "kA", "v2", 5_000L);

            verifyEmitted(driver, OUTPUT_TOPIC, "v2");
        }
        finish();
    }

    /**
     * The mirror image of {@link #nonWindowedTombstoneMustBeForwarded()}. In final-results mode
     * {@code safeToDropTombstones} is {@code true}, so the tombstone is deliberately swallowed: for a
     * windowed key that never emitted a result there is nothing downstream to delete, and an
     * unnecessary tombstone in the output is undesirable (see {@code SuppressedInternal}'s javadoc).
     *
     * <p>Pinned deliberately, so the swallow is not later "fixed" as a leak.
     *
     * <p>A null value cannot be piped into an aggregation, so the tombstone is staged by having the
     * reducer return {@code null} for the sentinel value {@code DELETE}.
     */
    @Disabled("Enabled by the fix for KAFKA-20413; see class javadoc")
    @Test
    public void windowedFinalResultsTombstoneIsDropped() {
        scenario = "windowed (untilWindowCloses) / tombstone is dropped by design";
        try (final TopologyTestDriver driver = new TopologyTestDriverBuilder(windowedTopology(false)).withConfig(config).build()) {
            final TestInputTopic<String, String> input =
                driver.createInputTopic(INPUT_TOPIC, new StringSerializer(), new StringSerializer());

            pipe(input, "kA", "v1", 0L);
            // Same window: the aggregate becomes null, so the buffered row becomes a tombstone.
            pipe(input, "kA", DELETE_SENTINEL, 1L);
            // Advance stream time so kA's window closes and its row is evicted.
            pipe(input, "kB", "v2", 120_000L);

            verifyNothingEmitted(driver, OUTPUT_TOPIC);
        }
        finish();
    }

    // ------------------------------------------------------------------ topologies

    private Topology nonWindowedTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder
            .table(
                INPUT_TOPIC,
                Consumed.with(observingSerde("key"), observingSerde("value")),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table-store")
                    .withKeySerde(observingSerde("key"))
                    .withValueSerde(observingSerde("value"))
                    .withCachingDisabled()
                    .withLoggingDisabled())
            .suppress(untilTimeLimit(Duration.ofSeconds(1), unbounded()).withName(SUPPRESS_NAME))
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private Topology windowedTopology(final boolean useTimeLimit) {
        final StreamsBuilder builder = new StreamsBuilder();

        // reduce((old, latest) -> latest) keeps the aggregate value equal to the latest input value,
        // so each aggregate value still identifies the record it came from, unlike count().
        final KTable<Windowed<String>, String> table = builder
            .stream(INPUT_TOPIC, Consumed.with(observingSerde("key"), observingSerde("value")))
            .groupByKey(Grouped.with(observingSerde("key"), observingSerde("value")))
            .windowedBy(useTimeLimit
                ? TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1))
                : TimeWindows.ofSizeAndGrace(ofMillis(2L), ofMillis(1L)))
            .reduce(
                (oldValue, latest) -> DELETE_SENTINEL.equals(latest) ? null : latest,
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("windowed-store")
                    .withKeySerde(observingSerde("key"))
                    .withValueSerde(observingSerde("value"))
                    .withCachingDisabled());

        final KTable<Windowed<String>, String> suppressed = useTimeLimit
            ? table.suppress(untilTimeLimit(Duration.ofSeconds(1), unbounded()).withName(SUPPRESS_NAME))
            : table.suppress(untilWindowCloses(unbounded()).withName(SUPPRESS_NAME));

        suppressed
            .toStream()
            .map((final Windowed<String> k, final String v) -> new KeyValue<>(k.key(), v))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // ------------------------------------------------------------------ serdes

    /**
     * Serde that only observes: it records every invocation and the {@link Headers} instance it
     * was handed, and never modifies them. Assertions are then made against what the buffer actually
     * passed in, on both the serialize and the deserialize side — a serde that wrote headers of its
     * own would overwrite the evidence and hide every serialize-side defect.
     *
     * <p>Headers reach the buffer the ordinary way: {@code pipe(...)} puts {@code origin=<value>} on
     * the input record, and the record context carries it into {@code suppress()}. Writes to the
     * upstream table store appear in the trace with <em>empty</em> headers, because the DSL does not
     * propagate record headers into a materialized store. That is expected and does not affect these
     * scenarios: {@code KTableSource} discards the store's headers when it builds the {@code Change},
     * so the buffer never reads that path.
     *
     * @param role {@code "key"} or {@code "value"}, recorded with each observation so the trace and
     *             the invariants can tell the two apart
     */
    private Serde<String> observingSerde(final String role) {
        return new Serde<>() {
            @Override
            public Serializer<String> serializer() {
                return new Serializer<>() {
                    @Override
                    public byte[] serialize(final String topic, final String data) {
                        record(role, "SER(no-headers-overload)", topic, data, null);
                        return raw(data);
                    }

                    @Override
                    public byte[] serialize(final String topic, final Headers headers, final String data) {
                        record(role, "SER", topic, data, headers);
                        return raw(data);
                    }
                };
            }

            @Override
            public Deserializer<String> deserializer() {
                return new Deserializer<>() {
                    @Override
                    public String deserialize(final String topic, final byte[] data) {
                        final String value = str(data);
                        record(role, "DESER(no-headers-overload)", topic, value, null);
                        return value;
                    }

                    @Override
                    public String deserialize(final String topic, final Headers headers, final byte[] data) {
                        final String value = str(data);
                        record(role, "DESER", topic, value, headers);
                        return value;
                    }
                };
            }
        };
    }

    private static byte[] raw(final String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(final byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ driving and checking

    private void pipe(final TestInputTopic<String, String> topic,
                      final String key,
                      final String value,
                      final long timestamp) {
        phase = String.format(Locale.ROOT, "%s=%s@%d", key, value, timestamp);
        topic.pipeInput(new TestRecord<>(key, value, expectedHeadersFor(value), timestamp));
    }

    /**
     * The origin marker for {@code value}. A tombstone gets its own marker rather than none, because
     * the record's headers still have to travel with the key.
     */
    private static String originOf(final String value) {
        return value == null ? "<tombstone>" : value;
    }

    /** The headers that belong to {@code value}: what every (de)serialization of it should see. */
    private static Headers expectedHeadersFor(final String value) {
        return new RecordHeaders().add(ORIGIN_HEADER, originOf(value).getBytes(StandardCharsets.UTF_8));
    }

    private void verifyEmitted(final TopologyTestDriver driver,
                               final String topic,
                               final String expectedValue) {
        final List<TestRecord<String, String>> output = driver
            .createOutputTopic(topic, new StringDeserializer(), new StringDeserializer())
            .readRecordsToList();

        checks.record("exactly one record is emitted", 1, output.size());
        if (output.isEmpty()) {
            return;
        }
        final TestRecord<String, String> emitted = output.get(0);
        checks.record("emitted value", expectedValue, emitted.value());
        // INV-2: the emitted record carries the headers of the value being emitted.
        checks.record(
            "INV-2 emitted headers match the emitted value's origin",
            render(expectedHeadersFor(emitted.value())),
            render(emitted.headers()));
    }

    private void verifyNothingEmitted(final TopologyTestDriver driver, final String topic) {
        final List<TestRecord<String, String>> output = driver
            .createOutputTopic(topic, new StringDeserializer(), new StringDeserializer())
            .readRecordsToList();
        checks.record("no record is emitted", 0, output.size());
    }

    /** Applies INV-1 and INV-3 over the captured trace, then makes the single assertion. */
    private void finish() {
        for (final Event e : events) {
            if (!e.insideSuppressBuffer()) {
                continue;
            }
            if ("value".equals(e.role) && e.headers != null) {
                // INV-1: a value must always meet the headers it was written with. Null values are
                // included: a tombstone's headers still have to be correct for the key's sake.
                checks.record(
                    "INV-1 " + e.op + " of '" + originOf(e.value) + "' during [" + e.phase + "]",
                    render(expectedHeadersFor(e.value)),
                    e.headers);
            } else if ("key".equals(e.role)) {
                checks.observe("INV-3 key " + e.op + " during [" + e.phase + "] saw " + e.headers);
            }
        }
        assertTrue(checks.allPassed(), "\n" + renderTrace() + "\n" + checks.render());
    }

    private String renderTrace() {
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ROOT, "%-22s %-5s %-28s %-8s %s%n",
            "PHASE", "ROLE", "OP", "VALUE", "HEADERS / topic"));
        for (final Event e : events) {
            sb.append(e).append(System.lineSeparator());
        }
        return sb.toString();
    }

    private void record(final String role,
                        final String op,
                        final String topic,
                        final String value,
                        final Headers headers) {
        // Headers are mutable and shared, so snapshot them as text immediately.
        events.add(new Event(phase, role, op, topic, value, headers == null ? null : render(headers)));
    }

    private static String render(final Headers headers) {
        if (headers == null) {
            return "<null>";
        }
        final StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (final Header h : headers) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(h.key()).append('=')
              .append(h.value() == null ? "<null>" : new String(h.value(), StandardCharsets.UTF_8));
        }
        return sb.append(']').toString();
    }

    // ------------------------------------------------------------------ small helpers

    private static class Event {
        private final String phase;
        private final String role;
        private final String op;
        private final String topic;
        private final String value;
        private final String headers;

        Event(final String phase, final String role, final String op,
              final String topic, final String value, final String headers) {
            this.phase = phase;
            this.role = role;
            this.op = op;
            this.topic = topic;
            this.value = value;
            this.headers = headers;
        }

        /** True for invocations made by the suppress buffer rather than the upstream table store. */
        boolean insideSuppressBuffer() {
            return topic != null && topic.contains(SUPPRESS_STORE_TOPIC_PART);
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "%-22s %-5s %-28s %-8s %s   (%s%s)",
                phase, role, op, value, headers, topic, insideSuppressBuffer() ? "  <-- SUPPRESS" : "");
        }
    }

    private static final class Checks {
        private final List<String> lines = new ArrayList<>();
        private int failures;

        void clear() {
            lines.clear();
            failures = 0;
        }

        void record(final String description, final Object expected, final Object actual) {
            final boolean ok = Objects.equals(expected, actual);
            if (!ok) {
                failures++;
            }
            lines.add(String.format(Locale.ROOT, "[%s] %s%n        expected: %s%n        actual:   %s",
                ok ? "PASS" : "FAIL", description, expected, actual));
        }

        void observe(final String note) {
            lines.add("[INFO] " + note);
        }

        boolean allPassed() {
            return failures == 0;
        }

        String render() {
            final StringBuilder sb = new StringBuilder();
            for (final String l : lines) {
                sb.append(l).append(System.lineSeparator());
            }
            sb.append(failures == 0
                ? "ALL CHECKS PASSED"
                : failures + " CHECK(S) FAILED");
            return sb.toString();
        }
    }
}
