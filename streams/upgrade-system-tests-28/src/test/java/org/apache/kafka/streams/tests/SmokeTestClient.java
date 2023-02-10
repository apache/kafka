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
package org.apache.kafka.streams.tests;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class SmokeTestClient extends SmokeTestUtil {

    private final String name;

    private KafkaStreams streams;
    private boolean uncaughtException = false;
    private boolean started;
    private volatile boolean closed;

    private static void addShutdownHook(final String name, final Runnable runnable) {
        if (name != null) {
            Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable));
        } else {
            Runtime.getRuntime().addShutdownHook(new Thread(runnable));
        }
    }

    private static File tempDirectory() {
        final String prefix = "kafka-";
        final File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();

        addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (final IOException e) {
                System.out.println("Error deleting " + file.getAbsolutePath());
                e.printStackTrace(System.out);
            }
        });

        return file;
    }

    public SmokeTestClient(final String name) {
        this.name = name;
    }

    public boolean started() {
        return started;
    }

    public boolean closed() {
        return closed;
    }

    public void start(final Properties streamsProperties) {
        final Topology build = getTopology();
        streams = new KafkaStreams(build, getStreamsConfig(streamsProperties));

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            System.out.printf("%s %s: %s -> %s%n", name, Instant.now(), oldState, newState);
            if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.RUNNING) {
                started = true;
                countDownLatch.countDown();
            }

            if (newState == KafkaStreams.State.NOT_RUNNING) {
                closed = true;
            }
        });

        streams.setUncaughtExceptionHandler(e -> {
            System.out.println(name + ": SMOKE-TEST-CLIENT-EXCEPTION");
            System.out.println(name + ": FATAL: An unexpected exception is encountered: " + e);
            e.printStackTrace(System.out);
            uncaughtException = true;
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        addShutdownHook("streams-shutdown-hook", this::close);

        streams.start();
        try {
            if (!countDownLatch.await(1, TimeUnit.MINUTES)) {
                System.out.println(name + ": SMOKE-TEST-CLIENT-EXCEPTION: Didn't start in one minute");
            }
        } catch (final InterruptedException e) {
            System.out.println(name + ": SMOKE-TEST-CLIENT-EXCEPTION: " + e);
            e.printStackTrace(System.out);
        }
        System.out.println(name + ": SMOKE-TEST-CLIENT-STARTED");
        System.out.println(name + " started at " + Instant.now());
    }

    public void closeAsync() {
        streams.close(Duration.ZERO);
    }

    public void close() {
        final boolean closed = streams.close(Duration.ofMinutes(1));

        if (closed && !uncaughtException) {
            System.out.println(name + ": SMOKE-TEST-CLIENT-CLOSED");
        } else if (closed) {
            System.out.println(name + ": SMOKE-TEST-CLIENT-EXCEPTION");
        } else {
            System.out.println(name + ": SMOKE-TEST-CLIENT-EXCEPTION: Didn't close");
        }
    }

    private Properties getStreamsConfig(final Properties props) {
        final Properties fullProps = new Properties(props);
        fullProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "SmokeTest");
        fullProps.put(StreamsConfig.CLIENT_ID_CONFIG, "SmokeTest-" + name);
        fullProps.put(StreamsConfig.STATE_DIR_CONFIG, tempDirectory().getAbsolutePath());
        fullProps.putAll(props);
        return fullProps;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, Integer> stringIntConsumed = Consumed.with(stringSerde, intSerde);
        final KStream<String, Integer> source = builder.stream("data", stringIntConsumed);
        source.filterNot((k, v) -> k.equals("flush"))
              .to("echo", Produced.with(stringSerde, intSerde));
        final KStream<String, Integer> data = source.filter((key, value) -> value == null || value != END);
        data.process(SmokeTestUtil.printProcessorSupplier("data", name));

        // min
        final KGroupedStream<String, Integer> groupedData = data.groupByKey(Grouped.with(stringSerde, intSerde));

        final KTable<Windowed<String>, Integer> minAggregation = groupedData
            .windowedBy(TimeWindows.of(Duration.ofDays(1)).grace(Duration.ofMinutes(1)))
            .aggregate(
                () -> Integer.MAX_VALUE,
                (aggKey, value, aggregate) -> (value < aggregate) ? value : aggregate,
                Materialized
                    .<String, Integer, WindowStore<Bytes, byte[]>>as("uwin-min")
                    .withValueSerde(intSerde)
                    .withRetention(Duration.ofHours(25))
            );

        streamify(minAggregation, "min-raw");

        streamify(minAggregation.suppress(untilWindowCloses(BufferConfig.unbounded())), "min-suppressed");

        minAggregation
            .toStream(new Unwindow<>())
            .filterNot((k, v) -> k.equals("flush"))
            .to("min", Produced.with(stringSerde, intSerde));

        final KTable<Windowed<String>, Integer> smallWindowSum = groupedData
            .windowedBy(TimeWindows.of(Duration.ofSeconds(2)).advanceBy(Duration.ofSeconds(1)).grace(Duration.ofSeconds(30)))
            .reduce((l, r) -> l + r);

        streamify(smallWindowSum, "sws-raw");
        streamify(smallWindowSum.suppress(untilWindowCloses(BufferConfig.unbounded())), "sws-suppressed");

        final KTable<String, Integer> minTable = builder.table(
            "min",
            Consumed.with(stringSerde, intSerde),
            Materialized.as("minStoreName"));

        minTable.toStream().process(SmokeTestUtil.printProcessorSupplier("min", name));

        // max
        groupedData
            .windowedBy(TimeWindows.of(Duration.ofDays(2)))
            .aggregate(
                () -> Integer.MIN_VALUE,
                (aggKey, value, aggregate) -> (value > aggregate) ? value : aggregate,
                Materialized.<String, Integer, WindowStore<Bytes, byte[]>>as("uwin-max").withValueSerde(intSerde))
            .toStream(new Unwindow<>())
            .filterNot((k, v) -> k.equals("flush"))
            .to("max", Produced.with(stringSerde, intSerde));

        final KTable<String, Integer> maxTable = builder.table(
            "max",
            Consumed.with(stringSerde, intSerde),
            Materialized.as("maxStoreName"));
        maxTable.toStream().process(SmokeTestUtil.printProcessorSupplier("max", name));

        // sum
        groupedData
            .windowedBy(TimeWindows.of(Duration.ofDays(2)))
            .aggregate(
                () -> 0L,
                (aggKey, value, aggregate) -> (long) value + aggregate,
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("win-sum").withValueSerde(longSerde))
            .toStream(new Unwindow<>())
            .filterNot((k, v) -> k.equals("flush"))
            .to("sum", Produced.with(stringSerde, longSerde));

        final Consumed<String, Long> stringLongConsumed = Consumed.with(stringSerde, longSerde);
        final KTable<String, Long> sumTable = builder.table("sum", stringLongConsumed);
        sumTable.toStream().process(SmokeTestUtil.printProcessorSupplier("sum", name));

        // cnt
        groupedData
            .windowedBy(TimeWindows.of(Duration.ofDays(2)))
            .count(Materialized.as("uwin-cnt"))
            .toStream(new Unwindow<>())
            .filterNot((k, v) -> k.equals("flush"))
            .to("cnt", Produced.with(stringSerde, longSerde));

        final KTable<String, Long> cntTable = builder.table(
            "cnt",
            Consumed.with(stringSerde, longSerde),
            Materialized.as("cntStoreName"));
        cntTable.toStream().process(SmokeTestUtil.printProcessorSupplier("cnt", name));

        // dif
        maxTable
            .join(
                minTable,
                (value1, value2) -> value1 - value2)
            .toStream()
            .filterNot((k, v) -> k.equals("flush"))
            .to("dif", Produced.with(stringSerde, intSerde));

        // avg
        sumTable
            .join(
                cntTable,
                (value1, value2) -> (double) value1 / (double) value2)
            .toStream()
            .filterNot((k, v) -> k.equals("flush"))
            .to("avg", Produced.with(stringSerde, doubleSerde));

        // test repartition
        final Agg agg = new Agg();
        cntTable.groupBy(agg.selector(), Grouped.with(stringSerde, longSerde))
                .aggregate(agg.init(), agg.adder(), agg.remover(),
                           Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("cntByCnt"))
                               .withKeySerde(Serdes.String())
                               .withValueSerde(Serdes.Long()))
                .toStream()
                .to("tagg", Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    private static void streamify(final KTable<Windowed<String>, Integer> windowedTable, final String topic) {
        windowedTable
            .toStream()
            .filterNot((k, v) -> k.key().equals("flush"))
            .map((key, value) -> new KeyValue<>(key.toString(), value))
            .to(topic, Produced.with(stringSerde, intSerde));
    }
}
