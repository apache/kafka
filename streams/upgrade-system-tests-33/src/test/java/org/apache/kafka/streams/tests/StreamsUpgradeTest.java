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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.tests.SmokeTestUtil.intSerde;
import static org.apache.kafka.streams.tests.SmokeTestUtil.longSerde;
import static org.apache.kafka.streams.tests.SmokeTestUtil.stringSerde;


public class StreamsUpgradeTest {

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeTest requires one argument (properties-file) but provided none");
        }
        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started (StreamsUpgradeTest v3.3)");
        System.out.println("props=" + streamsProperties);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Integer> dataTable = builder.table(
            "data", Consumed.with(stringSerde, intSerde));
        final KStream<String, Integer> dataStream = dataTable.toStream();
        dataStream.process(printProcessorSupplier("data"));
        dataStream.to("echo");

        final boolean runFkJoin = Boolean.parseBoolean(streamsProperties.getProperty(
            "test.run_fk_join",
            "false"));
        final boolean runTableAgg = Boolean.parseBoolean(streamsProperties.getProperty(
                "test.run_table_agg",
                "false"));
        if (runFkJoin) {
            try {
                final KTable<Integer, String> fkTable = builder.table(
                    "fk", Consumed.with(intSerde, stringSerde));
                buildFKTable(dataStream, fkTable);
            } catch (final Exception e) {
                System.err.println("Caught " + e.getMessage());
            }
        }
        if (runTableAgg) {
            final String aggProduceValue = streamsProperties.getProperty("test.agg_produce_value", "");
            if (aggProduceValue.isEmpty()) {
                System.err.printf("'%s' must be specified when '%s' is true.", "test.agg_produce_value", "test.run_table_agg");
            }
            final String expectedAggValuesStr = streamsProperties.getProperty("test.expected_agg_values", "");
            if (expectedAggValuesStr.isEmpty()) {
                System.err.printf("'%s' must be specified when '%s' is true.", "test.expected_agg_values", "test.run_table_agg");
            }
            final List<String> expectedAggValues = Arrays.asList(expectedAggValuesStr.split(","));

            try {
                buildTableAgg(dataTable, aggProduceValue, expectedAggValues);
            } catch (final Exception e) {
                System.err.println("Caught " + e.getMessage());
            }
        }

        final Properties config = new Properties();
        config.setProperty(
            StreamsConfig.APPLICATION_ID_CONFIG,
            "StreamsUpgradeTest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.putAll(streamsProperties);

        // Do not use try-with-resources here; otherwise KafkaStreams will be closed when `main()` exits
        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            System.out.println("UPGRADE-TEST-CLIENT-CLOSED");
            System.out.flush();
        }));
    }

    private static void buildFKTable(final KStream<String, Integer> primaryTable,
                                     final KTable<Integer, String> otherTable) {
        final KStream<String, String> kStream = primaryTable.toTable()
            .join(otherTable, v -> v, (k0, v0) -> v0)
            .toStream();
        kStream.process(printProcessorSupplier("fk"));
        kStream.to("fk-result", Produced.with(stringSerde, stringSerde));
    }

    private static void buildTableAgg(final KTable<String, Integer> sourceTable,
                                      final String aggProduceValue,
                                      final List<String> expectedAggValues) {
        final KStream<Integer, String> result = sourceTable
            .groupBy(
                (k, v) -> new KeyValue<>(v, aggProduceValue),
                Grouped.with(intSerde, stringSerde))
            .aggregate(
                () -> new Agg(Collections.emptyList(), 0),
                (k, v, agg) -> {
                    final List<String> seenValues;
                    final boolean updated;
                    if (!agg.seenValues.contains(v)) {
                        seenValues = new ArrayList<>(agg.seenValues);
                        seenValues.add(v);
                        Collections.sort(seenValues);
                        updated = true;
                    } else {
                        seenValues = agg.seenValues;
                        updated = false;
                    }

                    final boolean shouldLog = updated || (agg.recordsProcessed % 10 == 0); // value of 10 is chosen for debugging purposes. can increase to 100 once test is passing.
                    if (shouldLog) {
                        if (seenValues.containsAll(expectedAggValues)) {
                            System.out.printf("Table aggregate processor saw expected values. Seen: %s. Expected: %s%n", String.join(",", seenValues), String.join(",", expectedAggValues));
                        } else {
                            System.out.printf("Table aggregate processor did not see expected values. Seen: %s. Expected: %s%n", String.join(",", seenValues), String.join(",", expectedAggValues)); // this line for debugging purposes only.
                        }
                    }

                    return new Agg(seenValues, agg.recordsProcessed + 1);
                },
                (k, v, agg) -> agg,
                Materialized.with(intSerde, new AggSerde()))
            .mapValues((k, vAgg) -> String.join(",", vAgg.seenValues))
            .toStream();

        // adding dummy processor for better debugging (will print assigned tasks)
        result.process(SmokeTestUtil.printTaskProcessorSupplier("table-repartition"));
        result.to("table-agg-result", Produced.with(intSerde, stringSerde));
    }

    private static class Agg {
        private final List<String> seenValues;
        private final long recordsProcessed;

        Agg(final List<String> seenValues, final long recordsProcessed)  {
            this.seenValues = seenValues;
            this.recordsProcessed = recordsProcessed;
        }

        byte[] serialize() {
            final byte[] rawSeenValuees = stringSerde.serializer().serialize("", String.join(",", seenValues));
            final byte[] rawRecordsProcessed = longSerde.serializer().serialize("", recordsProcessed);
            return ByteBuffer
                    .allocate(rawSeenValuees.length + rawRecordsProcessed.length)
                    .put(rawSeenValuees)
                    .put(rawRecordsProcessed)
                    .array();
        }

        static Agg deserialize(final byte[] rawAgg) {
            final byte[] rawSeenValues = new byte[rawAgg.length - 8];
            System.arraycopy(rawAgg, 0, rawSeenValues, 0, rawSeenValues.length);
            final List<String> seenValues = Arrays.asList(stringSerde.deserializer().deserialize("", rawSeenValues).split(","));

            final byte[] rawRecordsProcessed = new byte[8];
            System.arraycopy(rawAgg, rawAgg.length - 8, rawRecordsProcessed, 0, 8);
            final long recordsProcessed = longSerde.deserializer().deserialize("", rawRecordsProcessed);

            return new Agg(seenValues, recordsProcessed);
        }
    }

    private static class AggSerde implements Serde<Agg> {

        @Override
        public Serializer<Agg> serializer() {
            return (topic, agg) -> agg.serialize();
        }

        @Override
        public Deserializer<Agg> deserializer() {
            return (topic, rawAgg) -> Agg.deserialize(rawAgg);
        }
    }

    private static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> printProcessorSupplier(final String topic) {
        return () -> new ContextualProcessor<KIn, VIn, KOut, VOut>() {
            private int numRecordsProcessed = 0;

            @Override
            public void init(final ProcessorContext<KOut, VOut> context) {
                System.out.println("[3.3] initializing processor: topic=" + topic + "taskId=" + context.taskId());
                numRecordsProcessed = 0;
            }

            @Override
            public void process(final Record<KIn, VIn> record) {
                numRecordsProcessed++;
                if (numRecordsProcessed % 100 == 0) {
                    System.out.println("processed " + numRecordsProcessed + " records from topic=" + topic);
                }
            }

            @Override
            public void close() {}
        };
    }
}
