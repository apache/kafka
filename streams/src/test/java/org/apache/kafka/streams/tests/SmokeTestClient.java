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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SmokeTestClient extends SmokeTestUtil {

    private final String kafka;
    private final File stateDir;
    private KafkaStreams streams;
    private Thread thread;
    private boolean uncaughtException = false;

    public SmokeTestClient(final File stateDir, final String kafka) {
        super();
        this.stateDir = stateDir;
        this.kafka = kafka;
    }

    public void start() {
        streams = createKafkaStreams(stateDir, kafka);
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                System.out.println("SMOKE-TEST-CLIENT-EXCEPTION");
                uncaughtException = true;
                e.printStackTrace();
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                close();
            }
        }));

        thread = new Thread() {
            public void run() {
                streams.start();
            }
        };
        thread.start();
    }

    public void close() {
        streams.close(5, TimeUnit.SECONDS);
        // do not remove these printouts since they are needed for health scripts
        if (!uncaughtException) {
            System.out.println("SMOKE-TEST-CLIENT-CLOSED");
        }
        try {
            thread.join();
        } catch (Exception ex) {
            // do not remove these printouts since they are needed for health scripts
            System.out.println("SMOKE-TEST-CLIENT-EXCEPTION");
            // ignore
        }
    }

    private static Properties getStreamsConfig(final File stateDir, final String kafka) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SmokeTest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 100);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //TODO remove this config or set to smaller value when KIP-91 is merged
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 60000);
        return props;
    }

    private static KafkaStreams createKafkaStreams(final File stateDir, final String kafka) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Consumed<String, Integer> stringIntConsumed = Consumed.with(stringSerde, intSerde);
        final KStream<String, Integer> source = builder.stream("data", stringIntConsumed);
        source.to("echo", Produced.with(stringSerde, intSerde));
        final KStream<String, Integer> data = source.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(final String key, final Integer value) {
                return value == null || value != END;
            }
        });
        data.process(SmokeTestUtil.<String, Integer>printProcessorSupplier("data"));

        // min
        final KGroupedStream<String, Integer> groupedData =
            data.groupByKey(Serialized.with(stringSerde, intSerde));

        groupedData
            .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(1)))
            .aggregate(
                new Initializer<Integer>() {
                    public Integer apply() {
                        return Integer.MAX_VALUE;
                    }
                },
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(final String aggKey, final Integer value, final Integer aggregate) {
                        return (value < aggregate) ? value : aggregate;
                    }
                },
                Materialized.<String, Integer, WindowStore<Bytes, byte[]>>as("uwin-min").withValueSerde(intSerde))
            .toStream(new Unwindow<String, Integer>())
            .to("min", Produced.with(stringSerde, intSerde));

        final KTable<String, Integer> minTable = builder.table(
            "min",
            Consumed.with(stringSerde, intSerde),
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("minStoreName"));
        minTable.toStream().process(SmokeTestUtil.<String, Integer>printProcessorSupplier("min"));

        // max
        groupedData
            .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(2)))
            .aggregate(
                new Initializer<Integer>() {
                    public Integer apply() {
                        return Integer.MIN_VALUE;
                    }
                },
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(final String aggKey, final Integer value, final Integer aggregate) {
                        return (value > aggregate) ? value : aggregate;
                    }
                },
                Materialized.<String, Integer, WindowStore<Bytes, byte[]>>as("uwin-max").withValueSerde(intSerde))
            .toStream(new Unwindow<String, Integer>())
            .to("max", Produced.with(stringSerde, intSerde));

        final KTable<String, Integer> maxTable = builder.table(
            "max",
            Consumed.with(stringSerde, intSerde),
            Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("maxStoreName"));
        maxTable.toStream().process(SmokeTestUtil.<String, Integer>printProcessorSupplier("max"));

        // sum
        groupedData
            .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(2)))
            .aggregate(
                new Initializer<Long>() {
                    public Long apply() {
                        return 0L;
                    }
                },
                new Aggregator<String, Integer, Long>() {
                    @Override
                    public Long apply(final String aggKey, final Integer value, final Long aggregate) {
                        return (long) value + aggregate;
                    }
                },
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("win-sum").withValueSerde(longSerde))
            .toStream(new Unwindow<String, Long>())
            .to("sum", Produced.with(stringSerde, longSerde));

        final Consumed<String, Long> stringLongConsumed = Consumed.with(stringSerde, longSerde);
        final KTable<String, Long> sumTable = builder.table("sum", stringLongConsumed);
        sumTable.toStream().process(SmokeTestUtil.printProcessorSupplier("sum"));

        // cnt
        groupedData
            .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(2)))
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("uwin-cnt"))
            .toStream(new Unwindow<String, Long>())
            .to("cnt", Produced.with(stringSerde, longSerde));

        final KTable<String, Long> cntTable = builder.table(
            "cnt",
            Consumed.with(stringSerde, longSerde),
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("cntStoreName"));
        cntTable.toStream().process(SmokeTestUtil.<String, Long>printProcessorSupplier("cnt"));

        // dif
        maxTable
            .join(
                minTable,
                new ValueJoiner<Integer, Integer, Integer>() {
                    public Integer apply(final Integer value1, final Integer value2) {
                        return value1 - value2;
                    }
                })
            .toStream()
            .to("dif", Produced.with(stringSerde, intSerde));

        // avg
        sumTable
            .join(
                cntTable,
                new ValueJoiner<Long, Long, Double>() {
                    public Double apply(final Long value1, final Long value2) {
                        return (double) value1 / (double) value2;
                    }
                })
            .toStream()
            .to("avg", Produced.with(stringSerde, doubleSerde));

        // test repartition
        final Agg agg = new Agg();
        cntTable.groupBy(agg.selector(), Serialized.with(stringSerde, longSerde))
            .aggregate(agg.init(), agg.adder(), agg.remover(),
                Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("cntByCnt"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()))
            .toStream()
            .to("tagg", Produced.with(stringSerde, longSerde));

        final KafkaStreams streamsClient = new KafkaStreams(builder.build(), getStreamsConfig(stateDir, kafka));
        streamsClient.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread t, final Throwable e) {
                System.out.println("FATAL: An unexpected exception is encountered on thread " + t + ": " + e);
                streamsClient.close(30, TimeUnit.SECONDS);
            }
        });

        return streamsClient;
    }
}
