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
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SmokeTestClient extends SmokeTestUtil {

    private final String kafka;
    private final Properties streamsProperties;
    private KafkaStreams streams;
    private Thread thread;
    private boolean uncaughtException = false;

    public SmokeTestClient(final Properties streamsProperties, final String kafka) {
        super();
        this.kafka = kafka;
        this.streamsProperties = streamsProperties;
    }

    public void start() {
        streams = createKafkaStreams(streamsProperties, kafka);
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
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

        thread = ()-> { streams.start();};
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

    private static KafkaStreams createKafkaStreams(final Properties props, final String kafka) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SmokeTest");
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
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 80000);

        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, Integer> stringIntConsumed = Consumed.with(stringSerde, intSerde);
        KStream<String, Integer> source = builder.stream("data", stringIntConsumed);
        source.to(stringSerde, intSerde, "echo");
        KStream<String, Integer> data = source.filter(new Predicate<String, Integer>() {
            @Override
            public boolean test(String key, Integer value) {
                return value == null || value != END;
            }
        });
        data.process(SmokeTestUtil.printProcessorSupplier("data"));

        // min
        KGroupedStream<String, Integer>
            groupedData =
            data.groupByKey(Serialized.with(stringSerde, intSerde));

        groupedData.aggregate(
                ()-> { return Integer.MAX_VALUE;},
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(String aggKey, Integer value, Integer aggregate) {
                        return (value < aggregate) ? value : aggregate;
                    }
                },
                TimeWindows.of(TimeUnit.DAYS.toMillis(1)),
                intSerde, "uwin-min"
        ).toStream().map(
                new Unwindow<String, Integer>()
        ).to(stringSerde, intSerde, "min");

        KTable<String, Integer> minTable = builder.table("min", stringIntConsumed);
        minTable.toStream().process(SmokeTestUtil.printProcessorSupplier("min"));

        // max
        groupedData.aggregate(
                ()-> { return Integer.MIN_VALUE;},
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(String aggKey, Integer value, Integer aggregate) {
                        return (value > aggregate) ? value : aggregate;
                    }
                },
                TimeWindows.of(TimeUnit.DAYS.toMillis(2)),
                intSerde, "uwin-max"
        ).toStream().map(
                new Unwindow<String, Integer>()
        ).to(stringSerde, intSerde, "max");

        KTable<String, Integer> maxTable = builder.table("max", stringIntConsumed);
        maxTable.toStream().process(SmokeTestUtil.printProcessorSupplier("max"));

        // sum
        groupedData.aggregate(
                ()-> { return 0L;},
                new Aggregator<String, Integer, Long>() {
                    @Override
                    public Long apply(String aggKey, Integer value, Long aggregate) {
                        return (long) value + aggregate;
                    }
                },
                TimeWindows.of(TimeUnit.DAYS.toMillis(2)),
                longSerde, "win-sum"
        ).toStream().map(
                new Unwindow<String, Long>()
        ).to(stringSerde, longSerde, "sum");

        Consumed<String, Long> stringLongConsumed = Consumed.with(stringSerde, longSerde);
        KTable<String, Long> sumTable = builder.table("sum", stringLongConsumed);
        sumTable.toStream().process(SmokeTestUtil.printProcessorSupplier("sum"));
        // cnt
        groupedData.count(TimeWindows.of(TimeUnit.DAYS.toMillis(2)), "uwin-cnt")
            .toStream().map(
                new Unwindow<String, Long>()
        ).to(stringSerde, longSerde, "cnt");

        KTable<String, Long> cntTable = builder.table("cnt", stringLongConsumed);
        cntTable.toStream().process(SmokeTestUtil.printProcessorSupplier("cnt"));

        // dif
        maxTable.join(minTable,
                new ValueJoiner<Integer, Integer, Integer>() {
                    public Integer apply(Integer value1, Integer value2) {
                        return value1 - value2;
                    }
                }
        ).to(stringSerde, intSerde, "dif");

        // avg
        sumTable.join(
                cntTable,
                new ValueJoiner<Long, Long, Double>() {
                    public Double apply(Long value1, Long value2) {
                        return (double) value1 / (double) value2;
                    }
                }
        ).to(stringSerde, doubleSerde, "avg");

        // test repartition
        Agg agg = new Agg();
        cntTable.groupBy(agg.selector(),
                         Serialized.with(stringSerde, longSerde)
        ).aggregate(agg.init(),
                    agg.adder(),
                    agg.remover(),
                    Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("cntByCnt"))
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.Long())
        ).to(stringSerde, longSerde, "tagg");

        final KafkaStreams streamsClient = new KafkaStreams(builder.build(), props);
        streamsClient.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.out.println("FATAL: An unexpected exception is encountered on thread " + t + ": " + e);
                
                streamsClient.close(30, TimeUnit.SECONDS);
            }
        });

        return streamsClient;
    }

}
