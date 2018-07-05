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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EosTestClient extends SmokeTestUtil {

    static final String APP_ID = "EosTest";
    private final Properties properties;
    private final boolean withRepartitioning;
    private final AtomicBoolean notRunningCallbackReceived = new AtomicBoolean(false);

    private KafkaStreams streams;
    private boolean uncaughtException;

    EosTestClient(final Properties properties, final boolean withRepartitioning) {
        super();
        this.properties = properties;
        this.withRepartitioning = withRepartitioning;
    }

    private volatile boolean isRunning = true;

    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                isRunning = false;
                streams.close(TimeUnit.SECONDS.toMillis(300), TimeUnit.SECONDS);

                // need to wait for callback to avoid race condition
                // -> make sure the callback printout to stdout is there as it is expected test output
                waitForStateTransitionCallback();

                // do not remove these printouts since they are needed for health scripts
                if (!uncaughtException) {
                    System.out.println(System.currentTimeMillis());
                    System.out.println("EOS-TEST-CLIENT-CLOSED");
                    System.out.flush();
                }

            }
        }));

        while (isRunning) {
            if (streams == null) {
                uncaughtException = false;

                streams = createKafkaStreams(properties);
                streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(final Thread t, final Throwable e) {
                        System.out.println(System.currentTimeMillis());
                        System.out.println("EOS-TEST-CLIENT-EXCEPTION");
                        e.printStackTrace();
                        System.out.flush();
                        uncaughtException = true;
                    }
                });
                streams.setStateListener(new KafkaStreams.StateListener() {
                    @Override
                    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
                        // don't remove this -- it's required test output
                        System.out.println(System.currentTimeMillis());
                        System.out.println("StateChange: " + oldState + " -> " + newState);
                        System.out.flush();
                        if (newState == KafkaStreams.State.NOT_RUNNING) {
                            notRunningCallbackReceived.set(true);
                        }
                    }
                });
                streams.start();
            }
            if (uncaughtException) {
                streams.close(TimeUnit.SECONDS.toMillis(60), TimeUnit.SECONDS);
                streams = null;
            }
            sleep(1000);
        }
    }

    private KafkaStreams createKafkaStreams(final Properties props) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        //TODO remove this config or set to smaller value when KIP-91 is merged
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 60000);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> data = builder.stream("data");

        data.to("echo");
        data.process(SmokeTestUtil.printProcessorSupplier("data"));

        final KGroupedStream<String, Integer> groupedData = data.groupByKey();
        // min
        groupedData
            .aggregate(
                new Initializer<Integer>() {
                    @Override
                    public Integer apply() {
                        return Integer.MAX_VALUE;
                    }
                },
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(final String aggKey,
                                         final Integer value,
                                         final Integer aggregate) {
                        return (value < aggregate) ? value : aggregate;
                    }
                },
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>with(null, intSerde))
            .toStream()
            .to("min", Produced.with(stringSerde, intSerde));

        // sum
        groupedData.aggregate(
            new Initializer<Long>() {
                @Override
                public Long apply() {
                    return 0L;
                }
            },
            new Aggregator<String, Integer, Long>() {
                @Override
                public Long apply(final String aggKey,
                                  final Integer value,
                                  final Long aggregate) {
                    return (long) value + aggregate;
                }
            },
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>with(null, longSerde))
            .toStream()
            .to("sum", Produced.with(stringSerde, longSerde));

        if (withRepartitioning) {
            final KStream<String, Integer> repartitionedData = data.through("repartition");

            repartitionedData.process(SmokeTestUtil.printProcessorSupplier("repartition"));

            final KGroupedStream<String, Integer> groupedDataAfterRepartitioning = repartitionedData.groupByKey();
            // max
            groupedDataAfterRepartitioning
                .aggregate(
                    new Initializer<Integer>() {
                        @Override
                        public Integer apply() {
                            return Integer.MIN_VALUE;
                        }
                    },
                    new Aggregator<String, Integer, Integer>() {
                        @Override
                        public Integer apply(final String aggKey,
                                             final Integer value,
                                             final Integer aggregate) {
                            return (value > aggregate) ? value : aggregate;
                        }
                    },
                    Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>with(null, intSerde))
                .toStream()
                .to("max", Produced.with(stringSerde, intSerde));

            // count
            groupedDataAfterRepartitioning.count()
                .toStream()
                .to("cnt", Produced.with(stringSerde, longSerde));
        }

        return new KafkaStreams(builder.build(), props);
    }

    private void waitForStateTransitionCallback() {
        final long maxWaitTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(300);
        while (!notRunningCallbackReceived.get() && System.currentTimeMillis() < maxWaitTime) {
            try {
                Thread.sleep(500);
            } catch (final InterruptedException ignoreAndSwallow) { /* just keep waiting */ }
        }
        if (!notRunningCallbackReceived.get()) {
            System.err.println("State transition callback to NOT_RUNNING never received. Timed out after 5 minutes.");
            System.err.flush();
        }
    }
}
