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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
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
        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            isRunning = false;
            streams.close(Duration.ofSeconds(300));

            // need to wait for callback to avoid race condition
            // -> make sure the callback printout to stdout is there as it is expected test output
            waitForStateTransitionCallback();

            // do not remove these printouts since they are needed for health scripts
            if (!uncaughtException) {
                System.out.println(System.currentTimeMillis());
                System.out.println("EOS-TEST-CLIENT-CLOSED");
                System.out.flush();
            }
        });

        while (isRunning) {
            if (streams == null) {
                uncaughtException = false;

                streams = createKafkaStreams(properties);
                streams.setUncaughtExceptionHandler(e -> {
                    System.out.println(System.currentTimeMillis());
                    System.out.println("EOS-TEST-CLIENT-EXCEPTION");
                    e.printStackTrace();
                    System.out.flush();
                    uncaughtException = true;
                    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
                });
                streams.setStateListener((newState, oldState) -> {
                    // don't remove this -- it's required test output
                    System.out.println(System.currentTimeMillis());
                    System.out.println("StateChange: " + oldState + " -> " + newState);
                    System.out.flush();
                    if (newState == KafkaStreams.State.NOT_RUNNING) {
                        notRunningCallbackReceived.set(true);
                    }
                });
                streams.start();
            }
            if (uncaughtException) {
                streams.close(Duration.ofSeconds(60_000L));
                streams = null;
            }
            sleep(1000);
        }
    }

    private KafkaStreams createKafkaStreams(final Properties props) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, Duration.ofMinutes(1).toMillis());
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, Integer.MAX_VALUE);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000L); // increase commit interval to make sure a client is killed having an open transaction
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Integer> data = builder.stream("data");

        data.to("echo");
        data.process(SmokeTestUtil.printProcessorSupplier("data"));

        final KGroupedStream<String, Integer> groupedData = data.groupByKey();
        // min
        groupedData
            .aggregate(
                () -> Integer.MAX_VALUE,
                (aggKey, value, aggregate) -> (value < aggregate) ? value : aggregate,
                Materialized.with(null, intSerde))
            .toStream()
            .to("min", Produced.with(stringSerde, intSerde));

        // sum
        groupedData.aggregate(
            () -> 0L,
            (aggKey, value, aggregate) -> (long) value + aggregate,
            Materialized.with(null, longSerde))
            .toStream()
            .to("sum", Produced.with(stringSerde, longSerde));

        if (withRepartitioning) {
            data.to("repartition");
            final KStream<String, Integer> repartitionedData = builder.stream("repartition");

            repartitionedData.process(SmokeTestUtil.printProcessorSupplier("repartition"));

            final KGroupedStream<String, Integer> groupedDataAfterRepartitioning = repartitionedData.groupByKey();
            // max
            groupedDataAfterRepartitioning
                .aggregate(
                    () -> Integer.MIN_VALUE,
                    (aggKey, value, aggregate) -> (value > aggregate) ? value : aggregate,
                    Materialized.with(null, intSerde))
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
