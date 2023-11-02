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
package org.apache.kafka.streams.examples.temperature;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * This demo demonstrates, using the high-level KStream DSL, how to implement an IoT demo application
 * which ingests temperature value to compute the maximum value in the latest TEMPERATURE_WINDOW_SIZE seconds (which
 * is 5 seconds) and send a new message if it exceeds the TEMPERATURE_THRESHOLD (which is 20)
 *
 * <p>In this example, the input stream reads from a topic named "iot-temperature", where the values of messages
 * represent temperature values; using a TEMPERATURE_WINDOW_SIZE seconds "tumbling" window, the maximum value is processed and
 * sent to a topic named "iot-temperature-max" if it exceeds the TEMPERATURE_THRESHOLD.
 *
 * <p>Before running this example you must create the input topic for temperature values in the following way :
 *
 * <p>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-temperature
 *
 * <p>and at same time create the output topic for filtered values :
 *
 * <p>bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-temperature-max
 *
 * <p>After that, a console consumer can be started in order to read filtered values from the "iot-temperature-max" topic :
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-temperature-max --from-beginning
 *
 * <p>On the other side, a console producer can be used for sending temperature values (which needs to be integers)
 * to "iot-temperature" by typing them on the console :
 *
 * <p>bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot-temperature
 * > 10
 * > 15
 * > 22
 */
public class TemperatureDemo {

    // threshold used for filtering max temperature values
    private static final int TEMPERATURE_THRESHOLD = 20;
    // window size within which the filtering is applied
    private static final int TEMPERATURE_WINDOW_SIZE = 5;

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        final Duration duration24Hours = Duration.ofHours(24);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> source = builder.stream("iot-temperature");

        final KStream<Windowed<String>, String> max = source
            // temperature values are sent without a key (null), so in order
            // to group and reduce them, a key is needed ("temp" has been chosen)
            .selectKey((key, value) -> "temp")
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE), duration24Hours))
            .reduce((value1, value2) -> {
                if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
                    return value1;
                } else {
                    return value2;
                }
            })
            .toStream()
            .filter((key, value) -> Integer.parseInt(value) > TEMPERATURE_THRESHOLD);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, TEMPERATURE_WINDOW_SIZE);

        // need to override key serde to Windowed<String> type
        max.to("iot-temperature-max", Produced.with(windowedSerde, Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
