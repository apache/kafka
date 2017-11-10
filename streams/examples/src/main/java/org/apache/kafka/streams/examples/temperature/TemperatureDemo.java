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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement an IoT demo application
 * which ingests temperature value processing the maximum value in the latest TEMPERATURE_WINDOW_SIZE seconds (which
 * is 5 seconds) sending a new message if it exceeds the TEMPERATURE_THRESHOLD (which is 20)
 *
 * In this example, the input stream reads from a topic named "iot-temperature", where the values of messages
 * represent temperature values; using a TEMPERATURE_WINDOW_SIZE seconds "tumbling" window, the maximum value is processed and
 * sent to a topic named "iot-temperature-max" if it exceeds the TEMPERATURE_THRESHOLD.
 *
 * Before running this example you must create the input topic for temperature values in the following way :
 *
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic iot-temperature
 *
 * and at same time the output topic for filtered values :
 *
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic iot-temperature-max
 *
 * After that, a console consumer can be started in order to read filtered values from the "iot-temperature-max" topic :
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-temperature-max --from-beginning
 *
 * On the other side, a console producer can be used for sending temperature values (which needs to be integers)
 * to "iot-temperature" typing them on the console :
 *
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iot-temperature
 * > 10
 * > 15
 * > 22
 */
public class TemperatureDemo {

    // threshold used for filtering max temperature values
    private static final int TEMPERATURE_THRESHOLD = 20;
    // window size within which the filtering is applied
    private static final int TEMPERATURE_WINDOW_SIZE = 5;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("iot-temperature");

        KStream<Windowed<String>, String> max = source
            // temperature values are sent without a key (null), so in order
            // to group and reduce them, a key is needed ("temp" has been chosen)
            .selectKey(new KeyValueMapper<String, String, String>() {
                @Override
                public String apply(String key, String value) {
                    return "temp";
                }
            })
            .groupByKey()
            .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(TEMPERATURE_WINDOW_SIZE)))
            .reduce(new Reducer<String>() {
                @Override
                public String apply(String value1, String value2) {
                    if (Integer.parseInt(value1) > Integer.parseInt(value2))
                        return value1;
                    else
                        return value2;
                }
            })
            .toStream()
            .filter(new Predicate<Windowed<String>, String>() {
                @Override
                public boolean test(Windowed<String> key, String value) {
                    return Integer.parseInt(value) > TEMPERATURE_THRESHOLD;
                }
            });

        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer(), TEMPERATURE_WINDOW_SIZE);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

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
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
