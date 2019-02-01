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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

public class StreamsNamedRepartitionTest {

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsNamedRepartitionTest requires one argument (properties-file) but none provided: ");
        }
        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started NAMED_REPARTITION_TEST");
        System.out.println("props=" + streamsProperties);

        final String inputTopic = (String) (Objects.requireNonNull(streamsProperties.remove("input.topic")));
        final String aggregationTopic = (String) (Objects.requireNonNull(streamsProperties.remove("aggregation.topic")));
        final boolean addOperators = Boolean.valueOf(Objects.requireNonNull((String) streamsProperties.remove("add.operations")));


        final Initializer<Integer> initializer = () -> 0;
        final Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + Integer.parseInt(v);

        final Function<String, String> keyFunction = s -> Integer.toString(Integer.parseInt(s) % 9);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        sourceStream.peek((k, v) -> System.out.println(String.format("input data key=%s, value=%s", k, v)));

        final KStream<String, String> mappedStream = sourceStream.selectKey((k, v) -> keyFunction.apply(v));

        final KStream<String, String> maybeUpdatedStream;

        if (addOperators) {
            maybeUpdatedStream = mappedStream.filter((k, v) -> true).mapValues(v -> Integer.toString(Integer.parseInt(v) + 1));
        } else {
            maybeUpdatedStream = mappedStream;
        }

        maybeUpdatedStream.groupByKey(Grouped.with("grouped-stream", Serdes.String(), Serdes.String()))
            .aggregate(initializer, aggregator, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("count-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer()))
            .toStream()
            .peek((k, v) -> System.out.println(String.format("AGGREGATED key=%s value=%s", k, v)))
            .to(aggregationTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        final Properties config = new Properties();

        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsNamedRepartitionTest");
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        config.putAll(streamsProperties);

        final Topology topology = builder.build(config);
        final KafkaStreams streams = new KafkaStreams(topology, config);


        streams.setStateListener((newState, oldState) -> {
            if (oldState == State.REBALANCING && newState == State.RUNNING) {
                if (addOperators) {
                    System.out.println("UPDATED Topology");
                } else {
                    System.out.println("REBALANCING -> RUNNING");
                }
                System.out.flush();
            }
        });

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("closing Kafka Streams instance");
            System.out.flush();
            streams.close(Duration.ofMillis(5000));
            System.out.println("NAMED_REPARTITION_TEST Streams Stopped");
            System.out.flush();
        }));

    }

}
