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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.time.Duration.ofMillis;

@SuppressWarnings("deprecation")
public class StreamsOptimizedTest {

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsOptimizedTest requires one argument (properties-file) but no provided: ");
        }
        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started StreamsOptimizedTest");
        System.out.println("props=" + streamsProperties);

        final String inputTopic = (String) Objects.requireNonNull(streamsProperties.remove("input.topic"));
        final String aggregationTopic = (String) Objects.requireNonNull(streamsProperties.remove("aggregation.topic"));
        final String reduceTopic = (String) Objects.requireNonNull(streamsProperties.remove("reduce.topic"));
        final String joinTopic = (String) Objects.requireNonNull(streamsProperties.remove("join.topic"));


        final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");
        final Initializer<Integer> initializer = () -> 0;
        final Aggregator<String, String, Integer> aggregator = (k, v, agg) -> agg + v.length();

        final Reducer<String> reducer = (v1, v2) -> Integer.toString(Integer.parseInt(v1) + Integer.parseInt(v2));

        final Function<String, String> keyFunction = s -> Integer.toString(Integer.parseInt(s) % 9);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> mappedStream = sourceStream.selectKey((k, v) -> keyFunction.apply(v));

        final KStream<String, Long> countStream = mappedStream.groupByKey()
                                                               .count(Materialized.with(Serdes.String(),
                                                                                        Serdes.Long())).toStream();

        mappedStream.groupByKey().aggregate(
            initializer,
            aggregator,
            Materialized.with(Serdes.String(), Serdes.Integer()))
            .toStream()
            .peek((k, v) -> System.out.printf("AGGREGATED key=%s value=%s%n", k, v))
            .to(aggregationTopic, Produced.with(Serdes.String(), Serdes.Integer()));


        mappedStream.groupByKey()
            .reduce(reducer, Materialized.with(Serdes.String(), Serdes.String()))
            .toStream()
            .peek((k, v) -> System.out.printf("REDUCED key=%s value=%s%n", k, v))
            .to(reduceTopic, Produced.with(Serdes.String(), Serdes.String()));

        mappedStream.join(countStream, (v1, v2) -> v1 + ":" + v2.toString(),
            JoinWindows.of(ofMillis(500)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.Long()))
            .peek((k, v) -> System.out.printf("JOINED key=%s value=%s%n", k, v))
            .to(joinTopic, Produced.with(Serdes.String(), Serdes.String()));

        final Properties config = new Properties();


        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsOptimizedTest");
        config.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.adminClientPrefix(AdminClientConfig.RETRIES_CONFIG), "100");


        config.putAll(streamsProperties);

        final Topology topology = builder.build(config);
        final KafkaStreams streams = new KafkaStreams(topology, config);


        streams.setStateListener((newState, oldState) -> {
            if (oldState == State.REBALANCING && newState == State.RUNNING) {
                final int repartitionTopicCount = getCountOfRepartitionTopicsFound(topology.describe().toString(), repartitionTopicPattern);
                System.out.printf("REBALANCING -> RUNNING with REPARTITION TOPIC COUNT=%d%n", repartitionTopicCount);
                System.out.flush();
            }
        });

        streams.cleanUp();
        streams.start();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            System.out.println("closing Kafka Streams instance");
            System.out.flush();
            streams.close(Duration.ofMillis(5000));
            System.out.println("OPTIMIZE_TEST Streams Stopped");
            System.out.flush();
        });

    }

    private static int getCountOfRepartitionTopicsFound(final String topologyString,
                                                        final Pattern repartitionTopicPattern) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            final String repartitionTopic = matcher.group();
            System.out.printf("REPARTITION TOPIC found -> %s%n", repartitionTopic);
            repartitionTopicsFound.add(repartitionTopic);
        }
        return repartitionTopicsFound.size();
    }
}
