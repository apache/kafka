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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;

public class TopologyDescriptionPluginSystemTest {

    private static final String APPLICATION_ID = "kafka-streams-system-test-topology-description-plugin";
    private static final String SOURCE_TOPIC = "topologyDescriptionPluginSource";
    private static final String SINK_TOPIC = "topologyDescriptionPluginSink";

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("TopologyDescriptionPluginSystemTest expects one parameter: propFile");
            Exit.exit(1);
        }

        System.out.println("TopologyDescriptionPluginSystemTest starting");

        final String propFileName = args[0];
        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String bootstrap = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (bootstrap == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            Exit.exit(1);
        }

        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues(value -> value.toLowerCase(Locale.ROOT))
            .groupByKey()
            .count()
            .toStream()
            .mapValues(count -> Long.toString(count))
            .to(SINK_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("FATAL: An unexpected exception " + e);
            System.err.flush();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        System.out.println("Start Kafka Streams");
        streams.start();
        System.out.println("STREAMS-STARTED");
        System.out.flush();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            streams.close(Duration.ofSeconds(30));
            System.out.println("TopologyDescriptionPluginSystemTest closed");
            System.out.flush();
        });
    }
}
