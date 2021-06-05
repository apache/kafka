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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class StreamsUpgradeToCooperativeRebalanceTest {


    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeToCooperativeRebalanceTest requires one argument (properties-file) but none provided");
        }
        final String propFileName = args[0];
        final Properties streamsProperties = Utils.loadProps(propFileName);
        final Properties config = new Properties();

        System.out.println("StreamsTest instance started (StreamsUpgradeToCooperativeRebalanceTest v0.11.0)");
        System.out.println("props=" + config);

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "cooperative-rebalance-upgrade");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.putAll(streamsProperties);

        final String sourceTopic = config.getProperty("source.topic", "source");
        final String sinkTopic = config.getProperty("sink.topic", "sink");
        final int reportInterval = Integer.parseInt(config.getProperty("report.interval", "100"));
        final String upgradePhase = config.getProperty("upgrade.phase",  "");

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> upgradeStream = builder.stream(sourceTopic);
        upgradeStream.foreach(new ForeachAction<String, String>() {
            int recordCounter = 0;

            @Override
            public void apply(final String key, final String value) {
                if (recordCounter++ % reportInterval == 0) {
                    System.out.println(String.format("%sProcessed %d records so far", upgradePhase, recordCounter));
                    System.out.flush();
                }
            }
        }
        );
        upgradeStream.to(sinkTopic);

        final KafkaStreams streams = new KafkaStreams(builder, config);


        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            System.out.println(String.format("%sCOOPERATIVE-REBALANCE-TEST-CLIENT-CLOSED", upgradePhase));
            System.out.flush();
        }));
    }
}
