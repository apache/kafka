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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Properties;


public class StreamsUpgradeTest {

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeTest requires one argument (properties-file) but provided none");
        }
        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);

        System.out.println("StreamsTest instance started (StreamsUpgradeTest v3.0)");
        System.out.println("props=" + streamsProperties);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream dataStream = builder.stream("data");
        dataStream.process(printProcessorSupplier());
        dataStream.to("echo");

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "StreamsUpgradeTest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        config.putAll(streamsProperties);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            System.out.println("UPGRADE-TEST-CLIENT-CLOSED");
            System.out.flush();
        }));
    }

    private static <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> printProcessorSupplier() {
        return () -> new ContextualProcessor<KIn, VIn, KOut, VOut>() {
            private int numRecordsProcessed = 0;

            @Override
            public void init(final ProcessorContext<KOut, VOut> context) {
                System.out.println("[3.0] initializing processor: topic=data taskId=" + context.taskId());
                numRecordsProcessed = 0;
            }

            @Override
            public void process(final Record<KIn, VIn> record) {
                numRecordsProcessed++;
                if (numRecordsProcessed % 100 == 0) {
                    System.out.println("processed " + numRecordsProcessed + " records from topic=data");
                }
            }

            @Override
            public void close() {}
        };
    }
}
