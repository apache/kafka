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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class StreamsUpgradeToCooperativeRebalanceTest {


    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeToCooperativeRebalanceTest requires one argument (kafka-url, properties-file) but none provided");
        }
        System.out.println("Args are " + Arrays.toString(args));
        final String propFileName = args[0];
        final Properties streamsProperties = Utils.loadProps(propFileName);

        final Properties config = new Properties();
        System.out.println("StreamsTest instance started (StreamsUpgradeToCooperativeRebalanceTest v2.3)");
        System.out.println("props=" + streamsProperties);

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "cooperative-rebalance-upgrade");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.putAll(streamsProperties);

        final String sourceTopic = streamsProperties.getProperty("source.topic", "source");
        final String sinkTopic = streamsProperties.getProperty("sink.topic", "sink");
        final String threadDelimiter = streamsProperties.getProperty("thread.delimiter", "&");
        final String taskDelimiter = streamsProperties.getProperty("task.delimiter", "#");
        final int reportInterval = Integer.parseInt(streamsProperties.getProperty("report.interval", "100"));

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(sourceTopic)
            .peek(new ForeachAction<String, String>() {
                int recordCounter = 0;

                @Override
                public void apply(final String key, final String value) {
                    if (recordCounter++ % reportInterval == 0) {
                        System.out.println(String.format("Processed %d records so far", recordCounter));
                        System.out.flush();
                    }
                }
            }
            ).to(sinkTopic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.setStateListener((newState, oldState) -> {
            if (newState == State.RUNNING && oldState == State.REBALANCING) {
                System.out.println("STREAMS in a RUNNING State");
                final Set<ThreadMetadata> allThreadMetadata = streams.localThreadsMetadata();
                final StringBuilder taskReportBuilder = new StringBuilder();
                for (final ThreadMetadata threadMetadata : allThreadMetadata) {
                    buildTaskAssignmentReport(taskReportBuilder, threadMetadata.activeTasks(), "ACTIVE-TASKS:");
                    if (!threadMetadata.standbyTasks().isEmpty()) {
                        taskReportBuilder.append(taskDelimiter);
                        buildTaskAssignmentReport(taskReportBuilder, threadMetadata.standbyTasks(), "STANDBY-TASKS:");
                    }
                    taskReportBuilder.append(threadDelimiter);
                }
                taskReportBuilder.setLength(taskReportBuilder.length() - 1);
                System.out.println("TASK-ASSIGNMENTS:" + taskReportBuilder);
            }

            if (newState == State.REBALANCING) {
                System.out.println("Starting a REBALANCE");
            }
        });


        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            System.out.println("COOPERATIVE-REBALANCE-TEST-CLIENT-CLOSED");
            System.out.flush();
        }));
    }

    private static void buildTaskAssignmentReport(final StringBuilder taskReportBuilder,
                                                  final Set<TaskMetadata> taskMetadata,
                                                  final String taskType) {
        taskReportBuilder.append(taskType);
        for (final TaskMetadata task : taskMetadata) {
            final Set<TopicPartition> topicPartitions = task.topicPartitions();
            for (final TopicPartition topicPartition : topicPartitions) {
                taskReportBuilder.append(topicPartition.toString()).append(",");
            }
        }
        taskReportBuilder.setLength(taskReportBuilder.length() - 1);
    }
}
