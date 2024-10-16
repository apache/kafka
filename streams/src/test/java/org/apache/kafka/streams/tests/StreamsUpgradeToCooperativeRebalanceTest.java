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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.kstream.ForeachAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class StreamsUpgradeToCooperativeRebalanceTest {


    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("StreamsUpgradeToCooperativeRebalanceTest requires one argument (properties-file) but no args provided");
        }
        System.out.println("Args are " + Arrays.toString(args));
        final String propFileName = args[0];
        final Properties streamsProperties = Utils.loadProps(propFileName);

        final Properties config = new Properties();
        System.out.println("StreamsTest instance started (StreamsUpgradeToCooperativeRebalanceTest)");
        System.out.println("props=" + streamsProperties);

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "cooperative-rebalance-upgrade");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        config.putAll(streamsProperties);

        final String sourceTopic = streamsProperties.getProperty("source.topic", "source");
        final String sinkTopic = streamsProperties.getProperty("sink.topic", "sink");
        final String taskDelimiter = "#";
        final int reportInterval = Integer.parseInt(streamsProperties.getProperty("report.interval", "100"));
        final String upgradePhase = streamsProperties.getProperty("upgrade.phase", "");

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(sourceTopic)
            .peek(new ForeachAction<String, String>() {
                int recordCounter = 0;

                @Override
                public void apply(final String key, final String value) {
                    if (recordCounter++ % reportInterval == 0) {
                        System.out.printf("%sProcessed %d records so far%n", upgradePhase, recordCounter);
                        System.out.flush();
                    }
                }
            }
            ).to(sinkTopic);

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.setStateListener((newState, oldState) -> {
            if (newState == State.RUNNING && oldState == State.REBALANCING) {
                System.out.printf("%sSTREAMS in a RUNNING State%n", upgradePhase);
                final Set<ThreadMetadata> allThreadMetadata = streams.metadataForLocalThreads();
                final StringBuilder taskReportBuilder = new StringBuilder();
                final List<String> activeTasks = new ArrayList<>();
                final List<String> standbyTasks = new ArrayList<>();
                for (final ThreadMetadata threadMetadata : allThreadMetadata) {
                    getTasks(threadMetadata.activeTasks(), activeTasks);
                    if (!threadMetadata.standbyTasks().isEmpty()) {
                        getTasks(threadMetadata.standbyTasks(), standbyTasks);
                    }
                }
                addTasksToBuilder(activeTasks, taskReportBuilder);
                taskReportBuilder.append(taskDelimiter);
                if (!standbyTasks.isEmpty()) {
                    addTasksToBuilder(standbyTasks, taskReportBuilder);
                }
                System.out.println("TASK-ASSIGNMENTS:" + taskReportBuilder);
            }

            if (newState == State.REBALANCING) {
                System.out.printf("%sStarting a REBALANCE%n", upgradePhase);
            }
        });


        streams.start();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            streams.close();
            System.out.printf("%sCOOPERATIVE-REBALANCE-TEST-CLIENT-CLOSED%n", upgradePhase);
            System.out.flush();
        });
    }

    private static void addTasksToBuilder(final List<String> tasks, final StringBuilder builder) {
        if (!tasks.isEmpty()) {
            for (final String task : tasks) {
                builder.append(task).append(",");
            }
            builder.setLength(builder.length() - 1);
        }
    }

    private static void getTasks(final Set<TaskMetadata> taskMetadata,
                                 final List<String> taskList) {
        for (final TaskMetadata task : taskMetadata) {
            final Set<TopicPartition> topicPartitions = task.topicPartitions();
            for (final TopicPartition topicPartition : topicPartitions) {
                taskList.add(topicPartition.toString());
            }
        }
    }
}
