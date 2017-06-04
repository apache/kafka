/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.tools;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * TODO
 */
public class StreamsCleanupClient {
    private static OptionSpec<String> bootstrapServerOption;
    private static OptionSpec<String> zookeeperOption;
    private static OptionSpec<String> applicationIdOption;
    private static OptionSpec<String> sourceTopicsOption;
    private static OptionSpec<String> intermediateTopicsOption;
    private static OptionSpec<String> operatorOption;
    private static OptionSpec<String> windowOption;
    private static OptionSpec<String> stateDirOption;

    private OptionSet options = null;

    private void run(String[] args) throws IOException {
        parseArguments(args);

        ZkUtils zkUtils = ZkUtils.apply(options.valueOf(zookeeperOption),
                30000,
                30000,
                JaasUtils.isZkSecurityEnabled());

        List<String> allTopics = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());

        resetSourceTopicOffsets();
        clearIntermediateTopics(zkUtils, allTopics);
        deleteInternalTopics(allTopics);
        removeLocalStateStore();
    }

    private void parseArguments(String[] args) throws IOException {
        OptionParser optionParser = new OptionParser();
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id)")
                .withRequiredArg()
                .ofType(String.class)
                .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-server", "Format: <host:port>")
                .withRequiredArg()
                .defaultsTo("localhost:9092")
                .describedAs("url")
                .ofType(String.class);
        zookeeperOption = optionParser.accepts("zookeeper", "Format: <host:port>")
                .withRequiredArg()
                .defaultsTo("localhost:2181")
                .describedAs("url")
                .ofType(String.class);
        sourceTopicsOption = optionParser.accepts("source-topics", "Comma separated list of user source topics")
                .withRequiredArg()
                .ofType(String.class);
        intermediateTopicsOption = optionParser.accepts("intermediate-topics", "Comma separated list of intermediate user topics")
                .withRequiredArg()
                .ofType(String.class);
        operatorOption = optionParser.accepts("operators", "Comma separted list of operator names")
                .withRequiredArg()
                .ofType(String.class);
        windowOption = optionParser.accepts("windows", "Comma separted list of window names")
                .withRequiredArg()
                .ofType(String.class);
        stateDirOption = optionParser.accepts("state-dir", "Local state store directory (state.dir)")
                .withRequiredArg()
                .defaultsTo("/tmp/kafka-streams")
                .ofType(String.class);

        try {
            options = optionParser.parse(args);
        } catch (OptionException e) {
            System.err.println(e.getMessage());
            optionParser.printHelpOn(System.err);
            System.exit(-1);
        }
    }

    private void resetSourceTopicOffsets() {
        List<String> topics = new LinkedList<>();
        topics.addAll(options.valuesOf(sourceTopicsOption));
        topics.addAll(options.valuesOf(intermediateTopicsOption));

        if (topics.size() == 0) {
            System.out.println("No source or intermediate topics specified.");
            return;
        }

        System.out.println("Resetting offests to zero for topics " + topics);

        Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(applicationIdOption));
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        try {
            client.subscribe(topics);
            client.poll(1);

            for (TopicPartition partition : client.assignment()) {
                client.seek(partition, 0);
            }
            client.commitSync();
        } catch (RuntimeException e) {
            System.err.println("Resetting source topic offsets failed");
            throw e;
        } finally {
            client.close();
        }
    }

    private void clearIntermediateTopics(ZkUtils zkUtils, List<String> allTopics) {
        if (options.has(intermediateTopicsOption)) {
            System.out.println("Clearing intermediate user topics.");
            
            HashMap<String, String> topicRetentionTime = new HashMap<>();
            for (String topic : options.valuesOf(intermediateTopicsOption)) {
                if (allTopics.contains(topic)) {
                    Properties configs = AdminUtils.fetchEntityConfig(zkUtils, "topics", topic);
                    topicRetentionTime.put(topic, configs.getProperty("retention.ms"));

                    configs.setProperty("retention.ms", "1");
                    AdminUtils.changeTopicConfig(zkUtils, topic, configs);
                }
            }

            Properties config = new Properties();
            config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));
            config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(applicationIdOption));
            config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            try {
                client.subscribe(topicRetentionTime.keySet());
                client.poll(1);

                HashMap<TopicPartition, Long> lowerOffsets = new HashMap<>();
                Set<TopicPartition> partitions = client.assignment();
                do {
                    lowerOffsets.clear();

                    for (TopicPartition partition : partitions) {
                        client.seek(partition, 0);
                        lowerOffsets.put(partition, client.position(partition));
                    }

                    client.seekToEnd(partitions);
                    Iterator<TopicPartition> it = partitions.iterator();
                    while (it.hasNext()) {
                        TopicPartition partition = it.next();
                        if(client.position(partition) == lowerOffsets.get(partition)) {
                            it.remove(); 
                        }
                    }
                } while(!lowerOffsets.isEmpty());
            } catch (RuntimeException e) {
                System.err.println("Clearing intermediate topics failed. Did not restore values for parameter <retention.ms>\n"
                                   + "Values for <retention.ms>: " + topicRetentionTime);
                throw e;
            } finally {
                client.close();
            }

            for (String topic : options.valuesOf(intermediateTopicsOption)) {
                if (allTopics.contains(topic)) {
                    Properties configs = AdminUtils.fetchEntityConfig(zkUtils, "topics", topic);

                    String retentionTime = topicRetentionTime.get(topic);
                    if (retentionTime == null) {
                        configs.remove("retention.ms");
                    } else {
                        configs.setProperty("retention.ms", retentionTime);
                    }
                    AdminUtils.changeTopicConfig(zkUtils, topic, configs);
                } else {
                    System.err.println("Intermediate user topic not found " + topic);
                }
            }

            System.out.println("Done.");
        } else {
            System.out.println("No intermediate topics specified.");
        }
    }

    private void deleteInternalTopics(List<String> allTopics) {
        if (options.has(operatorOption) || options.has(windowOption)) {
            System.out.println("Deleting internal topics.");

            String applicationId = options.valueOf(applicationIdOption);

            for (String operator : options.valuesOf(operatorOption)) {
                String changelog = applicationId + "-" + operator + "-changelog";
                if (allTopics.contains(changelog)) {
                    TopicCommand.main(new String[] {
                        "--zookeeper", options.valueOf(zookeeperOption),
                        "--delete", "--topic", changelog});
                } else {
                    System.err.println("No topic found for operator " + operator);
                }
            }

            List<String> windowTopics = new LinkedList<>();
            for (String operator : options.valuesOf(windowOption)) {
                windowTopics.add(operator + "-this");
                windowTopics.add(operator + "-other");
            }
            for (String window : windowTopics) {
                String changelog = applicationId + "-" + window + "-changelog";
                if (allTopics.contains(changelog)) {
                    TopicCommand.main(new String[] {
                        "--zookeeper", options.valueOf(zookeeperOption),
                        "--delete", "--topic", applicationId + "-" + window + "-changelog"});
                } else {
                    System.err.println("No topic found for window " + window);
                }
            }

            System.out.println("Done.");
        } else {
            System.out.println("No operators or windows specified.");
        }
    }

    private void removeLocalStateStore() {
        File stateStore = new File(options.valueOf(stateDirOption) + File.separator + options.valueOf(applicationIdOption));
        if (!stateStore.exists()) {
            System.out.println("Nothing to clear. Local state store directory does not exist.");
            return;
        }
        if (!stateStore.isDirectory()) {
            System.err.println("ERROR: " + stateStore.getAbsolutePath() + " is not a directory.");
            return;
        }

        System.out.println("Removing local state store.");
        Utils.delete(stateStore);
        System.out.println("Deleted " + stateStore.getAbsolutePath());
    }

    public static void main(String[] args) throws IOException {
        new StreamsCleanupClient().run(args);
    }

}
