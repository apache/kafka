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
package org.apache.kafka.streams.tools;

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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * TODO
 */
public class KafkaStreamsCleanupClient {
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

            // TODO fix -- simply sleep is not good enough
            try {
                Thread.sleep(120 * 1000); // default clean-up interval is 60 seconds
            } catch (Exception e) { }

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

            for (String window : options.valuesOf(windowOption)) {
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
        deleteDirectory(stateStore);
        System.out.println("Deleted " + stateStore.getAbsolutePath());
    }

    private void deleteDirectory(File directory) {
        for (File f : directory.listFiles()) {
            if (f.isDirectory()) {
                deleteDirectory(f);
            } else if (!f.delete()) {
                System.err.println("ERROR: could not delete file " + f.getName() + " within " + directory.getAbsolutePath());
            }
        }
        if (!directory.delete()) {
            System.err.println("ERROR: could not delete directory " + directory.getAbsolutePath());
        }
    }

    public static void main(String[] args) throws IOException {
        new KafkaStreamsCleanupClient().run(args);
    }

}
