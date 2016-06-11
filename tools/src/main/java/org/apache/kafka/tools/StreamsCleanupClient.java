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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * {@link StreamsCleanupClient} resets a Kafka Streams application to the very beginning. Thus, an application can
 * reprocess the whole input from scratch.
 * <p>
 * This includes, setting source topic offsets to zero, skipping over all intermediate user topics,
 * deleting all internally created topics, and deleting local state stores.
 */
public class StreamsCleanupClient {
    private static OptionSpec<String> bootstrapServerOption;
    private static OptionSpec<String> zookeeperOption;
    private static OptionSpec<String> applicationIdOption;
    private static OptionSpec<String> sourceTopicsOption;
    private static OptionSpec<String> intermediateTopicsOption;
    private static OptionSpec<String> stateDirOption;

    private OptionSet options = null;

    public void run(String[] args) {
        ZkUtils zkUtils = null;

        int exitCode = 0;
        try {
            parseArguments(args);

            zkUtils = ZkUtils.apply(options.valueOf(zookeeperOption),
                    30000,
                    30000,
                    JaasUtils.isZkSecurityEnabled());

            final List<String> allTopics = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());

            resetSourceTopicOffsets();
            seekToEndIntermediateTopics(zkUtils, allTopics);
            deleteInternalTopics(zkUtils, allTopics);
            removeLocalStateStore();
        } catch (Exception e) {
            exitCode = -1;
            System.err.println(e.getMessage());
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
            System.exit(exitCode);
        }
    }

    private void parseArguments(final String[] args) throws IOException {
        final OptionParser optionParser = new OptionParser();
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id)")
                .withRequiredArg()
                .describedAs("id")
                .ofType(String.class)
                .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-server", "Format: <host:port>")
                .withRequiredArg()
                .describedAs("url")
                .defaultsTo("localhost:9092")
                .ofType(String.class);
        zookeeperOption = optionParser.accepts("zookeeper", "Format: <host:port>")
                .withRequiredArg()
                .describedAs("url")
                .defaultsTo("localhost:2181")
                .ofType(String.class);
        sourceTopicsOption = optionParser.accepts("source-topics", "Comma separated list of user source topics")
                .withRequiredArg()
                .describedAs("list")
                .ofType(String.class);
        intermediateTopicsOption = optionParser.accepts("intermediate-topics", "Comma separated list of intermediate user topics")
                .withRequiredArg()
                .describedAs("list")
                .ofType(String.class);
        stateDirOption = optionParser.accepts("state-dir", "Local state store directory (state.dir)")
                .withRequiredArg()
                .describedAs("dir")
                .defaultsTo("/tmp/kafka-streams")
                .ofType(String.class);

        try {
            options = optionParser.parse(args);
        } catch (OptionException e) {
            optionParser.printHelpOn(System.err);
            throw e;
        }
    }

    private void resetSourceTopicOffsets() {
        final List<String> topics = new LinkedList<>();
        topics.addAll(options.valuesOf(sourceTopicsOption));
        topics.addAll(options.valuesOf(intermediateTopicsOption));

        if (topics.size() == 0) {
            System.out.println("No source or intermediate topics specified.");
            return;
        }

        System.out.println("Resetting offests to zero for topics " + topics);

        final Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(applicationIdOption));
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
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

    private void seekToEndIntermediateTopics(ZkUtils zkUtils, List<String> allTopics) {
        if (options.has(intermediateTopicsOption)) {
            System.out.println("Seek-to-end for intermediate user topics.");

            final Properties config = new Properties();
            config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));
            config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(applicationIdOption));
            config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            for (String topic : options.valuesOf(intermediateTopicsOption)) {
                if (allTopics.contains(topic)) {
                    System.out.println("Topic: " + topic);

                    final KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                    try {
                        client.subscribe(Collections.singleton(topic));
                        client.poll(1);

                        final Set<TopicPartition> partitions = client.assignment();
                        client.seekToEnd(partitions);
                        client.commitSync();
                    } catch (RuntimeException e) {
                        System.err.println("Seek to end for topic " + topic + " failed");
                        throw e;
                    } finally {
                        client.close();
                    }
                }
            }

            System.out.println("Done.");
        } else {
            System.out.println("No intermediate topics specified.");
        }
    }

    private void deleteInternalTopics(ZkUtils zkUtils, List<String> allTopics) {
        System.out.println("Deleting internal topics.");

        final String applicationId = options.valueOf(applicationIdOption);

        for (String topic : allTopics) {
            if (topic.startsWith(applicationId + "-") && topic.endsWith("-changelog")) {
                TopicCommand.TopicCommandOptions commandOptions = new TopicCommand.TopicCommandOptions(new String[] {
                    "--zookeeper", options.valueOf(zookeeperOption),
                    "--delete", "--topic", topic});
                try {
                    TopicCommand.deleteTopic(zkUtils, commandOptions);
                } catch (RuntimeException e) {
                    System.err.println("Deleting internal topic " + topic + " failed");
                    throw e;  
                } 
            }
        }

        System.out.println("Done.");
    }

    private void removeLocalStateStore() {
        final File stateStore = new File(options.valueOf(stateDirOption) + File.separator + options.valueOf(applicationIdOption));
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

    public static void main(String[] args) {
        new StreamsCleanupClient().run(args);
    }

}
