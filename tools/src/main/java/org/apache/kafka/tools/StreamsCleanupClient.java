/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * {@link StreamsCleanupClient} resets a Kafka Streams application to the very beginning. Thus, an application can
 * reprocess the whole input from scratch.
 * <p>
 * This includes, setting source topic offsets to zero, skipping over all intermediate user topics,
 * and deleting all internally created topics.
 */
public class StreamsCleanupClient {
    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_ERROR = 1;

    private static OptionSpec<String> bootstrapServerOption;
    private static OptionSpec<String> zookeeperOption;
    private static OptionSpec<String> applicationIdOption;
    private static OptionSpec<String> sourceTopicsOption;
    private static OptionSpec<String> intermediateTopicsOption;

    private OptionSet options = null;

    public int run(final String[] args) {
        ZkUtils zkUtils = null;

        int exitCode = EXIT_CODE_SUCCESS;
        try {
            parseArguments(args);

            zkUtils = ZkUtils.apply(this.options.valueOf(zookeeperOption),
                30000,
                30000,
                JaasUtils.isZkSecurityEnabled());

            final List<String> allTopics = scala.collection.JavaConversions.seqAsJavaList(zkUtils.getAllTopics());

            resetSourceTopicOffsets();
            seekToEndIntermediateTopics(zkUtils, allTopics);
            deleteInternalTopics(zkUtils, allTopics);
        } catch (final Exception e) {
            exitCode = EXIT_CODE_ERROR;
            System.err.println(e.getMessage());
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }

        return exitCode;
    }

    private void parseArguments(final String[] args) throws IOException {
        final OptionParser optionParser = new OptionParser();
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id)")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("id")
            .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-server", "Format: <host:port>")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:9092")
            .describedAs("url");
        zookeeperOption = optionParser.accepts("zookeeper", "Format: <host:port>")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:2181")
            .describedAs("url");
        sourceTopicsOption = optionParser.accepts("source-topics", "Comma separated list of user source topics")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
        intermediateTopicsOption = optionParser.accepts("intermediate-topics", "Comma separated list of intermediate user topics")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");

        try {
            this.options = optionParser.parse(args);
        } catch (final OptionException e) {
            optionParser.printHelpOn(System.err);
            throw e;
        }
    }

    private void resetSourceTopicOffsets() {
        final List<String> sourceTopics = this.options.valuesOf(sourceTopicsOption);

        if (sourceTopics.size() == 0) {
            System.out.println("No source topics specified.");
            return;
        }

        System.out.println("Resetting offsets to zero for topics " + sourceTopics);

        final Properties config = new Properties();
        config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.options.valueOf(bootstrapServerOption));
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.options.valueOf(applicationIdOption));
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        final KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        try {
            client.subscribe(sourceTopics);
            client.poll(1);

            for (final TopicPartition partition : client.assignment()) {
                client.seek(partition, 0);
            }
            client.commitSync();
        } catch (final RuntimeException e) {
            System.err.println("Resetting source topic offsets failed.");
            throw e;
        } finally {
            client.close();
        }
    }

    private void seekToEndIntermediateTopics(final ZkUtils zkUtils, final List<String> allTopics) {
        if (this.options.has(intermediateTopicsOption)) {
            System.out.println("Seek-to-end for intermediate user topics.");

            final Properties config = new Properties();
            config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.options.valueOf(bootstrapServerOption));
            config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.options.valueOf(applicationIdOption));
            config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            for (final String topic : this.options.valuesOf(intermediateTopicsOption)) {
                if (allTopics.contains(topic)) {
                    System.out.println("Topic: " + topic);

                    final KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                    try {
                        client.subscribe(Collections.singleton(topic));
                        client.poll(1);

                        final Set<TopicPartition> partitions = client.assignment();
                        client.seekToEnd(partitions);
                        client.commitSync();
                    } catch (final RuntimeException e) {
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

    private void deleteInternalTopics(final ZkUtils zkUtils, final List<String> allTopics) {
        System.out.println("Deleting internal topics.");

        for (final String topic : allTopics) {
            if (isInternalStreamsTopic(topic)) {
                final TopicCommand.TopicCommandOptions commandOptions = new TopicCommand.TopicCommandOptions(new String[]{
                    "--zookeeper", this.options.valueOf(zookeeperOption),
                    "--delete", "--topic", topic});
                try {
                    TopicCommand.deleteTopic(zkUtils, commandOptions);
                } catch (final RuntimeException e) {
                    System.err.println("Deleting internal topic " + topic + " failed");
                    throw e;
                }
            }
        }

        System.out.println("Done.");
    }

    private boolean isInternalStreamsTopic(final String topicName) {
        return topicName.startsWith(this.options.valueOf(applicationIdOption) + "-")
            && (topicName.endsWith("-changelog") || topicName.endsWith("-repartition"));
    }

    public static void main(final String[] args) {
        System.exit(new StreamsCleanupClient().run(args));
    }

}
