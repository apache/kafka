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
package org.apache.kafka.tools;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.apache.kafka.connect.util.TopicAdmin;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ConnectInternalTopics {

    private static final String CREATE_COMMAND = "create";

    public static void main(String[] args) {
        Exit.exit(mainNoExit(args, System.out, System.err));
    }

    static int mainNoExit(String[] args, PrintStream out, PrintStream err) {
        var parser = parser();
        try {
            var namespace = parser.parseArgs(args);
            var workerProperties = parseWorkerProperties(parser, namespace);
            out.println("Parsed arguments and loaded worker properties");
            execute(parser, namespace, workerProperties, out);
            out.println("Command executed successfully");
            return 0;
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return 1;
        } catch (TerseException | ConfigException e) {
            err.println(e.getMessage());
            return 2;
        } catch (Throwable e) {
            err.println("Unexpected error: " + e.getMessage());
            err.println(Utils.stackTrace(e));
            return 3;
        }
    }

    private static void execute(ArgumentParser parser, Namespace namespace, Map<String, String> workerProperties, PrintStream out) throws ArgumentParserException {
        var subcommand = namespace.getString("subcommand");
        out.println("Subcommand: " + subcommand);
        if (subcommand == null) {
            throw new ArgumentParserException("No subcommand specified", parser);
        }
        if (CREATE_COMMAND.equals(subcommand)) {
            var internalTopicsConfig = new InternalTopicsConfig(workerProperties);
            internalTopicsConfig.validateTopicNames();
            out.println("Running create command for internal topics");
            runCommand(internalTopicsConfig, out);
        } else {
            throw new ArgumentParserException("Unrecognized subcommand: '" + subcommand + "'", parser);
        }
    }

    private static void runCommand(InternalTopicsConfig config, PrintStream out) {
        var adminProps = new HashMap<>(config.originals());
        out.println("Admin properties loaded for topic admin");
        try (var sharedAdmin = new SharedTopicAdmin(adminProps)) {
            createInternalTopic(sharedAdmin, buildOffsetTopicSettings(config, out), out);
            createInternalTopic(sharedAdmin, buildConfigTopicSettings(config, out), out);
            createInternalTopic(sharedAdmin, buildStatusTopicSettings(config, out), out);
        }
    }

    private static void createInternalTopic(SharedTopicAdmin sharedAdmin, TopicSettings settings, PrintStream out) {
        var topicDescription = TopicAdmin.defineTopic(settings.topicName)
                .config(settings.topicSettings)
                .compacted()
                .partitions(settings.partitions)
                .replicationFactor(settings.replicationFactor)
                .build();
        var created = sharedAdmin.topicAdmin().createTopics(topicDescription);
        if (created.contains(settings.topicName)) {
            out.println("Created internal topic: " + settings.topicName);
        } else {
            out.println("Internal topic already exists: " + settings.topicName);
        }
    }

    private static TopicSettings buildOffsetTopicSettings(InternalTopicsConfig config, PrintStream out) {
        return new TopicSettings(
                config.getString(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG),
                config.topicSettings(DistributedConfig.OFFSET_STORAGE_PREFIX, out),
                config.getInt(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG),
                config.getShort(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
        );
    }

    private static TopicSettings buildConfigTopicSettings(InternalTopicsConfig config, PrintStream out) {
        return new TopicSettings(
                config.getString(DistributedConfig.CONFIG_TOPIC_CONFIG),
                config.topicSettings(DistributedConfig.CONFIG_STORAGE_PREFIX, out),
                1,
                config.getShort(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG)
        );
    }

    private static TopicSettings buildStatusTopicSettings(InternalTopicsConfig config, PrintStream out) {
        return new TopicSettings(
                config.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG),
                config.topicSettings(DistributedConfig.STATUS_STORAGE_PREFIX, out),
                config.getInt(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG),
                config.getShort(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG)
        );
    }

    private record TopicSettings(String topicName, Map<String, Object> topicSettings, int partitions,
                                short replicationFactor) {
    }

    private static Map<String, String> parseWorkerProperties(ArgumentParser parser, Namespace namespace) throws ArgumentParserException, TerseException {
        String workerConfigPath = namespace.getString("worker_config");
        if (workerConfigPath == null || workerConfigPath.isBlank()) {
            throw new ArgumentParserException("--worker-config must be specified and non-blank", parser);
        }

        try {
            return Utils.propsToStringMap(Utils.loadProps(workerConfigPath));
        } catch (IOException e) {
            throw new TerseException("Unable to read worker config at " + workerConfigPath);
        }
    }

    private static ArgumentParser parser() {
        var parser = ArgumentParsers.newArgumentParser("connect-internal-topics")
                .defaultHelp(true)
                .description("Manage internal topics required by Kafka Connect clusters (config, status, and offset topics).");

        parser.addSubparsers()
                .description("Create internal topics required for Kafka Connect operation using the provided worker configuration.")
                .dest("subcommand")
                .addParser(CREATE_COMMAND)
                .addArgument("--worker-config")
                .setDefault("")
                .type(String.class)
                .action(store())
                .help("Path to a Connect worker configuration file. This file must define the internal topic names and connection information for the Kafka cluster.");

        return parser;
    }

    private static class InternalTopicsConfig extends AbstractConfig {
        private static final ConfigDef CONFIG_DEF = new ConfigDef()
                .define(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "")
                .define(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        25,
                        ConfigDef.Importance.LOW,
                        "")
                .define(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        (short) 3,
                        ConfigDef.Importance.LOW,
                        "")
                .define(DistributedConfig.CONFIG_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "")
                .define(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        (short) 3,
                        ConfigDef.Importance.LOW,
                        "")
                .define(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "")
                .define(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        5,
                        ConfigDef.Importance.LOW,
                        "")
                .define(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG,
                        ConfigDef.Type.SHORT,
                        (short) 3,
                        ConfigDef.Importance.LOW,
                        "");

        InternalTopicsConfig(Map<String, String> props) {
            super(CONFIG_DEF, props, false);
        }

        void validateTopicNames() {
            validateTopicName(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
            validateTopicName(DistributedConfig.CONFIG_TOPIC_CONFIG);
            validateTopicName(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
        }

        private void validateTopicName(String config) {
            var value = getString(config);
            if (value == null || value.trim().isEmpty()) {
                throw new ConfigException("Must specify non-empty value for required internal topic config: '" + config + "'.");
            }
        }

        private Map<String, Object> topicSettings(String prefix, PrintStream out) {
            var result = originalsWithPrefix(prefix);
            if (DistributedConfig.CONFIG_STORAGE_PREFIX.equals(prefix) && result.containsKey(DistributedConfig.PARTITIONS_SUFFIX)) {
                out.println("Ignoring '" + prefix + DistributedConfig.PARTITIONS_SUFFIX + "=" + result.get(DistributedConfig.PARTITIONS_SUFFIX) + "' setting, since config topic partitions is always 1");
            }
            var removedPolicy = result.remove(TopicConfig.CLEANUP_POLICY_CONFIG);
            if (removedPolicy != null) {
                out.println("Ignoring '" + prefix + "cleanup.policy=" + removedPolicy + "' setting, since compaction is always used");
            }
            result.remove(DistributedConfig.TOPIC_SUFFIX);
            result.remove(DistributedConfig.REPLICATION_FACTOR_SUFFIX);
            result.remove(DistributedConfig.PARTITIONS_SUFFIX);
            return result;
        }
    }
}
