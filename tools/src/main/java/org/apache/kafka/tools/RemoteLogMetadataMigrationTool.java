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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tool to manage remote.log.metadata.version upgrades and migrate the __remote_log_metadata topic.
 *
 * This tool supports two upgrade paths:
 *
 * 1. Version 0 to 1: Upgrades feature and configures topic with retention.ms
 *    - Changes topic cleanup.policy to "compact,delete"
 *    - Sets retention.ms to ensure old-format (null-key) messages expire after specified period
 *    - Sets min.compaction.lag.ms to the same value as retention.ms
 *    - CRITICAL: This dual configuration ensures safe migration:
 *      * retention.ms: Old-format (null-key) messages expire naturally after this period
 *      * min.compaction.lag.ms: Log cleaner waits this long before compacting
 *      Without these settings, the log cleaner could immediately delete null-key messages during compaction,
 *      causing data loss. By setting both to the same value, null-key messages expire via retention
 *      BEFORE compaction begins.
 *
 * 2. Version 1 to 2: Validates no null-key messages exist, then upgrades to compact-only cleanup policy
 *    - Scans the entire topic for null-key messages
 *    - Only proceeds if no null-key messages are found
 *    - Changes cleanup.policy to "compact" (removes "delete")
 *    - Removes min.compaction.lag.ms override (no longer needed)
 *    - Removes retention.ms override (compact-only topics retain all data)
 *
 * Usage:
 *   # Upgrade from version 0 to 1
 *   kafka-remote-log-metadata-migration.sh --bootstrap-server localhost:9092 --upgrade-to-v1 --retention-ms 1209600000
 *
 *   # Upgrade from version 1 to 2 (with validation)
 *   kafka-remote-log-metadata-migration.sh --bootstrap-server localhost:9092 --check --upgrade-to-v2
 *
 *   # Force upgrade to version 2 (skip validation)
 *   kafka-remote-log-metadata-migration.sh --bootstrap-server localhost:9092 --check --upgrade-to-v2 --force
 *
 * Exit codes:
 *   0 - Success (no null-key messages found, or operation completed successfully)
 *   1 - Failure (null-key messages found, or error occurred)
 */
public class RemoteLogMetadataMigrationTool {
    private static final String METADATA_TOPIC = "__remote_log_metadata";

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (HelpScreenException e) {
            return 0;
        } catch (ArgumentParserException e) {
            System.err.println("Command line error: " + e.getMessage() + ". Type --help for help.");
            return 1;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("kafka-remote-log-metadata-migration")
            .defaultHelp(true)
            .description("Tool to manage remote.log.metadata.version upgrades and migrate the __remote_log_metadata topic.");

        parser.addArgument("--bootstrap-server")
            .required(true)
            .help("REQUIRED: A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.");

        parser.addArgument("--command-config")
            .type(Arguments.fileType())
            .help("Property file containing configs to be passed to Admin/Consumer Client.");

        parser.addArgument("--upgrade-to-v1")
            .action(Arguments.storeTrue())
            .help("Upgrade from remote.log.metadata.version=0 to version 1, and configure topic with min.compaction.lag.ms.");

        parser.addArgument("--check")
            .action(Arguments.storeTrue())
            .help("Check if the topic contains any messages with null keys. This is required before upgrading to version 2.");

        parser.addArgument("--upgrade-to-v2")
            .action(Arguments.storeTrue())
            .help("Upgrade to remote.log.metadata.version=2 after validation. Requires --check and --auto-upgrade.");

        parser.addArgument("--auto-upgrade")
            .action(Arguments.storeTrue())
            .help("Automatically upgrade to version 2 if validation passes. Must be used with --check --upgrade-to-v2. " +
                  "This is a safety flag to prevent accidental upgrades.");

        parser.addArgument("--force")
            .action(Arguments.storeTrue())
            .help("Force upgrade to version 2 even if null-key messages are found. Use with caution: null-key messages will be lost during compaction.");

        parser.addArgument("--retention-ms")
            .type(Long.class)
            .setDefault(1209600000L)
            .help("Retention period in milliseconds for the __remote_log_metadata topic when upgrading to version 1 (default: 1209600000, which is 14 days). " +
                  "This parameter is CRITICAL: it serves two purposes: " +
                  "1) retention.ms: Ensures old-format (null-key) messages expire and are deleted after this period. " +
                  "2) min.compaction.lag.ms: Set to the same value to prevent log cleaner from compacting before old messages expire. " +
                  "Once the topic cleanup policy is changed to 'compact,delete', the log cleaner could immediately delete null-key messages during compaction. " +
                  "By setting both retention.ms and min.compaction.lag.ms to the same value, we ensure null-key messages expire naturally via retention " +
                  "before the log cleaner begins compacting. This prevents data loss during the migration period. Used with --upgrade-to-v1.");

        parser.addArgument("--timeout-ms")
            .type(Long.class)
            .setDefault(60000L)
            .help("Maximum time in milliseconds to wait while checking for messages (default: 60000). Used with --check.");

        Namespace namespace = parser.parseArgs(args);

        String bootstrapServers = namespace.getString("bootstrap_server");
        String commandConfig = namespace.getString("command_config");
        boolean upgradeToV1 = namespace.getBoolean("upgrade_to_v1");
        boolean check = namespace.getBoolean("check");
        boolean upgradeToV2 = namespace.getBoolean("upgrade_to_v2");
        boolean autoUpgrade = namespace.getBoolean("auto_upgrade");
        boolean force = namespace.getBoolean("force");
        long retentionMs = namespace.getLong("retention_ms");
        long timeoutMs = namespace.getLong("timeout_ms");

        Properties props = new Properties();
        if (commandConfig != null) {
            try {
                props = Utils.loadProps(commandConfig);
            } catch (java.io.IOException e) {
                throw new TerseException("Failed to load properties from file: " + commandConfig + ". Error: " + e.getMessage());
            }
        }

        if (upgradeToV2 && !check) {
            throw new TerseException("--upgrade-to-v2 requires --check to be specified.");
        }

        if (autoUpgrade && !upgradeToV2) {
            throw new TerseException("--auto-upgrade can only be used with --upgrade-to-v2.");
        }

        if (force && !upgradeToV2) {
            throw new TerseException("--force can only be used with --upgrade-to-v2.");
        }

        if (upgradeToV1 && check) {
            throw new TerseException("Cannot specify both --upgrade-to-v1 and --check. Use --upgrade-to-v1 for 0->1 upgrade, or --check for 1->2 validation.");
        }

        if (upgradeToV1) {
            performUpgradeToV1(bootstrapServers, props, retentionMs);
        } else if (check) {
            checkForNullKeyMessages(bootstrapServers, props, timeoutMs, upgradeToV2, autoUpgrade, force);
        } else {
            throw new TerseException("No operation specified. Use --upgrade-to-v1 for version 0->1 upgrade, or --check for version 1->2 validation.");
        }
    }

    private static void performUpgradeToV1(String bootstrapServers, Properties baseProps, long retentionMs) throws Exception {
        System.out.println("Initiating upgrade to remote.log.metadata.version=1...");
        System.out.println();

        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (Admin admin = Admin.create(adminProps)) {
            // Check current version
            org.apache.kafka.clients.admin.FeatureMetadata featureMetadata =
                admin.describeFeatures().featureMetadata().get();

            org.apache.kafka.clients.admin.FinalizedVersionRange versionRange =
                featureMetadata.finalizedFeatures().get(org.apache.kafka.server.common.RemoteLogMetadataVersion.FEATURE_NAME);

            short currentVersion = (versionRange != null) ? versionRange.maxVersionLevel() : 0;

            System.out.println("Current remote.log.metadata.version: " + currentVersion);

            if (currentVersion != 0) {
                if (currentVersion == 1) {
                    System.out.println("ℹ️  Already at version 1. No upgrade needed.");
                    return;
                } else {
                    throw new TerseException("Current version is " + currentVersion + ". This command is for upgrading from version 0 to 1.");
                }
            }

            // First, update topic configurations before upgrading feature
            // This ensures the controller doesn't overwrite with hardcoded values
            long retentionDays = retentionMs / (24 * 60 * 60 * 1000L);

            System.out.println("Pre-configuring __remote_log_metadata topic...");
            System.out.println("  - cleanup.policy=compact,delete");
            System.out.println("  - retention.ms=" + retentionMs + " (" + retentionDays + " days)");
            System.out.println("  - min.compaction.lag.ms=" + retentionMs + " (same as retention.ms, " + retentionDays + " days)");
            System.out.println();
            System.out.println("IMPORTANT: retention.ms and min.compaction.lag.ms are critical for safe migration.");
            System.out.println("Setting retention.ms=" + retentionMs + "ms ensures old-format (null-key) messages expire after " + retentionDays + " days.");
            System.out.println("Setting min.compaction.lag.ms to the same value ensures the log cleaner waits " + retentionDays + " days before compacting,");
            System.out.println("allowing null-key messages to expire naturally via retention before compaction begins.");
            System.out.println("This prevents data loss during the migration period.");
            System.out.println();

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);

            List<AlterConfigOp> configOps = new java.util.ArrayList<>();
            configOps.add(new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact,delete"), AlterConfigOp.OpType.SET));
            configOps.add(new AlterConfigOp(new ConfigEntry("retention.ms", String.valueOf(retentionMs)), AlterConfigOp.OpType.SET));
            configOps.add(new AlterConfigOp(new ConfigEntry("min.compaction.lag.ms", String.valueOf(retentionMs)), AlterConfigOp.OpType.SET));

            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(topicResource, configOps);

            admin.incrementalAlterConfigs(configs).all().get();
            System.out.println("✅ Topic configurations updated successfully.");
            System.out.println();

            // Now upgrade feature to version 1
            // The controller will check the topic config and skip updates since it's already correct
            System.out.println("Upgrading feature to version 1...");
            Map<String, FeatureUpdate> updates = new HashMap<>();
            updates.put(
                org.apache.kafka.server.common.RemoteLogMetadataVersion.FEATURE_NAME,
                new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE)
            );

            admin.updateFeatures(updates, new UpdateFeaturesOptions()).all().get();
            System.out.println("✅ Feature upgraded to version 1 successfully.");
            System.out.println();
            System.out.println("✅ Upgrade to version 1 completed successfully!");
            System.out.println();
            System.out.println("==================== NEXT STEPS ====================");
            System.out.println();
            System.out.println("Wait for the retention period (" + retentionDays + " days) before upgrading to version 2.");
            System.out.println();
            System.out.println("After waiting, run validation and upgrade to version 2:");
            System.out.println("  kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --check --auto-upgrade");
            System.out.println();
            System.out.println("The validation will:");
            System.out.println("  - Scan for remaining null-key messages");
            System.out.println("  - Show estimated cleanup time based on the last null-key message timestamp");
            System.out.println("  - Suggest retry time if null-key messages still exist");
            System.out.println();
            System.out.println("===================================================");
        }
    }

    private static Properties createConsumerProperties(Properties baseProps, String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.putAll(baseProps);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "remote-log-metadata-migration-tool-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return consumerProps;
    }

    private static void printCheckHeader(String bootstrapServers, long timeoutMs, boolean upgradeToV2, boolean force) {
        System.out.println("Checking __remote_log_metadata topic for messages with null keys...");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Timeout: " + timeoutMs + "ms");
        if (upgradeToV2) {
            System.out.println("Upgrade to V2: ENABLED (will upgrade to version 2 if validation passes)");
            if (force) {
                System.out.println("Force mode: ENABLED (will upgrade even if null-key messages are found)");
            }
        }
        System.out.println();
    }

    private static void printTopicConfigurationReminder(Admin admin) {
        try {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);
            Config topicConfig = admin.describeConfigs(Collections.singleton(topicResource))
                .all().get().get(topicResource);

            ConfigEntry retentionMsEntry = topicConfig.get(TopicConfig.RETENTION_MS_CONFIG);
            ConfigEntry minCompactionLagMsEntry = topicConfig.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG);

            if (retentionMsEntry != null && retentionMsEntry.value() != null) {
                long retentionMs = Long.parseLong(retentionMsEntry.value());
                long retentionDays = retentionMs / (24 * 60 * 60 * 1000L);

                System.out.println("========== IMPORTANT REMINDER ==========");
                System.out.println();
                System.out.println("Current __remote_log_metadata topic configuration:");
                System.out.println("  - retention.ms=" + retentionMs + "ms (" + retentionDays + " days)");
                if (minCompactionLagMsEntry != null && minCompactionLagMsEntry.value() != null) {
                    long minCompactionLagMs = Long.parseLong(minCompactionLagMsEntry.value());
                    long minCompactionLagDays = minCompactionLagMs / (24 * 60 * 60 * 1000L);
                    System.out.println("  - min.compaction.lag.ms=" + minCompactionLagMs + "ms (" + minCompactionLagDays + " days)");
                }
                System.out.println();
                System.out.println("Before proceeding with this validation, ensure that:");
                System.out.println("1. At least " + retentionDays + " days have passed since upgrading to version 1");
                System.out.println("2. This allows all old-format (null-key) messages to expire via retention");
                System.out.println("3. The log cleaner has NOT compacted the topic yet (prevented by min.compaction.lag.ms)");
                System.out.println();
                System.out.println("If you upgraded to version 1 recently (less than " + retentionDays + " days ago),");
                System.out.println("you should WAIT before running this validation to avoid false negatives.");
                System.out.println();
                System.out.println("========================================");
                System.out.println();
            }
        } catch (Exception e) {
            System.out.println("Warning: Could not retrieve topic configuration. Proceeding with validation anyway.");
            System.out.println("Error: " + e.getMessage());
            System.out.println();
        }
    }

    private static void printRetrySuggestion(Admin admin, long lastNullKeyTimestamp) {
        try {
            long currentTime = System.currentTimeMillis();
            long messageAgeMs = currentTime - lastNullKeyTimestamp;
            long messageAgeHours = messageAgeMs / (60 * 60 * 1000L);

            System.out.println("Last null-key message timestamp: " + lastNullKeyTimestamp + " (" + new java.util.Date(lastNullKeyTimestamp) + ")");
            System.out.println("Last null-key message age: " + messageAgeHours + " hours");
            System.out.println();

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, METADATA_TOPIC);
            Config topicConfig = admin.describeConfigs(Collections.singleton(topicResource))
                .all().get().get(topicResource);
            ConfigEntry retentionMsEntry = topicConfig.get(TopicConfig.RETENTION_MS_CONFIG);

            if (retentionMsEntry != null && retentionMsEntry.value() != null) {
                long retentionMs = Long.parseLong(retentionMsEntry.value());
                long retentionHours = retentionMs / (60 * 60 * 1000L);
                long remainingMs = retentionMs - messageAgeMs;

                if (remainingMs > 0) {
                    long remainingHours = (long) Math.ceil(remainingMs / (60.0 * 60 * 1000));
                    long retryTimestamp = lastNullKeyTimestamp + retentionMs;

                    System.out.println("Topic retention.ms: " + retentionMs + "ms (" + retentionHours + " hours)");
                    System.out.println();
                    System.out.println("💡 SUGGESTION:");
                    System.out.println("  Estimated cleanup time: " + new java.util.Date(retryTimestamp));
                    System.out.println("  Remaining wait time: approximately " + remainingHours + " hours");
                    System.out.println();
                    System.out.println("  Please retry this validation after the estimated cleanup time.");
                    System.out.println();
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            // Ignore errors when trying to get retry suggestion (ExecutionException, InterruptedException, etc.)
        }
    }

    private static ScanResult scanMessagesForNullKeys(KafkaConsumer<byte[], byte[]> consumer, long timeoutMs) {
        List<TopicPartition> partitions = consumer.partitionsFor(METADATA_TOPIC)
            .stream()
            .map(info -> new TopicPartition(info.topic(), info.partition()))
            .toList();

        if (partitions.isEmpty()) {
            return new ScanResult(0, 0, -1);
        }

        System.out.println("Found " + partitions.size() + " partition(s) in " + METADATA_TOPIC);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        AtomicLong totalMessages = new AtomicLong(0);
        AtomicLong nullKeyMessages = new AtomicLong(0);
        long lastNullKeyTimestamp = -1;
        long startTime = System.currentTimeMillis();
        boolean hasMoreRecords = true;

        System.out.println("Scanning messages...");

        while (hasMoreRecords && (System.currentTimeMillis() - startTime) < timeoutMs) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));

            if (records.isEmpty()) {
                hasMoreRecords = checkHasMoreRecords(consumer, partitions);
            } else {
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    totalMessages.incrementAndGet();

                    if (record.key() == null) {
                        nullKeyMessages.incrementAndGet();
                        lastNullKeyTimestamp = Math.max(lastNullKeyTimestamp, record.timestamp());
                        System.out.println("⚠️  Found message with null key at partition=" + record.partition() +
                            ", offset=" + record.offset() + ", timestamp=" + record.timestamp());
                    }

                    if (totalMessages.get() % 10000 == 0) {
                        System.out.println("Scanned " + totalMessages.get() + " messages so far...");
                    }
                }
            }
        }

        return new ScanResult(totalMessages.get(), nullKeyMessages.get(), lastNullKeyTimestamp);
    }

    private static boolean checkHasMoreRecords(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            long position = consumer.position(partition);
            long endOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
            if (position < endOffset) {
                return true;
            }
        }
        return false;
    }

    private static void handleScanResults(ScanResult result, Admin admin, String bootstrapServers, Properties baseProps,
                                          boolean upgradeToV2, boolean autoUpgrade, boolean force) throws Exception {
        System.out.println();
        System.out.println("Scan completed.");
        System.out.println("Total messages scanned: " + result.totalMessages);
        System.out.println("Messages with null keys: " + result.nullKeyMessages);
        System.out.println();

        if (result.nullKeyMessages > 0) {
            handleNullKeysFound(result, admin, bootstrapServers, baseProps, upgradeToV2, autoUpgrade, force);
        } else {
            handleNoNullKeysFound(bootstrapServers, baseProps, upgradeToV2, autoUpgrade);
        }
    }

    private static void handleNullKeysFound(ScanResult result, Admin admin, String bootstrapServers,
                                            Properties baseProps, boolean upgradeToV2, boolean autoUpgrade, boolean force) throws Exception {
        System.out.println("❌ VALIDATION FAILED: Found " + result.nullKeyMessages + " message(s) with null keys.");
        System.out.println();

        if (result.lastNullKeyTimestamp > 0) {
            printRetrySuggestion(admin, result.lastNullKeyTimestamp);
        }

        System.out.println("Action required:");
        System.out.println("1. Wait for null-key messages to expire based on retention.ms setting");
        System.out.println("2. Then run this tool again to verify all null-key messages are gone");
        System.out.println("3. Only then proceed with the upgrade to remote.log.metadata.version=2");
        System.out.println();

        if (force) {
            System.out.println("⚠️  WARNING: --force flag is enabled. Proceeding with upgrade despite null-key messages.");
            System.out.println("⚠️  These null-key messages will be LOST during compaction!");
            System.out.println();
            if (upgradeToV2 && autoUpgrade) {
                performUpgradeToV2(bootstrapServers, baseProps);
            }
        } else {
            System.out.println("To force upgrade despite null-key messages (NOT RECOMMENDED), use --force flag.");
            throw new TerseException("Cannot upgrade to version 2: null-key messages found in " + METADATA_TOPIC);
        }
    }

    private static void handleNoNullKeysFound(String bootstrapServers, Properties baseProps, boolean upgradeToV2, boolean autoUpgrade) throws Exception {
        System.out.println("✅ VALIDATION PASSED: No null-key messages found.");
        System.out.println("✅ Safe to upgrade to remote.log.metadata.version=2.");
        System.out.println();

        if (upgradeToV2 && autoUpgrade) {
            performUpgradeToV2(bootstrapServers, baseProps);
        } else if (upgradeToV2 && !autoUpgrade) {
            System.out.println("To complete the upgrade, add --auto-upgrade flag:");
            System.out.println("  kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --check --upgrade-to-v2 --auto-upgrade");
        } else {
            System.out.println("To upgrade, run:");
            System.out.println("  kafka-features.sh upgrade --bootstrap-server " + bootstrapServers + " --feature remote.log.metadata.version=2");
            System.out.println();
            System.out.println("Or run this tool with --upgrade-to-v2 --auto-upgrade to automatically upgrade:");
            System.out.println("  kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --check --upgrade-to-v2 --auto-upgrade");
        }
    }

    private static class ScanResult {
        final long totalMessages;
        final long nullKeyMessages;
        final long lastNullKeyTimestamp;

        ScanResult(long totalMessages, long nullKeyMessages, long lastNullKeyTimestamp) {
            this.totalMessages = totalMessages;
            this.nullKeyMessages = nullKeyMessages;
            this.lastNullKeyTimestamp = lastNullKeyTimestamp;
        }
    }

    private static void checkForNullKeyMessages(String bootstrapServers, Properties baseProps, long timeoutMs, boolean upgradeToV2, boolean autoUpgrade, boolean force) throws Exception {
        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put("bootstrap.servers", bootstrapServers);

        // Check current version if upgrade is requested
        if (upgradeToV2 && autoUpgrade) {
            try (Admin admin = Admin.create(adminProps)) {
                org.apache.kafka.clients.admin.FeatureMetadata featureMetadata =
                    admin.describeFeatures().featureMetadata().get();

                org.apache.kafka.clients.admin.FinalizedVersionRange versionRange =
                    featureMetadata.finalizedFeatures().get(org.apache.kafka.server.common.RemoteLogMetadataVersion.FEATURE_NAME);

                short currentVersion = (versionRange != null) ? versionRange.maxVersionLevel() : 0;

                if (currentVersion >= 2) {
                    System.out.println("✅ Cluster is already at remote.log.metadata.version=" + currentVersion);
                    System.out.println("✅ No upgrade needed.");
                    return;
                }

                if (currentVersion == 0) {
                    throw new TerseException(
                        "Cannot upgrade directly from version 0 to version 2. " +
                        "Must upgrade to version 1 first using: " +
                        "kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --upgrade-to-v1");
                }
            }
        }

        try (Admin admin = Admin.create(adminProps)) {
            printTopicConfigurationReminder(admin);
        }

        Properties consumerProps = createConsumerProperties(baseProps, bootstrapServers);

        printCheckHeader(bootstrapServers, timeoutMs, upgradeToV2, force);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = consumer.partitionsFor(METADATA_TOPIC)
                .stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();

            if (partitions.isEmpty()) {
                System.out.println("✅ Topic " + METADATA_TOPIC + " does not exist or has no partitions.");
                System.out.println("✅ No null-key messages found. Safe to upgrade to version 2.");
                return;
            }

            ScanResult result = scanMessagesForNullKeys(consumer, timeoutMs);

            try (Admin admin = Admin.create(adminProps)) {
                handleScanResults(result, admin, bootstrapServers, baseProps, upgradeToV2, autoUpgrade, force);
            }
        }
    }

    private static void performUpgradeToV2(String bootstrapServers, Properties baseProps) throws Exception {
        System.out.println("Initiating upgrade to remote.log.metadata.version=2...");
        System.out.println();

        Properties adminProps = new Properties();
        adminProps.putAll(baseProps);
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (Admin admin = Admin.create(adminProps)) {
            // First, check current version
            org.apache.kafka.clients.admin.FeatureMetadata featureMetadata =
                admin.describeFeatures().featureMetadata().get();

            org.apache.kafka.clients.admin.FinalizedVersionRange versionRange =
                featureMetadata.finalizedFeatures().get(org.apache.kafka.server.common.RemoteLogMetadataVersion.FEATURE_NAME);

            short currentVersion = (versionRange != null) ? versionRange.maxVersionLevel() : 0;

            System.out.println("Current remote.log.metadata.version: " + currentVersion);

            if (currentVersion == 0) {
                throw new TerseException(
                    "Cannot upgrade directly from version 0 to version 2. " +
                    "Must upgrade to version 1 first using: " +
                    "kafka-remote-log-metadata-migration.sh --bootstrap-server " + bootstrapServers + " --upgrade-to-v1");
            }

            if (currentVersion == 2) {
                System.out.println("ℹ️  Already at version 2. No upgrade needed.");
                return;
            }

            if (currentVersion != 1) {
                throw new TerseException("Unexpected current version: " + currentVersion + ". Expected version 1.");
            }

            // Perform the upgrade from 1 to 2
            // The controller will automatically update topic configurations:
            //   - Change cleanup.policy to 'compact' (removing 'delete')
            //   - Remove min.compaction.lag.ms override
            //   - Remove retention.ms override
            System.out.println("Upgrading from version 1 to version 2...");
            Map<String, FeatureUpdate> updates = new HashMap<>();
            updates.put(
                org.apache.kafka.server.common.RemoteLogMetadataVersion.FEATURE_NAME,
                new FeatureUpdate((short) 2, FeatureUpdate.UpgradeType.UPGRADE)
            );

            admin.updateFeatures(updates, new UpdateFeaturesOptions()).all().get();

            System.out.println();
            System.out.println("✅ Successfully upgraded to remote.log.metadata.version=2!");
            System.out.println();
            System.out.println("The controller has automatically updated __remote_log_metadata topic configuration:");
            System.out.println("  - cleanup.policy changed to 'compact' (compact-only)");
            System.out.println("  - min.compaction.lag.ms override removed");
            System.out.println("  - retention.ms override removed");
            System.out.println();
            System.out.println("All metadata messages now have proper keys and will be retained indefinitely via compaction.");
        }
    }
}
