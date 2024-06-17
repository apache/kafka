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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.SupportedVersionRange;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.CommandLineUtils;

import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class FeatureCommand {
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
            System.err.printf("Command line error: " + e.getMessage() + ". Type --help for help.");
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-features")
                .defaultHelp(true)
                .description("This tool manages feature flags in Kafka.");
        MutuallyExclusiveGroup bootstrapGroup = parser.addMutuallyExclusiveGroup().required(true);
        bootstrapGroup.addArgument("--bootstrap-server")
                .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.");
        bootstrapGroup.addArgument("--bootstrap-controller")
                .help("A comma-separated list of host:port pairs to use for establishing the connection to the KRaft quorum.");
        parser.addArgument("--command-config")
                .type(Arguments.fileType())
                .help("Property file containing configs to be passed to Admin Client.");
        Subparsers subparsers = parser.addSubparsers().dest("command");
        addDescribeParser(subparsers);
        addUpgradeParser(subparsers);
        addDowngradeParser(subparsers);
        addDisableParser(subparsers);

        Namespace namespace = parser.parseArgsOrFail(args);
        String command = namespace.getString("command");
        String configPath = namespace.getString("command_config");
        Properties properties = (configPath == null) ? new Properties() : Utils.loadProps(configPath);

        CommandLineUtils.initializeBootstrapProperties(properties,
            Optional.ofNullable(namespace.getString("bootstrap_server")),
            Optional.ofNullable(namespace.getString("bootstrap_controller")));

        try (Admin adminClient = Admin.create(properties)) {
            switch (command) {
                case "describe":
                    handleDescribe(adminClient);
                    break;
                case "upgrade":
                    handleUpgrade(namespace, adminClient);
                    break;
                case "downgrade":
                    handleDowngrade(namespace, adminClient);
                    break;
                case "disable":
                    handleDisable(namespace, adminClient);
                    break;
                default:
                    throw new TerseException("Unknown command " + command);
            }
        }
    }

    private static void addDescribeParser(Subparsers subparsers) {
        subparsers.addParser("describe")
                .help("Describes the current active feature flags.");
    }

    private static void addUpgradeParser(Subparsers subparsers) {
        Subparser upgradeParser = subparsers.addParser("upgrade")
                .help("Upgrade one or more feature flags.");
        upgradeParser.addArgument("--metadata")
                .help("The level to which we should upgrade the metadata. For example, 3.3-IV3.")
                .action(store());
        upgradeParser.addArgument("--feature")
                .help("A feature upgrade we should perform, in feature=level format. For example: `metadata.version=5`.")
                .action(append());
        upgradeParser.addArgument("--dry-run")
                .help("Validate this upgrade, but do not perform it.")
                .action(storeTrue());

    }

    private static void addDowngradeParser(Subparsers subparsers) {
        Subparser downgradeParser = subparsers.addParser("downgrade")
                .help("Upgrade one or more feature flags.");
        downgradeParser.addArgument("--metadata")
                .help("The level to which we should downgrade the metadata. For example, 3.3-IV0.")
                .action(store());
        downgradeParser.addArgument("--feature")
                .help("A feature downgrade we should perform, in feature=level format. For example: `metadata.version=5`.")
                .action(append());
        downgradeParser.addArgument("--unsafe")
                .help("Perform this downgrade even if it may irreversibly destroy metadata.")
                .action(storeTrue());
        downgradeParser.addArgument("--dry-run")
                .help("Validate this downgrade, but do not perform it.")
                .action(storeTrue());
    }

    private static void addDisableParser(Subparsers subparsers) {
        Subparser downgradeParser = subparsers.addParser("disable")
                .help("Disable one or more feature flags. This is the same as downgrading the version to zero.");
        downgradeParser.addArgument("--feature")
                .help("A feature flag to disable.")
                .action(append());
        downgradeParser.addArgument("--unsafe")
                .help("Disable this feature flag even if it may irreversibly destroy metadata.")
                .action(storeTrue());
        downgradeParser.addArgument("--dry-run")
                .help("Perform a dry-run of this disable operation.")
                .action(storeTrue());
    }

    static String levelToString(String feature, short level) {
        if (feature.equals(MetadataVersion.FEATURE_NAME)) {
            try {
                return MetadataVersion.fromFeatureLevel(level).version();
            } catch (Throwable e) {
                return "UNKNOWN " + level;
            }
        }
        return String.valueOf(level);
    }

    static void handleDescribe(Admin adminClient) throws ExecutionException, InterruptedException {
        FeatureMetadata featureMetadata = adminClient.describeFeatures().featureMetadata().get();
        featureMetadata.supportedFeatures().keySet().stream().sorted().forEach(feature -> {
            short finalizedLevel = (featureMetadata.finalizedFeatures().get(feature) == null) ? 0 : featureMetadata.finalizedFeatures().get(feature).maxVersionLevel();
            SupportedVersionRange range = featureMetadata.supportedFeatures().get(feature);
            System.out.printf("Feature: %s\tSupportedMinVersion: %s\tSupportedMaxVersion: %s\tFinalizedVersionLevel: %s\tEpoch: %s%n",
                    feature,
                    levelToString(feature, range.minVersion()),
                    levelToString(feature, range.maxVersion()),
                    levelToString(feature, finalizedLevel),
                    (featureMetadata.finalizedFeaturesEpoch().isPresent()) ? featureMetadata.finalizedFeaturesEpoch().get().toString() : "-");
        });
    }

    static String metadataVersionsToString(MetadataVersion first, MetadataVersion last) {
        List<MetadataVersion> versions = Arrays.asList(MetadataVersion.VERSIONS).subList(first.ordinal(), last.ordinal() + 1);
        return versions.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));
    }

    static void handleUpgrade(Namespace namespace, Admin adminClient) throws TerseException {
        handleUpgradeOrDowngrade("upgrade", namespace, adminClient, FeatureUpdate.UpgradeType.UPGRADE);
    }

    static FeatureUpdate.UpgradeType downgradeType(Namespace namespace) {
        Boolean unsafe = namespace.getBoolean("unsafe");
        if (unsafe == null || !unsafe) {
            return FeatureUpdate.UpgradeType.SAFE_DOWNGRADE;
        } else {
            return FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE;
        }
    }

    static void handleDowngrade(Namespace namespace, Admin adminClient) throws TerseException {
        handleUpgradeOrDowngrade("downgrade", namespace, adminClient, downgradeType(namespace));
    }

    static String[] parseNameAndLevel(String input) {
        int equalsIndex = input.indexOf("=");
        if (equalsIndex < 0) {
            throw new RuntimeException("Can't parse feature=level string " + input + ": equals sign not found.");
        }
        String name = input.substring(0, equalsIndex).trim();
        String levelString = input.substring(equalsIndex + 1).trim();
        try {
            Short.parseShort(levelString);
        } catch (Throwable t) {
            throw new RuntimeException("Can't parse feature=level string " + input + ": " +
                    "unable to parse " + levelString + " as a short.");
        }
        return new String[]{name, levelString};
    }

    private static void handleUpgradeOrDowngrade(String op, Namespace namespace, Admin admin, FeatureUpdate.UpgradeType upgradeType) throws TerseException {
        Map<String, FeatureUpdate> updates = new HashMap<>();
        MetadataVersion version;
        String metadata = namespace.getString("metadata");
        if (metadata !=  null) {
            try {
                version = MetadataVersion.fromVersionString(metadata);
            } catch (Throwable e) {
                throw new TerseException("Unsupported metadata.version " + metadata +
                        ". Supported metadata.version are " + metadataVersionsToString(
                        MetadataVersion.MINIMUM_BOOTSTRAP_VERSION, MetadataVersion.latestProduction()));
            }
            updates.put(MetadataVersion.FEATURE_NAME, new FeatureUpdate(version.featureLevel(), upgradeType));
        }

        List<String> features = namespace.getList("feature");
        if (features != null) {
            features.forEach(feature -> {
                String[] nameAndLevel;
                nameAndLevel = parseNameAndLevel(feature);

                if (updates.put(nameAndLevel[0], new FeatureUpdate(Short.parseShort(nameAndLevel[1]), upgradeType)) != null) {
                    throw new RuntimeException("Feature " + nameAndLevel[0] + " was specified more than once.");
                }
            });
        }

        update(op, admin, updates, namespace.getBoolean("dry_run"));
    }

    static void handleDisable(Namespace namespace, Admin adminClient) throws TerseException {
        FeatureUpdate.UpgradeType upgradeType = downgradeType(namespace);
        Map<String, FeatureUpdate> updates = new HashMap<>();

        List<String> features = namespace.getList("feature");
        if (features != null) {
            features.forEach(feature -> {
                if (updates.put(feature, new FeatureUpdate((short) 0, upgradeType)) != null) {
                    throw new RuntimeException("Feature " + feature + " was specified more than once.");
                }
            });
        }

        update("disable", adminClient, updates, namespace.getBoolean("dry_run"));
    }

    private static void update(String op, Admin admin, Map<String, FeatureUpdate> updates, Boolean dryRun) throws TerseException {
        if (updates.isEmpty()) {
            throw new TerseException("You must specify at least one feature to " + op);
        }

        UpdateFeaturesResult result = admin.updateFeatures(updates, new UpdateFeaturesOptions().validateOnly(dryRun));
        Map<String, Optional<Throwable>> errors = new TreeMap<>();
        result.values().forEach((feature, future) -> {
            try {
                future.get();
                errors.put(feature, null);
            } catch (ExecutionException e) {
                errors.put(feature, Optional.ofNullable(e.getCause()));
            } catch (Throwable t) {
                errors.put(feature, Optional.of(t));
            }
        });

        int numFailures = 0;
        for (Map.Entry<String, Optional<Throwable>> feature: errors.entrySet()) {
            short level = updates.get(feature.getKey()).maxVersionLevel();
            Optional<Throwable> maybeThrowable = feature.getValue();
            if (maybeThrowable != null && maybeThrowable.isPresent()) {
                String helper = dryRun ? "Can not " : "Could not ";
                String suffix = (op.equals("disable")) ? "disable " + feature.getKey() : op + " " + feature.getKey() + " to " + level;
                System.out.println(helper + suffix + ". " + maybeThrowable.get().getMessage());
                numFailures++;
            } else {
                String verb = dryRun ? " can be " : " was ";
                String obj = (op.equals("disable")) ? "disabled." : op + "d to " + level + ".";
                System.out.println(feature.getKey() + verb + obj);
            }
        }

        if (numFailures > 0) {
            throw new TerseException(numFailures + " out of " + updates.size() + " operation(s) failed.");
        }
    }
}
