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
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.config.DynamicConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

/**
 * This script can be used to change configs for topics/clients/users/brokers/ips/client-metrics/groups dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: {@code --topic <topic>} OR {@code --entity-type topics --entity-name <topic>}
 *     <li> client: {@code --client <client-id>} OR {@code --entity-type clients --entity-name <client-id>}
 *     <li> user: {@code --user <user-principal>} OR {@code --entity-type users --entity-name <user-principal>}
 *     <li> {@code <user, client>}: {@code --user <user-principal> --client <client-id>} OR
 *                          {@code --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>}
 *     <li> broker: {@code --broker <broker-id>} OR {@code --entity-type brokers --entity-name <broker-id>}
 *     <li> broker-logger: {@code --broker-logger <broker-id>} OR {@code --entity-type broker-loggers --entity-name <broker-id>}
 *     <li> ip: {@code --ip <ip>} OR {@code --entity-type ips --entity-name <ip>}
 *     <li> client-metrics: {@code --client-metrics <name>} OR {@code --entity-type client-metrics --entity-name <name>}
 *     <li> group: {@code --group <group>} OR {@code --entity-type groups --entity-name <group>}
 * </ul>
 * {@code --entity-type <users|clients|brokers|ips> --entity-default} may be specified in place of {@code --entity-type <users|clients|brokers|ips> --entity-name <entityName>}
 * when describing or altering default configuration for users, clients, brokers, or ips, respectively.
 * Alternatively, {@code --user-defaults}, {@code --client-defaults}, {@code --broker-defaults}, or {@code --ip-defaults} may be specified in place of
 * {@code --entity-type <users|clients|brokers|ips> --entity-default}, respectively.
 */
public class ConfigCommand {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigCommand.class);

    private static final String BROKER_DEFAULT_ENTITY_NAME = "";
    private static final int DEFAULT_SCRAM_ITERATIONS = 4096;
    private static final String TOPIC_TYPE = ConfigType.TOPIC.value();
    private static final String CLIENT_METRICS_TYPE = ConfigType.CLIENT_METRICS.value();
    private static final String BROKER_TYPE = ConfigType.BROKER.value();
    private static final String GROUP_TYPE = ConfigType.GROUP.value();
    private static final String USER_TYPE = ConfigType.USER.value();
    private static final String CLIENT_TYPE = ConfigType.CLIENT.value();
    private static final String IP_TYPE = ConfigType.IP.value();

    static final String BROKER_LOGGER_CONFIG_TYPE = "broker-loggers";
    private static final List<String> BROKER_SUPPORTED_CONFIG_TYPES = Stream.concat(
            Stream.of(BROKER_LOGGER_CONFIG_TYPE),
            Stream.of(ConfigType.values()).map(ConfigType::value)
    ).toList();

    public static void main(String[] args) {
        try {
            ConfigCommandOptions opts = new ConfigCommandOptions(args);
            CommandLineUtils.maybePrintHelpOrVersion(opts,
                    "This tool helps to manipulate and describe entity config for a topic, client, user, broker, ip, client-metrics or group");
            opts.checkArgs();
            processCommand(opts);
        } catch (UnsupportedVersionException uve) {
            LOG.debug("Unsupported API encountered in server when executing config command with args '{}'", String.join(" ", args));
            System.err.println(uve.getMessage());
            Exit.exit(1);
        } catch (IllegalArgumentException | InvalidConfigurationException | OptionException e) {
            LOG.debug("Failed config command with args '{}'", String.join(" ", args), e);
            System.err.println(e.getMessage());
            Exit.exit(1);
        } catch (Throwable t) {
            LOG.debug("Error while executing config command with args '{}'", String.join(" ", args), t);
            System.err.println("Error while executing config command with args '" + String.join(" ", args) + "'");
            t.printStackTrace(System.err);
            Exit.exit(1);
        }
    }

    static Properties parseConfigsToBeAdded(ConfigCommandOptions opts) throws IOException {
        Properties props = new Properties();
        if (opts.options.has(opts.addConfigFile)) {
            String file = opts.options.valueOf(opts.addConfigFile);
            props.putAll(Utils.loadProps(file));
        }
        if (opts.options.has(opts.addConfig)) {
            // Split list by commas, but avoid those in [], then into KV pairs
            // Each KV pair is of format key=value, split them into key and value, using -1 as the limit for split() to
            // include trailing empty strings. This is to support empty value (e.g. 'ssl.endpoint.identification.algorithm=')
            String pattern = "(?=[^\\]]*(?:\\[|$))";
            String[][] configsToBeAdded = Stream.of(opts.options.valueOf(opts.addConfig).split("," + pattern))
                    .map(s -> s.split("\\s*=\\s*" + pattern, -1))
                    .toArray(String[][]::new);

            if (Stream.of(configsToBeAdded).anyMatch(config -> config.length != 2)) {
                throw new IllegalArgumentException("Invalid entity config: all configs to be added must be in the format \"key=val\" or  \"key=[val1,val2]\" to group values which contain commas.");
            }

            //Create properties, parsing square brackets from values if necessary
            Stream.of(configsToBeAdded).forEach(pair ->
                props.setProperty(pair[0].trim(), pair[1].replaceAll("\\[?\\]?", "").trim())
            );
        }
        validatePropsKey(props);
        return props;
    }

    static List<String> parseConfigsToBeDeleted(ConfigCommandOptions opts) {
        if (opts.options.has(opts.deleteConfig)) {
            return opts.options.valuesOf(opts.deleteConfig).stream().map(String::trim).toList();
        } else {
            return List.of();
        }
    }

    private static void validatePropsKey(Properties props) {
        props.keySet().forEach(propsKey -> {
            // Allows the '$' symbol to support valid logger names for internal classes (e.g. org.apache.kafka.server.quota.ClientQuotaManager$ThrottledChannelReaper)
            if (!propsKey.toString().matches("[$a-zA-Z0-9._-]*")) {
                throw new IllegalArgumentException("Invalid character found for config key: " + propsKey);
            }
        });
    }

    private static void processCommand(ConfigCommandOptions opts) throws Exception {
        Properties props = opts.options.has(opts.commandConfigOpt)
            ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
            : new Properties();
        CommandLineUtils.initializeBootstrapProperties(opts.parser,
                opts.options,
                props,
                opts.bootstrapServerOpt,
                opts.bootstrapControllerOpt);

        if (opts.options.has(opts.alterOpt) && opts.entityTypes().size() != opts.entityNames().size()) {
            throw new IllegalArgumentException("An entity name must be specified for every entity type");
        }

        try (Admin adminClient = Admin.create(props)) {
            if (opts.options.has(opts.alterOpt)) {
                alterConfig(adminClient, opts);
            } else if (opts.options.has(opts.describeOpt)) {
                describeConfig(adminClient, opts);
            }
        }
    }

    static void alterConfig(Admin adminClient, ConfigCommandOptions opts) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        List<String> entityTypes = opts.entityTypes();
        List<String> entityNames = opts.entityNames();
        String entityType = entityTypes.get(0);
        String entityName = entityNames.get(0);
        Properties configsToBeAddedProps = parseConfigsToBeAdded(opts);
        Map<String, String> configsToBeAddedMap = configsToBeAddedProps.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        e -> e.getValue().toString()
                ));
        Map<String, ConfigEntry> configsToBeAdded = configsToBeAddedMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new ConfigEntry(e.getKey(), e.getValue())
                ));
        List<String> configsToBeDeleted = parseConfigsToBeDeleted(opts);

        if (TOPIC_TYPE.equals(entityType) || CLIENT_METRICS_TYPE.equals(entityType) ||
                BROKER_TYPE.equals(entityType) || GROUP_TYPE.equals(entityType)) {
            ConfigResource.Type configResourceType;
            if (TOPIC_TYPE.equals(entityType)) {
                configResourceType = ConfigResource.Type.TOPIC;
            } else if (CLIENT_METRICS_TYPE.equals(entityType)) {
                configResourceType = ConfigResource.Type.CLIENT_METRICS;
            } else if (BROKER_TYPE.equals(entityType)) {
                if (!BROKER_DEFAULT_ENTITY_NAME.equals(entityName)) {
                    validateBrokerId(entityName, entityType);
                }
                configResourceType = ConfigResource.Type.BROKER;
            } else {
                configResourceType = ConfigResource.Type.GROUP;
            }
            try {
                alterResourceConfig(adminClient, entityName, configsToBeDeleted, configsToBeAdded, configResourceType);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof UnsupportedVersionException) {
                    throw new UnsupportedVersionException("The " + ApiKeys.INCREMENTAL_ALTER_CONFIGS + " API is not supported by the cluster. The API is supported starting from version 2.3.0."
                            + " You may want to use an older version of this tool to interact with your cluster, or upgrade your brokers to version 2.3.0 or newer to avoid this error.");
                }
                throw ee;
            }
        } else if (BROKER_LOGGER_CONFIG_TYPE.equals(entityType)) {
            List<String> validLoggers = getResourceConfig(adminClient, entityType, entityName, false, false).stream().map(ConfigEntry::name).toList();
            // fail the command if any of the configured broker loggers do not exist
            List<String> invalidBrokerLoggers = Stream.concat(
                    configsToBeDeleted.stream().filter(c -> !validLoggers.contains(c)),
                    configsToBeAdded.keySet().stream().filter(c -> !validLoggers.contains(c))
            ).toList();
            if (!invalidBrokerLoggers.isEmpty())
                throw new InvalidConfigurationException("Invalid broker logger(s): " + String.join(",", invalidBrokerLoggers));

            ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, entityName);
            AlterConfigsOptions alterOptions = new AlterConfigsOptions().timeoutMs(30000);
            List<AlterConfigOp> addEntries = configsToBeAdded.values().stream().map(k -> new AlterConfigOp(k, AlterConfigOp.OpType.SET)).toList();
            List<AlterConfigOp> deleteEntries = configsToBeDeleted.stream().map(k -> new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE)).toList();
            Collection<AlterConfigOp> alterEntries = Stream.concat(deleteEntries.stream(), addEntries.stream()).toList();
            adminClient.incrementalAlterConfigs(Map.of(configResource, alterEntries), alterOptions).all().get(60, TimeUnit.SECONDS);
        } else if (USER_TYPE.equals(entityType) || CLIENT_TYPE.equals(entityType)) {
            boolean hasQuotaConfigsToAdd = configsToBeAdded.keySet().stream()
                    .anyMatch(QuotaConfig::isClientOrUserQuotaConfig);
            Map<String, ConfigEntry> scramConfigsToAddMap = configsToBeAdded.entrySet().stream()
                    .filter(entry -> ScramMechanism.isScram(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Set<String> unknownConfigsToAdd = configsToBeAdded.keySet().stream()
                    .filter(key -> !ScramMechanism.isScram(key) && !QuotaConfig.isClientOrUserQuotaConfig(key))
                    .collect(Collectors.toSet());
            boolean hasQuotaConfigsToDelete = configsToBeDeleted.stream()
                    .anyMatch(QuotaConfig::isClientOrUserQuotaConfig);
            List<String> scramConfigsToDelete = configsToBeDeleted.stream()
                    .filter(ScramMechanism::isScram)
                    .toList();
            Set<String> unknownConfigsToDelete = configsToBeDeleted.stream()
                    .filter(key -> !ScramMechanism.isScram(key) && !QuotaConfig.isClientOrUserQuotaConfig(key))
                    .collect(Collectors.toSet());

            if (CLIENT_TYPE.equals(entityType) || entityTypes.size() == 2) { // size==2 for case where users is specified first on the command line, before clients
                // either just a client or both a user and a client
                if (!unknownConfigsToAdd.isEmpty() || !scramConfigsToAddMap.isEmpty()) {
                    Set<String> combined = new HashSet<>(unknownConfigsToAdd);
                    combined.addAll(scramConfigsToAddMap.keySet());
                    throw new IllegalArgumentException("Only quota configs can be added for '" + CLIENT_TYPE + "' using --bootstrap-server. Unexpected config names: " + String.join(",", combined));
                }
                if (!unknownConfigsToDelete.isEmpty() || !scramConfigsToDelete.isEmpty()) {
                    Set<String> combined = new HashSet<>(unknownConfigsToDelete);
                    combined.addAll(scramConfigsToDelete);
                    throw new IllegalArgumentException("Only quota configs can be deleted for '" + CLIENT_TYPE + "' using --bootstrap-server. Unexpected config names: " + String.join(",", combined));
                }
            } else { // ConfigType.User
                if (!unknownConfigsToAdd.isEmpty())
                    throw new IllegalArgumentException("Only quota and SCRAM credential configs can be added for '" + USER_TYPE + "' using --bootstrap-server. Unexpected config names: " + String.join(",", unknownConfigsToAdd));
                if (!unknownConfigsToDelete.isEmpty())
                    throw new IllegalArgumentException("Only quota and SCRAM credential configs can be deleted for '" + USER_TYPE + "' using --bootstrap-server. Unexpected config names: " + String.join(",", unknownConfigsToDelete));
                if (!scramConfigsToAddMap.isEmpty() || !scramConfigsToDelete.isEmpty()) {
                    if (entityNames.stream().anyMatch(String::isEmpty)) // either --entity-type users --entity-default or --user-defaults
                        throw new IllegalArgumentException("The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.");
                    if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete)
                        throw new IllegalArgumentException("Cannot alter both quota and SCRAM credential configs simultaneously for '" + USER_TYPE + "' using --bootstrap-server.");
                }
            }

            if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete) {
                alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted);
            } else {
                // handle altering user SCRAM credential configs
                if (entityNames.size() != 1) {
                    // should never happen, if we get here then it is a bug
                    throw new IllegalStateException("Altering user SCRAM credentials should never occur for more zero or multiple users: " + entityNames);
                }
                alterUserScramCredentialConfigs(adminClient, entityNames.get(0), scramConfigsToAddMap, scramConfigsToDelete);
            }
        } else if (IP_TYPE.equals(entityType)) {
            Set<String> allConfigNames = new HashSet<>(configsToBeAdded.keySet());
            allConfigNames.addAll(configsToBeDeleted);
            Set<String> unknownConfigs = allConfigNames.stream()
                    .filter(key -> !QuotaConfig.ipConfigs().names().contains(key))
                    .collect(Collectors.toSet());
            if (!unknownConfigs.isEmpty()) {
                throw new IllegalArgumentException("Only connection quota configs can be added for '" + IP_TYPE + "' using --bootstrap-server. Unexpected config names: " + String.join(", ", unknownConfigs));
            }
            alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted);
        } else {
            throw new IllegalArgumentException("Unsupported entity type: " + entityType);
        }

        if (!entityName.isEmpty()) {
            String entityTypeSingular = entityType.substring(0, entityType.length() - 1);
            System.out.println("Completed updating config for " + entityTypeSingular + " " + entityName + ".");
        } else {
            System.out.println("Completed updating default config for " + entityType + " in the cluster.");
        }
    }

    private record IterationsAndPassword(int iterations, byte[] passwordBytes) {
    }

    private static IterationsAndPassword parseIterationsAndPasswordBytes(org.apache.kafka.common.security.scram.internals.ScramMechanism mechanism, String credentialStr) {
        Pattern pattern = Pattern.compile("(?:iterations=(\\-?[0-9]*),)?password=(.*)");
        Matcher matcher = pattern.matcher(credentialStr);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid credential property " + mechanism + "=" + credentialStr);
        }

        String iterationsStr = matcher.group(1);
        String password = matcher.group(2);

        int iterations = (iterationsStr != null && !"-1".equals(iterationsStr))
            ? Integer.parseInt(iterationsStr)
            : DEFAULT_SCRAM_ITERATIONS;

        if (iterations < mechanism.minIterations()) {
            throw new IllegalArgumentException("Iterations " + iterations + " is less than the minimum " + mechanism.minIterations() + " required for " + mechanism.mechanismName());
        }

        return new IterationsAndPassword(iterations, password.getBytes(StandardCharsets.UTF_8));
    }

    private static void alterUserScramCredentialConfigs(Admin adminClient, String user, Map<String, ConfigEntry> scramConfigsToAddMap, List<String> scramConfigsToDelete) throws ExecutionException, InterruptedException, TimeoutException {
        List<UserScramCredentialDeletion> deletions = scramConfigsToDelete.stream()
                .map(mechanismName -> new UserScramCredentialDeletion(user, org.apache.kafka.clients.admin.ScramMechanism.fromMechanismName(mechanismName)))
                .toList();

        List<UserScramCredentialUpsertion> upsertions = scramConfigsToAddMap.entrySet().stream()
                .map(entry -> {
                    String mechanismName = entry.getKey();
                    ConfigEntry configEntry = entry.getValue();
                    org.apache.kafka.common.security.scram.internals.ScramMechanism mechanism =
                            org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(mechanismName);
                    IterationsAndPassword result = parseIterationsAndPasswordBytes(mechanism, configEntry.value());
                    return new UserScramCredentialUpsertion(
                            user,
                            new ScramCredentialInfo(org.apache.kafka.clients.admin.ScramMechanism.fromMechanismName(mechanismName), result.iterations),
                            result.passwordBytes
                    );
                })
                .toList();

        // we are altering only a single user by definition, so we don't have to worry about one user succeeding and another
        // failing; therefore just check the success of all the futures (since there will only be 1)
        List<UserScramCredentialAlteration> allCredentials = new ArrayList<>();
        allCredentials.addAll(deletions);
        allCredentials.addAll(upsertions);
        adminClient.alterUserScramCredentials(allCredentials).all().get(60, TimeUnit.SECONDS);
    }

    private static void alterQuotaConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames, Map<String, String> configsToBeAddedMap, List<String> configsToBeDeleted) throws ExecutionException, InterruptedException, TimeoutException {
        // handle altering client/user quota configs
        Map<String, Double> oldConfig = getClientQuotasConfig(adminClient, entityTypes, entityNames);

        List<String> invalidConfigs = configsToBeDeleted.stream()
                .filter(config -> !oldConfig.containsKey(config))
                .toList();
        if (!invalidConfigs.isEmpty())
            throw new InvalidConfigurationException("Invalid config(s): " + String.join(",", invalidConfigs));

        List<String> alterEntityTypes = entityTypes.stream()
                .map(type -> {
                    if (USER_TYPE.equals(type)) {
                        return ClientQuotaEntity.USER;
                    } else if (CLIENT_TYPE.equals(type)) {
                        return ClientQuotaEntity.CLIENT_ID;
                    } else if (IP_TYPE.equals(type)) {
                        return ClientQuotaEntity.IP;
                    } else {
                        throw new IllegalArgumentException("Unexpected entity type: " + type);
                    }
                })
                .toList();

        List<String> alterEntityNames = entityNames.stream()
                .map(en -> en.isEmpty() ? null : en)
                .toList();

        // Explicitly populate a HashMap to ensure nulls are recorded properly.
        Map<String, String> alterEntityMap = new HashMap<>();
        for (int i = 0; i < alterEntityTypes.size(); i++) {
            alterEntityMap.put(alterEntityTypes.get(i), alterEntityNames.get(i));
        }
        ClientQuotaEntity entity = new ClientQuotaEntity(alterEntityMap);

        AlterClientQuotasOptions alterOptions = new AlterClientQuotasOptions();

        List<ClientQuotaAlteration.Op> addOps = configsToBeAddedMap.entrySet().stream()
                .map(entry -> {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    double doubleValue;
                    try {
                        doubleValue = Double.parseDouble(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Cannot parse quota configuration value for " + key + ": " + value);
                    }
                    return new ClientQuotaAlteration.Op(key, doubleValue);
                })
                .toList();

        List<ClientQuotaAlteration.Op> deleteOps = configsToBeDeleted.stream()
                .map(key -> new ClientQuotaAlteration.Op(key, null))
                .toList();

        Collection<ClientQuotaAlteration.Op> alterOps = Stream.concat(addOps.stream(), deleteOps.stream()).toList();

        adminClient.alterClientQuotas(Collections.singleton(new ClientQuotaAlteration(entity, alterOps)), alterOptions)
                .all().get(60, TimeUnit.SECONDS);
    }

    static void describeConfig(Admin adminClient, ConfigCommandOptions opts) throws Exception {
        List<String> entityTypes = opts.entityTypes();
        List<String> entityNames = opts.entityNames();
        boolean describeAll = opts.options.has(opts.allOpt);

        String entityType = entityTypes.get(0);
        if (TOPIC_TYPE.equals(entityType) || BROKER_TYPE.equals(entityType) || BROKER_LOGGER_CONFIG_TYPE.equals(entityType) ||
                CLIENT_METRICS_TYPE.equals(entityType) || GROUP_TYPE.equals(entityType)) {
            describeResourceConfig(adminClient, entityType, entityNames.isEmpty() ? Optional.empty() : Optional.of(entityNames.get(0)), describeAll);
        } else if (USER_TYPE.equals(entityType) || CLIENT_TYPE.equals(entityType)) {
            describeClientQuotaAndUserScramCredentialConfigs(adminClient, entityTypes, entityNames);
        } else if (IP_TYPE.equals(entityType)) {
            describeQuotaConfigs(adminClient, entityTypes, entityNames);
        } else {
            throw new IllegalArgumentException("Invalid entity type: " + entityType);
        }
    }

    private static void describeResourceConfig(Admin adminClient, String entityType, Optional<String> entityName, boolean describeAll) throws Exception {
        if (!describeAll) {
            if (entityName.isPresent()) {
                String name = entityName.get();
                String entityTypeSingular = entityType.substring(0, entityType.length() - 1);
                if (TOPIC_TYPE.equals(entityType)) {
                    Topic.validate(name);
                    if (!adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get().contains(name)) {
                        System.out.println("The " + entityTypeSingular + " '" + name + "' doesn't exist and doesn't have dynamic config.");
                        return;
                    }
                } else if (BROKER_TYPE.equals(entityType) || BROKER_LOGGER_CONFIG_TYPE.equals(entityType)) {
                    if (adminClient.describeCluster().nodes().get().stream().anyMatch(n -> n.idString().equals(name))) {
                        // valid broker id
                    } else if (BROKER_DEFAULT_ENTITY_NAME.equals(name)) {
                        // default broker configs
                    } else {
                        System.out.println("The " + entityTypeSingular + " '" + name + "' doesn't exist and doesn't have dynamic config.");
                        return;
                    }
                } else if (CLIENT_METRICS_TYPE.equals(entityType)) {
                    if (adminClient.listConfigResources(Set.of(ConfigResource.Type.CLIENT_METRICS), new ListConfigResourcesOptions()).all().get()
                            .stream().noneMatch(resource -> resource.name().equals(name))) {
                        System.out.println("The " + entityTypeSingular + " '" + name + "' doesn't exist and doesn't have dynamic config.");
                        return;
                    }
                } else if (GROUP_TYPE.equals(entityType)) {
                    boolean noMatchInGroups = adminClient.listGroups().all().get().stream()
                            .noneMatch(group -> group.groupId().equals(name));
                    boolean noMatchInResources = listGroupConfigResources(adminClient)
                            .map(resources -> resources.stream().noneMatch(resource -> resource.name().equals(name)))
                            .orElse(false);
                    if (noMatchInGroups && noMatchInResources) {
                        System.out.println("The " + entityTypeSingular + " '" + name + "' doesn't exist and doesn't have dynamic config.");
                        return;
                    }
                } else {
                    throw new IllegalArgumentException("Invalid entity type: " + entityType);
                }
            }
        }

        Set<String> entities;
        if (entityName.isPresent()) {
            entities = Set.of(entityName.get());
        } else {
            if (TOPIC_TYPE.equals(entityType)) {
                entities = new LinkedHashSet<>(adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get());
            } else if (BROKER_TYPE.equals(entityType) || BROKER_LOGGER_CONFIG_TYPE.equals(entityType)) {
                Set<String> brokerIds = adminClient.describeCluster(new DescribeClusterOptions()).nodes().get().stream()
                        .map(Node::idString)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                brokerIds.add(BROKER_DEFAULT_ENTITY_NAME);
                entities = brokerIds;
            } else if (CLIENT_METRICS_TYPE.equals(entityType)) {
                entities = adminClient.listConfigResources(Set.of(ConfigResource.Type.CLIENT_METRICS), new ListConfigResourcesOptions()).all().get().stream()
                        .map(ConfigResource::name)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
            } else if (GROUP_TYPE.equals(entityType)) {
                Set<String> groupIds = adminClient.listGroups().all().get().stream()
                        .map(GroupListing::groupId)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                Set<String> groupResources = listGroupConfigResources(adminClient)
                        .map(resources -> resources.stream()
                                .map(ConfigResource::name)
                                .collect(Collectors.toCollection(LinkedHashSet::new)))
                        .orElseGet(LinkedHashSet::new);
                Set<String> combined = new LinkedHashSet<>(groupIds);
                combined.addAll(groupResources);
                entities = combined;
            } else {
                throw new IllegalArgumentException("Invalid entity type: " + entityType);
            }
        }

        if (entities.isEmpty()) {
            return;
        }

        Map<String, DescribeConfigContext> contextsByEntity = new LinkedHashMap<>();
        for (String entity : entities) {
            contextsByEntity.put(entity, describeConfigContext(entityType, entity));
        }

        DescribeConfigsOptions describeOptions = new DescribeConfigsOptions().includeSynonyms(true);
        Map<ConfigResource, KafkaFuture<Config>> configs = adminClient.describeConfigs(
                contextsByEntity.values().stream()
                        .map(DescribeConfigContext::configResource)
                        .toList(),
                describeOptions
        ).values();

        for (String entity : entities) {
            DescribeConfigContext context = contextsByEntity.get(entity);
            if (BROKER_DEFAULT_ENTITY_NAME.equals(entity)) {
                System.out.println("Default configs for " + entityType + " in the cluster are:");
            } else {
                String configSourceStr = describeAll ? "All" : "Dynamic";
                String entityTypeSingular = entityType.substring(0, entityType.length() - 1);
                System.out.println(configSourceStr + " configs for " + entityTypeSingular + " " + entity + " are:");
            }

            Optional<ConfigEntry.ConfigSource> configSourceFilter = describeAll
                    ? Optional.empty()
                    : Optional.of(context.dynamicConfigSource());
            Config config = configs.get(context.configResource()).get(30, TimeUnit.SECONDS);
            filterAndSortEntries(config, configSourceFilter).forEach(entry -> {
                String synonyms = entry.synonyms().stream()
                        .map(synonym -> synonym.source() + ":" + synonym.name() + "=" + synonym.value())
                        .collect(Collectors.joining(", ", "{", "}"));
                System.out.println("  " + entry.name() + "=" + entry.value() + " sensitive=" + entry.isSensitive() + " synonyms=" + synonyms);
            });
        }
    }

    private static void alterResourceConfig(Admin adminClient, String entityNameHead, List<String> configsToBeDeleted, Map<String, ConfigEntry> configsToBeAdded, ConfigResource.Type resourceType) throws ExecutionException, InterruptedException, TimeoutException {
        ConfigResource configResource = new ConfigResource(resourceType, entityNameHead);
        AlterConfigsOptions alterOptions = new AlterConfigsOptions().timeoutMs(30000);
        List<AlterConfigOp> addEntries = configsToBeAdded.values().stream().map(k -> new AlterConfigOp(k, AlterConfigOp.OpType.SET)).toList();
        List<AlterConfigOp> deleteEntries = configsToBeDeleted.stream().map(k -> new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE)).toList();
        Collection<AlterConfigOp> alterEntries = Stream.concat(deleteEntries.stream(), addEntries.stream()).toList();
        adminClient.incrementalAlterConfigs(Map.of(configResource, alterEntries), alterOptions).all().get(60, TimeUnit.SECONDS);
    }

    static void validateBrokerId(String entityName, String entityType) {
        try {
            Integer.parseInt(entityName);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("The entity name for " + entityType + " must be a valid integer broker id, found: " + entityName);
        }
    }

    private record DescribeConfigContext(ConfigResource configResource, ConfigEntry.ConfigSource dynamicConfigSource) {
    }

    private static DescribeConfigContext describeConfigContext(String entityType, String entityName) {
        ConfigResource.Type configResourceType;
        ConfigEntry.ConfigSource dynamicConfigSource;

        if (TOPIC_TYPE.equals(entityType)) {
            if (!entityName.isEmpty()) {
                Topic.validate(entityName);
            }
            configResourceType = ConfigResource.Type.TOPIC;
            dynamicConfigSource = ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
        } else if (BROKER_TYPE.equals(entityType)) {
            configResourceType = ConfigResource.Type.BROKER;
            if (BROKER_DEFAULT_ENTITY_NAME.equals(entityName)) {
                dynamicConfigSource = ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG;
            } else {
                validateBrokerId(entityName, entityType);
                dynamicConfigSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
            }
        } else if (BROKER_LOGGER_CONFIG_TYPE.equals(entityType)) {
            if (!entityName.isEmpty()) {
                validateBrokerId(entityName, entityType);
            }
            configResourceType = ConfigResource.Type.BROKER_LOGGER;
            dynamicConfigSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG;
        } else if (CLIENT_METRICS_TYPE.equals(entityType)) {
            configResourceType = ConfigResource.Type.CLIENT_METRICS;
            dynamicConfigSource = ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG;
        } else if (GROUP_TYPE.equals(entityType)) {
            configResourceType = ConfigResource.Type.GROUP;
            dynamicConfigSource = ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG;
        } else {
            throw new IllegalArgumentException("Invalid entity type: " + entityType);
        }

        return new DescribeConfigContext(new ConfigResource(configResourceType, entityName), dynamicConfigSource);
    }

    private static List<ConfigEntry> filterAndSortEntries(Config config, Optional<ConfigEntry.ConfigSource> configSourceFilter) {
        return config.entries().stream()
                .filter(entry -> configSourceFilter.isEmpty() || entry.source() == configSourceFilter.get())
                .sorted(Comparator.comparing(ConfigEntry::name))
                .toList();
    }

    private static List<ConfigEntry> getResourceConfig(Admin adminClient, String entityType, String entityName, boolean includeSynonyms, boolean describeAll) throws ExecutionException, InterruptedException, TimeoutException {
        DescribeConfigContext context = describeConfigContext(entityType, entityName);
        Optional<ConfigEntry.ConfigSource> configSourceFilter = describeAll
                ? Optional.empty()
                : Optional.of(context.dynamicConfigSource());
        DescribeConfigsOptions describeOptions = new DescribeConfigsOptions().includeSynonyms(includeSynonyms);
        Map<ConfigResource, Config> configs = adminClient.describeConfigs(Collections.singleton(context.configResource()), describeOptions)
                    .all().get(30, TimeUnit.SECONDS);

        return filterAndSortEntries(configs.get(context.configResource()), configSourceFilter);
    }

    private static void describeQuotaConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws ExecutionException, InterruptedException, TimeoutException {
        Map<ClientQuotaEntity, Map<String, Double>> quotaConfigs = getAllClientQuotasConfigs(adminClient, entityTypes, entityNames);
        quotaConfigs.forEach((entity, entries) -> {
            Map<String, String> entityEntries = entity.entries();

            Function<String, Optional<String>> entitySubstr = entityType -> {
                String name = entityEntries.get(entityType);
                if (name == null && !entityEntries.containsKey(entityType)) {
                    return Optional.empty();
                }
                String typeStr = switch (entityType) {
                    case ClientQuotaEntity.USER -> "user-principal";
                    case ClientQuotaEntity.CLIENT_ID -> "client-id";
                    case ClientQuotaEntity.IP -> "ip";
                    default -> throw new IllegalArgumentException("Unknown entity type: " + entityType);
                };
                String result = (name == null || name.isEmpty()) ? "the default " + typeStr : typeStr + " '" + name + "'";
                return Optional.of(result);
            };

            String entityStr = Stream.of(
                    entitySubstr.apply(ClientQuotaEntity.USER),
                    entitySubstr.apply(ClientQuotaEntity.CLIENT_ID),
                    entitySubstr.apply(ClientQuotaEntity.IP)
            )
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.joining(", "));

            String entriesStr = entries.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(", "));

            System.out.println("Quota configs for " + entityStr + " are " + entriesStr);
        });
    }

    private static void describeClientQuotaAndUserScramCredentialConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws ExecutionException, InterruptedException, TimeoutException {
        describeQuotaConfigs(adminClient, entityTypes, entityNames);
        // we describe user SCRAM credentials only when we are not describing client information
        // and we are not given either --entity-default or --user-defaults
        if (!entityTypes.contains(CLIENT_TYPE) && !entityNames.contains("")) {
            DescribeUserScramCredentialsResult result = adminClient.describeUserScramCredentials(entityNames);
            result.users().get(30, TimeUnit.SECONDS).forEach(user -> {
                try {
                    UserScramCredentialsDescription description = result.description(user).get(30, TimeUnit.SECONDS);
                    String descriptionText = description.credentialInfos().stream()
                            .map(info -> info.mechanism().mechanismName() + "=iterations=" + info.iterations())
                            .collect(Collectors.joining(", "));
                    System.out.println("SCRAM credential configs for user-principal '" + user + "' are " + descriptionText);
                } catch (Exception e) {
                    System.out.println("Error retrieving SCRAM credential configs for user-principal '" + user + "': " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            });
        }
    }

    private static Map<String, Double> getClientQuotasConfig(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws ExecutionException, InterruptedException, TimeoutException {
        if (entityTypes.size() != entityNames.size())
            throw new IllegalArgumentException("Exactly one entity name must be specified for every entity type");
        return getAllClientQuotasConfigs(adminClient, entityTypes, entityNames)
                .values()
                .stream()
                .findFirst()
                .orElse(Map.of());
    }

    private static Map<ClientQuotaEntity, Map<String, Double>> getAllClientQuotasConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws ExecutionException, InterruptedException, TimeoutException {
        int maxSize = Math.max(entityTypes.size(), entityNames.size());
        List<ClientQuotaFilterComponent> components = new ArrayList<>();

        for (int i = 0; i < maxSize; i++) {
            Optional<String> entityTypeOpt = i < entityTypes.size() ? Optional.of(entityTypes.get(i)) : Optional.empty();
            Optional<String> entityNameOpt = i < entityNames.size() ? Optional.of(entityNames.get(i)) : Optional.empty();

            String entityType;
            if (entityTypeOpt.isPresent()) {
                String typeValue = entityTypeOpt.get();
                if (USER_TYPE.equals(typeValue)) {
                    entityType = ClientQuotaEntity.USER;
                } else if (CLIENT_TYPE.equals(typeValue)) {
                    entityType = ClientQuotaEntity.CLIENT_ID;
                } else if (IP_TYPE.equals(typeValue)) {
                    entityType = ClientQuotaEntity.IP;
                } else {
                    throw new IllegalArgumentException("Unexpected entity type " + typeValue);
                }
            } else {
                throw new IllegalArgumentException("More entity names specified than entity types");
            }

            ClientQuotaFilterComponent component;
            if (entityNameOpt.isEmpty()) {
                component = ClientQuotaFilterComponent.ofEntityType(entityType);
            } else if (entityNameOpt.get().isEmpty()) {
                component = ClientQuotaFilterComponent.ofDefaultEntity(entityType);
            } else {
                component = ClientQuotaFilterComponent.ofEntity(entityType, entityNameOpt.get());
            }
            components.add(component);
        }

        return adminClient.describeClientQuotas(ClientQuotaFilter.containsOnly(components)).entities().get(30, TimeUnit.SECONDS);
    }

    private static Optional<Collection<ConfigResource>> listGroupConfigResources(Admin adminClient) throws Exception {
        try {
            return Optional.of(adminClient.listConfigResources(Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions()).all().get());
        } catch (ExecutionException ee) {
            // (KIP-1142) 4.1+ admin client vs older broker: treat UnsupportedVersionException and ClusterAuthorizationException as None
            if (ee.getCause() instanceof UnsupportedVersionException) return Optional.empty();
            if (ee.getCause() instanceof ClusterAuthorizationException) return Optional.empty();
            else throw (Exception) ee.getCause();
        }
    }

    static class ConfigCommandOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> bootstrapControllerOpt;
        private final OptionSpec<String> commandConfigOpt;
        private final OptionSpec<Void> alterOpt;
        private final OptionSpec<Void> describeOpt;
        private final OptionSpec<Void> allOpt;
        private final OptionSpec<String> entityType;
        private final OptionSpec<String> entityName;
        private final OptionSpec<Void> entityDefault;
        private final OptionSpec<String> addConfig;
        private final OptionSpec<String> addConfigFile;
        private final OptionSpec<String> deleteConfig;
        private final OptionSpec<String> topic;
        private final OptionSpec<String> client;
        private final OptionSpec<Void> clientDefaults;
        private final OptionSpec<String> user;
        private final OptionSpec<Void> userDefaults;
        private final OptionSpec<String> broker;
        private final OptionSpec<Void> brokerDefaults;
        private final OptionSpec<String> brokerLogger;
        private final OptionSpec<Void> ipDefaults;
        private final OptionSpec<String> ip;
        private final OptionSpec<String> group;
        private final OptionSpec<String> clientMetrics;

        private static String formatConfigNames(Collection<String> names) {
            String nl = System.lineSeparator();
            return names.stream()
                    .sorted()
                    .map(name -> "\t" + name)
                    .collect(Collectors.joining(nl, nl, nl));
        }

        ConfigCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "The Kafka servers to connect to.")
                    .withRequiredArg()
                    .describedAs("server to connect to")
                    .ofType(String.class);
            bootstrapControllerOpt = parser.accepts("bootstrap-controller", "The Kafka controllers to connect to.")
                    .withRequiredArg()
                    .describedAs("controller to connect to")
                    .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
                            "This is used only with --bootstrap-server option for describing and altering broker configs.")
                    .withRequiredArg()
                    .describedAs("command config property file")
                    .ofType(String.class);
            alterOpt = parser.accepts("alter", "Alter the configuration for the entity.");
            describeOpt = parser.accepts("describe", "List configs for the given entity.");
            allOpt = parser.accepts("all", "List all configs for the given entity, including static configs if available.");

            entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers/ips/client-metrics/groups)")
                    .withRequiredArg()
                    .ofType(String.class);
            entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id/ip/client metrics/group id)")
                    .withRequiredArg()
                    .ofType(String.class);
            entityDefault = parser.accepts("entity-default", "Default entity name for clients/users/brokers/ips (applies to corresponding entity type)");

            addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
                            "For entity-type '" + TOPIC_TYPE + "': " + formatConfigNames(LogConfig.nonInternalConfigNames()) +
                            "For entity-type '" + BROKER_TYPE + "': " + formatConfigNames(DynamicConfig.Broker.names()) +
                            "For entity-type '" + USER_TYPE + "': " + formatConfigNames(QuotaConfig.scramMechanismsPlusUserAndClientQuotaConfigs().names()) +
                            "For entity-type '" + CLIENT_TYPE + "': " + formatConfigNames(QuotaConfig.userAndClientQuotaConfigs().names()) +
                            "For entity-type '" + IP_TYPE + "': " + formatConfigNames(QuotaConfig.ipConfigs().names()) +
                            "For entity-type '" + CLIENT_METRICS_TYPE + "': " + formatConfigNames(ClientMetricsConfigs.configNames()) +
                            "For entity-type '" + GROUP_TYPE + "': " + formatConfigNames(GroupConfig.configNames()) +
                            "Entity types '" + USER_TYPE + "' and '" + CLIENT_TYPE + "' may be specified together to update config for clients of a specific user.")
                    .withRequiredArg()
                    .ofType(String.class);
            addConfigFile = parser.accepts("add-config-file", "Path to a properties file with configs to add. See add-config for a list of valid configurations.")
                    .withRequiredArg()
                    .ofType(String.class);
            deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
                    .withRequiredArg()
                    .ofType(String.class)
                    .withValuesSeparatedBy(',');
            topic = parser.accepts("topic", "The topic's name.")
                    .withRequiredArg()
                    .ofType(String.class);
            client = parser.accepts("client", "The client's ID.")
                    .withRequiredArg()
                    .ofType(String.class);
            clientDefaults = parser.accepts("client-defaults", "The config defaults for all clients.");
            user = parser.accepts("user", "The user's principal name.")
                    .withRequiredArg()
                    .ofType(String.class);
            userDefaults = parser.accepts("user-defaults", "The config defaults for all users.");
            broker = parser.accepts("broker", "The broker's ID.")
                    .withRequiredArg()
                    .ofType(String.class);
            brokerDefaults = parser.accepts("broker-defaults", "The config defaults for all brokers.");
            brokerLogger = parser.accepts("broker-logger", "The broker's ID for its logger config.")
                    .withRequiredArg()
                    .ofType(String.class);
            ipDefaults = parser.accepts("ip-defaults", "The config defaults for all IPs.");
            ip = parser.accepts("ip", "The IP address.")
                    .withRequiredArg()
                    .ofType(String.class);
            group = parser.accepts("group", "The group's ID.")
                    .withRequiredArg()
                    .ofType(String.class);
            clientMetrics = parser.accepts("client-metrics", "The client metrics config resource name.")
                    .withRequiredArg()
                    .ofType(String.class);

            options = parser.parse(args);
        }

        private record EntityFlag(OptionSpec<?> spec, String type) { }

        private List<EntityFlag> entityFlags() {
            return List.of(
                new EntityFlag(topic, TOPIC_TYPE),
                new EntityFlag(client, CLIENT_TYPE),
                new EntityFlag(user, USER_TYPE),
                new EntityFlag(broker, BROKER_TYPE),
                new EntityFlag(brokerLogger, BROKER_LOGGER_CONFIG_TYPE),
                new EntityFlag(ip, IP_TYPE),
                new EntityFlag(clientMetrics, CLIENT_METRICS_TYPE),
                new EntityFlag(group, GROUP_TYPE)
            );
        }

        private List<EntityFlag> entityDefaultsFlags() {
            return List.of(
                new EntityFlag(clientDefaults, CLIENT_TYPE),
                new EntityFlag(userDefaults, USER_TYPE),
                new EntityFlag(brokerDefaults, BROKER_TYPE),
                new EntityFlag(ipDefaults, IP_TYPE)
            );
        }

        List<String> entityTypes() {
            List<String> fromEntityType = new ArrayList<>(options.valuesOf(entityType));
            List<String> fromFlags = Stream.concat(entityFlags().stream(), entityDefaultsFlags().stream())
                    .filter(entity -> options.has(entity.spec()))
                    .map(EntityFlag::type)
                    .toList();
            List<String> result = new ArrayList<>(fromEntityType);
            result.addAll(fromFlags);
            return result;
        }

        @SuppressWarnings("unchecked")
        List<String> entityNames() {
            Iterator<String> namesIterator = options.valuesOf(entityName).iterator();
            List<String> fromSpecs = options.specs().stream()
                    .filter(spec -> spec.options().contains("entity-name") || spec.options().contains("entity-default"))
                    .map(spec -> spec.options().contains("entity-name") ? namesIterator.next() : "")
                    .toList();

            List<String> fromEntityFlags = entityFlags().stream()
                    .filter(entity -> options.has(entity.spec()))
                    .map(entity -> options.valueOf((OptionSpec<String>) entity.spec()))
                    .toList();

            List<String> fromDefaultFlags = entityDefaultsFlags().stream()
                    .filter(entity -> options.has(entity.spec()))
                    .map(entity -> "")
                    .toList();

            return Stream.of(fromSpecs, fromEntityFlags, fromDefaultFlags)
                    .flatMap(List::stream)
                    .toList();
        }

        public void checkArgs() {
            // should have exactly one action
            long actions = Stream.of(alterOpt, describeOpt).filter(options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --describe, --alter");
            // check required args
            CommandLineUtils.checkInvalidArgs(parser, options, alterOpt, describeOpt);
            CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, alterOpt, addConfig, deleteConfig);

            List<String> entityTypeVals = entityTypes();
            long distinctCount = entityTypeVals.stream().distinct().count();
            if (entityTypeVals.size() != distinctCount) {
                Set<String> seen = new HashSet<>();
                List<String> duplicates = entityTypeVals.stream()
                        .filter(type -> !seen.add(type))
                        .distinct()
                        .toList();
                throw new IllegalArgumentException("Duplicate entity type(s) specified: " + String.join(",", duplicates));
            }

            List<String> allowedEntityTypes;
            if (options.has(bootstrapServerOpt) || options.has(bootstrapControllerOpt)) {
                allowedEntityTypes = BROKER_SUPPORTED_CONFIG_TYPES;
            } else {
                throw new IllegalArgumentException("Either --bootstrap-server or --bootstrap-controller must be specified.");
            }

            String connectOptString = "--bootstrap-server or --bootstrap-controller";
            entityTypeVals.forEach(entityTypeVal -> {
                if (!allowedEntityTypes.contains(entityTypeVal))
                    throw new IllegalArgumentException("Invalid entity type " + entityTypeVal + ", the entity type must be one of " + String.join(", ", allowedEntityTypes) + " with a " + connectOptString + " argument");
            });
            if (entityTypeVals.isEmpty())
                throw new IllegalArgumentException("At least one entity type must be specified");
            else if (entityTypeVals.size() > 1 && !(Set.copyOf(entityTypeVals).equals(Set.of(USER_TYPE, CLIENT_TYPE))))
                throw new IllegalArgumentException("Only '" + USER_TYPE + "' and '" + CLIENT_TYPE + "' entity types may be specified together");

            if ((options.has(entityName) || options.has(entityType) || options.has(entityDefault)) &&
                    Stream.concat(entityFlags().stream(), entityDefaultsFlags().stream())
                            .anyMatch(entity -> options.has(entity.spec())))
                throw new IllegalArgumentException("--entity-{type,name,default} should not be used in conjunction with specific entity flags");

            List<String> entityNamesVals = entityNames();
            boolean hasEntityName = entityNamesVals.stream().anyMatch(name -> !name.isEmpty());
            boolean hasEntityDefault = entityNamesVals.stream().anyMatch(String::isEmpty);

            int numConnectOptions = (options.has(bootstrapServerOpt) ? 1 : 0) + (options.has(bootstrapControllerOpt) ? 1 : 0);
            if (numConnectOptions > 1)
                throw new IllegalArgumentException("Only one of --bootstrap-server or --bootstrap-controller can be specified");
            if (hasEntityName && (entityTypeVals.contains(BROKER_TYPE) || entityTypeVals.contains(BROKER_LOGGER_CONFIG_TYPE))) {
                Stream.of(entityName, broker, brokerLogger)
                        .filter(options::has)
                        .map(options::valueOf)
                        .forEach(brokerId -> {
                            try {
                                Integer.parseInt(brokerId);
                            } catch (NumberFormatException nfe) {
                                throw new IllegalArgumentException("The entity name for " + entityTypeVals.get(0) + " must be a valid integer broker id, but it is: " + brokerId);
                            }
                        });
            }

            if (hasEntityName && entityTypeVals.contains(IP_TYPE)) {
                Stream.of(entityName, ip)
                        .filter(options::has)
                        .map(options::valueOf)
                        .forEach(ipEntity -> validateIpEntity(ipEntity, entityTypeVals.get(0)));
            }

            if (options.has(describeOpt)) {
                if (!(entityTypeVals.contains(USER_TYPE) ||
                        entityTypeVals.contains(CLIENT_TYPE) ||
                        entityTypeVals.contains(BROKER_TYPE) ||
                        entityTypeVals.contains(IP_TYPE)) && options.has(entityDefault)) {
                    throw new IllegalArgumentException("--entity-default must not be specified with --describe of " + String.join(",", entityTypeVals));
                }

                if (entityTypeVals.contains(BROKER_LOGGER_CONFIG_TYPE) && !hasEntityName)
                    throw new IllegalArgumentException("An entity name must be specified with --describe of " + String.join(",", entityTypeVals));
            }

            if (options.has(alterOpt)) {
                if (entityTypeVals.contains(USER_TYPE) ||
                        entityTypeVals.contains(CLIENT_TYPE) ||
                        entityTypeVals.contains(BROKER_TYPE) ||
                        entityTypeVals.contains(IP_TYPE)) {
                    if (!hasEntityName && !hasEntityDefault)
                        throw new IllegalArgumentException("An entity-name or default entity must be specified with --alter of users, clients, brokers or ips");
                } else if (!hasEntityName)
                    throw new IllegalArgumentException("An entity name must be specified with --alter of " + String.join(",", entityTypeVals));

                boolean isAddConfigPresent = options.has(addConfig);
                boolean isAddConfigFilePresent = options.has(addConfigFile);
                boolean isDeleteConfigPresent = options.has(deleteConfig);

                if (isAddConfigPresent && isAddConfigFilePresent)
                    throw new IllegalArgumentException("Only one of --add-config or --add-config-file must be specified");

                if (!isAddConfigPresent && !isAddConfigFilePresent && !isDeleteConfigPresent)
                    throw new IllegalArgumentException("At least one of --add-config, --add-config-file, or --delete-config must be specified with --alter");
            }
        }
    }

    private static void validateIpEntity(String ip, String entityType) {
        try {
            InetAddress.getByName(ip);
        } catch (UnknownHostException uhe) {
            throw new IllegalArgumentException("The entity name for " + entityType + " must be a valid IP or resolvable host, but it is: " + ip);
        }
    }

}
