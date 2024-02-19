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
package org.apache.kafka.tools.config;

import joptsimple.OptionException;
import kafka.admin.ZkSecurityMigrator;
import kafka.server.DynamicBrokerConfig;
import kafka.server.KafkaConfig;
import kafka.utils.Exit;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.security.PasswordEncoder;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.server.config.ConfigEntityName;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This script can be used to change configs for topics/clients/users/brokers/ips/client-metrics dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: --topic <topic> OR --entity-type topics --entity-name <topic>
 *     <li> client: --client <client> OR --entity-type clients --entity-name <client-id>
 *     <li> user: --user <user-principal> OR --entity-type users --entity-name <user-principal>
 *     <li> <user, client>: --user <user-principal> --client <client-id> OR
 *                          --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>
 *     <li> broker: --broker <broker-id> OR --entity-type brokers --entity-name <broker-id>
 *     <li> broker-logger: --broker-logger <broker-id> OR --entity-type broker-loggers --entity-name <broker-id>
 *     <li> ip: --ip <ip> OR --entity-type ips --entity-name <ip>
 *     <li> client-metrics: --client-metrics <name> OR --entity-type client-metrics --entity-name <name>
 * </ul>
 * --entity-type <users|clients|brokers|ips> --entity-default may be specified in place of --entity-type <users|clients|brokers|ips> --entity-name <entityName>
 * when describing or altering default configuration for users, clients, brokers, or ips, respectively.
 * Alternatively, --user-defaults, --client-defaults, --broker-defaults, or --ip-defaults may be specified in place of
 * --entity-type <users|clients|brokers|ips> --entity-default, respectively.
 *
 * For most use cases, this script communicates with a kafka cluster (specified via the
 * `--bootstrap-server` option). There are three exceptions where direct communication with a
 * ZooKeeper ensemble (specified via the `--zookeeper` option) is allowed:
 *
 * 1. Describe/alter user configs where the config is a SCRAM mechanism name (i.e. a SCRAM credential for a user)
 * 2. Describe/alter broker configs for a particular broker when that broker is down
 * 3. Describe/alter broker default configs when all brokers are down
 *
 * For example, this allows password configs to be stored encrypted in ZK before brokers are started,
 * avoiding cleartext passwords in `server.properties`.
 */
public class ConfigCommand {
    public static final String BrokerDefaultEntityName = "";
    public static final String BrokerLoggerConfigType = "broker-loggers";

    private static final Logger logger = LoggerFactory.getLogger(ConfigCommand.class);

    static final List<String> BrokerSupportedConfigTypes;
    static final List<String> ZkSupportedConfigTypes = Arrays.asList(ConfigType.USER, ConfigType.BROKER);
    static final int DefaultScramIterations = 4096;

    static {
        List<String> _BrokerSupportedConfigTypes = new ArrayList<>(ConfigType.ALL);
        _BrokerSupportedConfigTypes.add(BrokerLoggerConfigType);
        _BrokerSupportedConfigTypes.add(ConfigType.CLIENT_METRICS);

        BrokerSupportedConfigTypes = Collections.unmodifiableList(_BrokerSupportedConfigTypes);
    }

    public static void main(String[] args) {
        try {
            ConfigCommandOptions opts = new ConfigCommandOptions(args);

            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to manipulate and describe entity config for a topic, client, user, broker, ip or client-metrics");

            opts.checkArgs();

            if (opts.options.has(opts.zkConnectOpt)) {
                System.out.println("Warning: --zookeeper is deprecated and will be removed in a future version of Kafka.");
                System.out.println("Use --bootstrap-server instead to specify a broker to connect to.");
                processCommandWithZk(opts.options.valueOf(opts.zkConnectOpt), opts);
            } else {
                processCommand(opts);
            }
        } catch (IllegalArgumentException | InvalidConfigurationException | OptionException e) {
            logger.debug("Failed config command with args '" + Utils.join(args, " ") + "'", e);
            System.err.println(e.getMessage());
            Exit.exit(1, scala.None$.empty());
        } catch (Throwable t) {
            logger.debug("Error while executing config command with args '" + Utils.join(args, " ") + "'", t);
            System.err.println("Error while executing config command with args '" + Utils.join(args, " ") + "'");
            t.printStackTrace(System.err);
            Exit.exit(1, scala.None$.empty());
        }
    }

    private static void processCommandWithZk(String zkConnectString, ConfigCommandOptions opts) throws IOException {
        ZKClientConfig zkClientConfig = ZkSecurityMigrator.createZkClientConfigFromOption(opts.options, opts.zkTlsConfigFile)
                .getOrElse(ZKClientConfig::new);
        KafkaZkClient zkClient = KafkaZkClient.apply(zkConnectString, JaasUtils.isZkSaslEnabled() || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig), 30000, 30000,
                Integer.MAX_VALUE, Time.SYSTEM, "ConfigCommand", zkClientConfig,
                "kafka.server", "SessionExpireListener", false);
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        try {
            if (opts.options.has(opts.alterOpt))
                alterConfigWithZk(zkClient, opts, adminZkClient);
            else if (opts.options.has(opts.describeOpt))
                describeConfigWithZk(zkClient, opts, adminZkClient);
        } finally {
            zkClient.close();
        }
    }

    private static void alterConfigWithZk(KafkaZkClient zkClient, ConfigCommandOptions opts, AdminZkClient adminZkClient) throws IOException {
        Properties configsToBeAdded = parseConfigsToBeAdded(opts);
        List<String> configsToBeDeleted = parseConfigsToBeDeleted(opts);
        ConfigEntity entity = parseEntity(opts);
        String entityType = entity.root.entityType;
        String entityName = entity.fullSanitizedName();
        String errorMessage = "--bootstrap-server option must be specified to update " + entityType + " configs: {add: " + configsToBeAdded + ", delete: " + configsToBeDeleted + "}";
        boolean isUserClientId = false;

        if (Objects.equals(entityType, ConfigType.USER)) {
            isUserClientId = entity.child.map(e -> ConfigType.CLIENT.equals(e.entityType)).orElse(false);
            if (!configsToBeAdded.isEmpty() || !configsToBeDeleted.isEmpty()) {
                String info = "User configuration updates using ZooKeeper are only supported for SCRAM credential updates.";
                Collection<String> scramMechanismNames = Arrays.stream(ScramMechanism.values()).map(ScramMechanism::mechanismName).collect(Collectors.toList());
                // make sure every added/deleted configs are SCRAM related, other configs are not supported using zookeeper
                require(scramMechanismNames.containsAll(configsToBeAdded.stringPropertyNames()),
                        errorMessage + ". " + info);
                require(scramMechanismNames.containsAll(configsToBeDeleted), errorMessage + ". " + info);
            }
            preProcessScramCredentials(configsToBeAdded);
        } else if (Objects.equals(entityType, ConfigType.BROKER)) {
            // Dynamic broker configs can be updated using ZooKeeper only if the corresponding broker is not running.
            if (!configsToBeAdded.isEmpty() || !configsToBeDeleted.isEmpty()) {
                validateBrokersNotRunning(entityName, adminZkClient, zkClient, errorMessage);

                boolean perBrokerConfig = !Objects.equals(entityName, ConfigEntityName.DEFAULT);
                preProcessBrokerConfigs(configsToBeAdded, perBrokerConfig);
            }
        }

        // compile the final set of configs
        Properties configs = adminZkClient.fetchEntityConfig(entityType, entityName);

        // fail the command if any of the configs to be deleted does not exist
        List<String> invalidConfigs = configsToBeDeleted.stream().filter(c -> !configs.containsKey(c)).collect(Collectors.toList());
        if (!invalidConfigs.isEmpty())
            throw new InvalidConfigurationException("Invalid config(s): " + Utils.join(invalidConfigs, ","));

        configs.putAll(configsToBeAdded);
        configsToBeDeleted.forEach(configs::remove);

        adminZkClient.changeConfigs(entityType, entityName, configs, isUserClientId);

        System.out.println("Completed updating config for entity: " + entity + ".");
    }

    private static void validateBrokersNotRunning(String entityName,
                                                  AdminZkClient adminZkClient,
                                                  KafkaZkClient zkClient,
                                                  String errorMessage) {
        boolean perBrokerConfig = !Objects.equals(entityName, ConfigEntityName.DEFAULT);
        String info = "Broker configuration operations using ZooKeeper are only supported if the affected broker(s) are not running.";
        if (perBrokerConfig) {
            adminZkClient.parseBroker(entityName).foreach(brokerId -> {
                require(zkClient.getBroker((Integer) brokerId).isEmpty(), errorMessage + " - broker " + brokerId + " is running. " + info);
                return null;
            });
        } else {
            int runningBrokersCount = zkClient.getAllBrokersInCluster().size();
            require(runningBrokersCount == 0, errorMessage + " - " + runningBrokersCount + " brokers are running. " + info);
        }
    }

    private static void preProcessScramCredentials(Properties configsToBeAdded) {
    }

    private static PasswordEncoder createPasswordEncoder(Properties encoderConfigs) {
        encoderConfigs.get(PasswordEncoderConfigs.SECRET);
        String encoderSecret = (String) encoderConfigs.get(PasswordEncoderConfigs.SECRET);
        if (encoderSecret == null)
            throw new IllegalArgumentException("Password encoder secret not specified");
        String keyLength = (String) encoderConfigs.get(PasswordEncoderConfigs.KEY_LENGTH);
        String iteration = (String) encoderConfigs.get(PasswordEncoderConfigs.ITERATIONS);
        return PasswordEncoder.encrypting(new Password(encoderSecret),
                null,
                encoderConfigs.getProperty(PasswordEncoderConfigs.CIPHER_ALGORITHM, PasswordEncoderConfigs.DEFAULT_CIPHER_ALGORITHM),
                keyLength != null ? Integer.parseInt(keyLength) : PasswordEncoderConfigs.DEFAULT_KEY_LENGTH,
                iteration != null ? Integer.parseInt(iteration) : PasswordEncoderConfigs.DEFAULT_ITERATIONS);
    }

    /**
     * Pre-process broker configs provided to convert them to persistent format.
     * Password configs are encrypted using the secret `PasswordEncoderConfigs.SECRET`.
     * The secret is removed from `configsToBeAdded` and will not be persisted in ZooKeeper.
     */
    private static void preProcessBrokerConfigs(Properties configsToBeAdded, boolean perBrokerConfig) {
        Properties passwordEncoderConfigs = new Properties();
        configsToBeAdded.stringPropertyNames().forEach(key -> {
            if (key.startsWith("password.encoder."))
                passwordEncoderConfigs.put(key, configsToBeAdded.get(key));
        });

        if (!passwordEncoderConfigs.isEmpty()) {
            logger.info("Password encoder configs " + passwordEncoderConfigs.keySet() + " will be used for encrypting" +
                    " passwords, but will not be stored in ZooKeeper.");
            passwordEncoderConfigs.stringPropertyNames().forEach(configsToBeAdded::remove);
        }

        DynamicBrokerConfig.validateConfigs(configsToBeAdded, perBrokerConfig);
        List<String> passwordConfigs = configsToBeAdded.stringPropertyNames().stream().filter(DynamicBrokerConfig::isPasswordConfig).collect(Collectors.toList());
        if (!passwordConfigs.isEmpty()) {
            require(passwordEncoderConfigs.containsKey(PasswordEncoderConfigs.SECRET),
                PasswordEncoderConfigs.SECRET + " must be specified to update " + passwordConfigs + "." +
                    " Other password encoder configs like cipher algorithm and iterations may also be specified" +
                    " to override the default encoding parameters. Password encoder configs will not be persisted" +
                    " in ZooKeeper."
            );

            PasswordEncoder passwordEncoder = createPasswordEncoder(passwordEncoderConfigs);
            passwordConfigs.forEach(configName -> {
                try {
                    String encodedValue = passwordEncoder.encode(new Password(configsToBeAdded.getProperty(configName)));
                    configsToBeAdded.setProperty(configName, encodedValue);
                } catch (GeneralSecurityException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static void describeConfigWithZk(KafkaZkClient zkClient, ConfigCommandOptions opts, AdminZkClient adminZkClient) {
        ConfigEntity configEntity = parseEntity(opts);
        String entityType = configEntity.root.entityType;
        boolean describeAllUsers = Objects.equals(entityType, ConfigType.USER) && !configEntity.root.sanitizedName.isPresent() && !configEntity.child.isPresent();
        String entityName = configEntity.fullSanitizedName();
        String errorMessage = "--bootstrap-server option must be specified to describe " + entityType;
        if (Objects.equals(entityType, ConfigType.BROKER)) {
            // Dynamic broker configs can be described using ZooKeeper only if the corresponding broker is not running.
            validateBrokersNotRunning(entityName, adminZkClient, zkClient, errorMessage);
        }

        List<ConfigEntity> entities = configEntity.getAllEntities(zkClient);
        for (ConfigEntity entity : entities) {
            Properties configs = adminZkClient.fetchEntityConfig(entity.root.entityType, entity.fullSanitizedName());
            // When describing all users, don't include empty user nodes with only <user, client> quota overrides.
            if (!configs.isEmpty() || !describeAllUsers) {
                System.out.printf("Configs for %s are %s",
                    entity, configs.entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue()).collect(Collectors.joining(",")));
            }
        }
    }

    private static Properties parseConfigsToBeAdded(ConfigCommandOptions opts) throws IOException {
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
            List<String[]> configsToBeAdded = Arrays.stream(opts.options.valueOf(opts.addConfig)
                    .split("," + pattern))
                    .map(c -> c.split("\s*=\s*" + pattern, -1))
                    .collect(Collectors.toList());
            require(configsToBeAdded.stream().allMatch(config -> config.length == 2), "Invalid entity config: all configs to be added must be in the format \"key=val\".");
            //Create properties, parsing square brackets from values if necessary
            configsToBeAdded.forEach(pair -> props.setProperty(pair[0].trim(), pair[1].replaceAll("\\[?\\]?", "").trim()));
        }
        if (props.containsKey(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)) {
            System.out.print("WARNING: The configuration " + TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG + "=" + props.getProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG) + " is specified. " +
                "This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker or " +
                "if the inter.broker.protocol.version is 3.0 or newer. This configuration is deprecated and it will be removed in Apache Kafka 4.0.");
        }
        validatePropsKey(props);
        return props;
    }

    private static List<String> parseConfigsToBeDeleted(ConfigCommandOptions opts) {
        return opts.options.has(opts.deleteConfig)
            ? opts.options.valuesOf(opts.deleteConfig).stream().map(String::trim).collect(Collectors.toList())
            : Collections.emptyList();
    }

    private static void validatePropsKey(Properties props) {
        props.keySet().forEach(propsKey -> {
            if (!propsKey.toString().matches("[a-zA-Z0-9._-]*")) {
                throw new IllegalArgumentException(
                    "Invalid character found for config key: " + propsKey
                );
            }
        });
    }

    private static void processCommand(ConfigCommandOptions opts) throws IOException {
        Properties props = opts.options.has(opts.commandConfigOpt)
            ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
            : new Properties();

        CommandLineUtils.initializeBootstrapProperties(opts.parser,
            opts.options,
            props,
            opts.bootstrapServerOpt,
            opts.bootstrapControllerOpt);
        Admin adminClient = Admin.create(props);

        if (opts.options.has(opts.alterOpt) && opts.entityTypes().size() != opts.entityNames().size())
            throw new IllegalArgumentException("An entity name must be specified for every entity type");

        try {
            if (opts.options.has(opts.alterOpt))
                alterConfig(adminClient, opts);
            else if (opts.options.has(opts.describeOpt))
                describeConfig(adminClient, opts);
        } finally {
            adminClient.close();
        }
    }

    private static void alterConfig(Admin adminClient, ConfigCommandOptions opts) {

    }

    private static void alterUserScramCredentialConfigs(Admin adminClient, String user, Map<String, ConfigEntry> scramConfigsToAddMap, List<String> scramConfigsToDelete) {

    }

    private static void alterQuotaConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames, Map<String, String> configsToBeAddedMap, List<String> configsToBeDeleted) {

    }

    private static void describeConfig(Admin adminClient, ConfigCommandOptions opts) {

    }

    private static void describeResourceConfig(Admin adminClient, String entityType, Optional<String> entityName, boolean describeAll) {

    }

    private static List<ConfigEntry> getResourceConfig(Admin adminClient, String entityType, String entityName, boolean includeSynonyms, boolean describeAll) {
        return null;
    }

    private static void describeQuotaConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) {
        Map<ClientQuotaEntity, Map<String, Double>> quotaConfigs = getAllClientQuotasConfigs(adminClient, entityTypes, entityNames);
        quotaConfigs.forEach((entity, entries) -> {
            Map<String, String> entityEntries = entity.entries();
            Function<String, Optional<String>> entitySubstr = entityType -> {
                if (!entityEntries.containsKey(entityType))
                    return Optional.empty();
                String name = entityEntries.get(entityType);
                String typeStr = "";
                switch (entityType) {
                    case ClientQuotaEntity.USER:
                        typeStr = "user-principal";
                        break;
                    case ClientQuotaEntity.CLIENT_ID:
                        typeStr = "client-id";
                        break;
                    case ClientQuotaEntity.IP:
                        typeStr = "ip";
                        break;
                }
                //TODO: CHECKME
                return Optional.of(name != null
                    ? typeStr + " '" + name + "'"
                    : "the default " + typeStr);
            };

            val entityStr = Arrays.asList(
                    entitySubstr.apply(ClientQuotaEntity.USER),
                    entitySubstr(ClientQuotaEntity.CLIENT_ID),
                    entitySubstr(ClientQuotaEntity.IP)).mkString(", ")
            val entriesStr = entries.asScala.map(e => s"${e._1}=${e._2}").mkString(", ");

            System.out.println("Quota configs for " + entityStr + " are " + entriesStr);
        });
    }

    private static void describeClientQuotaAndUserScramCredentialConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws ExecutionException, InterruptedException, TimeoutException {
        describeQuotaConfigs(adminClient, entityTypes, entityNames);
        // we describe user SCRAM credentials only when we are not describing client information
        // and we are not given either --entity-default or --user-defaults
        if (!entityTypes.contains(ConfigType.CLIENT) && !entityNames.contains("")) {
            DescribeUserScramCredentialsResult result = adminClient.describeUserScramCredentials(entityNames);
            result.users().get(30, TimeUnit.SECONDS).forEach(user -> {
                try {
                    UserScramCredentialsDescription description = result.description(user).get(30, TimeUnit.SECONDS);
                    String descriptionText = description.credentialInfos().stream().map(info ->
                            info.mechanism().mechanismName() + "=iterations=" + info.iterations()).collect(Collectors.joining(", "));
                    System.out.println("SCRAM credential configs for user-principal '" + user + "' are " + descriptionText);
                } catch (Exception e) {
                    System.out.println("Error retrieving SCRAM credential configs for user-principal '" + user + "': " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            });
        }
    }

    private static Map<String, Double> getClientQuotasConfig(Admin adminClient, List<String> entityTypes, List<String> entityNames) {
        if (entityTypes.size() != entityNames.size())
            throw new IllegalArgumentException("Exactly one entity name must be specified for every entity type");
        Map<ClientQuotaEntity, Map<String, Double>> cfg = getAllClientQuotasConfigs(adminClient, entityTypes, entityNames);
        return !cfg.isEmpty() ? cfg.values().iterator().next() : Collections.emptyMap();
    }

    private static Map<ClientQuotaEntity, Map<String, Double>> getAllClientQuotasConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) {
        return null;
    }

    private static ConfigEntity parseEntity(ConfigCommandOptions opts) {
        List<String> entityTypes = opts.entityTypes();
        List<String> entityNames = opts.entityNames();
        if (Objects.equals(entityTypes.get(0), ConfigType.USER) || Objects.equals(entityTypes.get(0), ConfigType.CLIENT))
            return parseClientQuotaEntity(opts, entityTypes, entityNames);
        else {
            // Exactly one entity type and at-most one entity name expected for other entities
            Optional<String> name = entityNames.isEmpty()
                ? Optional.empty()
                : Optional.of(entityNames.get(0).isEmpty() ? ConfigEntityName.DEFAULT : entityNames.get(0));
            return new ConfigEntity(new Entity(entityTypes.get(0), name), Optional.empty());
        }
    }

    private static String sanitizeName(String entityType, String name) {
        if (name.isEmpty())
            return ConfigEntityName.DEFAULT;
        else {
            switch (entityType) {
                case ConfigType.USER:
                case ConfigType.CLIENT:
                    return Sanitizer.sanitize(name);
                default:
                    throw new IllegalArgumentException("Invalid entity type " + entityType);
            }
        }
    }

    private static ConfigEntity parseClientQuotaEntity(ConfigCommandOptions opts, List<String> types, List<String> names) {
        if (opts.options.has(opts.alterOpt) && names.size() != types.size())
            throw new IllegalArgumentException("--entity-name or --entity-default must be specified with each --entity-type for --alter");

        boolean reverse = types.size() == 2 && Objects.equals(types.get(0), ConfigType.CLIENT);
        List<String> entityTypes = reverse ? reverse(types) : types;
        Iterator<String> sortedNames = ((reverse && names.size() == 2) ? reverse(names) : names).iterator();

        List<Entity> entities = entityTypes.stream()
                .map(t -> new Entity(t, sortedNames.hasNext() ? Optional.of(sanitizeName(t, sortedNames.next())) : Optional.empty()))
                .collect(Collectors.toList());

        return new ConfigEntity(entities.get(0), (entities.size() > 1) ? Optional.of(entities.get(1)) : Optional.empty());
    }

    public static <V> List<V> reverse(List<V> list) {
        List<V> res = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            res.add(list.get(list.size() - i));
        }
        return res;
    }

    public static void require(boolean requirement, String message) {
        if (!requirement)
            throw new IllegalArgumentException("requirement failed: " + message);
    }
}
