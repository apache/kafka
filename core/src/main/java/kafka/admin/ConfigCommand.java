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
package kafka.admin;

import joptsimple.OptionException;
import kafka.server.DynamicBrokerConfig;
import kafka.server.DynamicConfig;
import kafka.server.KafkaConfig;
import kafka.utils.Exit;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ClientMetricsResourceListing;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.UserScramCredentialAlteration;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.security.PasswordEncoder;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.server.config.ConfigType;
import org.apache.kafka.server.config.QuotaConfigs;
import org.apache.kafka.server.config.ZooKeeperInternals;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public static final String BROKER_DEFAULT_ENTITY_NAME = "";
    public static final String BROKER_LOGGER_CONFIG_TYPE = "broker-loggers";

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigCommand.class);

    static final List<String> BROKER_SUPPORTED_CONFIG_TYPES;
    static final List<String> ZK_SUPPORTED_CONFIG_TYPES = Arrays.asList(ConfigType.USER, ConfigType.BROKER);
    static final int DEFAULT_SCRAM_ITERATIONS = 4096;

    static {
        List<String> brokerSupportedConfigTypes = new ArrayList<>(ConfigType.ALL);
        brokerSupportedConfigTypes.add(BROKER_LOGGER_CONFIG_TYPE);
        brokerSupportedConfigTypes.add(ConfigType.CLIENT_METRICS);

        BROKER_SUPPORTED_CONFIG_TYPES = Collections.unmodifiableList(brokerSupportedConfigTypes);
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
            LOGGER.debug("Failed config command with args '" + Utils.join(args, " ") + "'", e);
            System.err.println(e.getMessage());
            Exit.exit(1, scala.None$.empty());
        } catch (Throwable t) {
            LOGGER.debug("Error while executing config command with args '" + Utils.join(args, " ") + "'", t);
            System.err.println("Error while executing config command with args '" + Utils.join(args, " ") + "'");
            t.printStackTrace(System.err);
            Exit.exit(1, scala.None$.empty());
        }
    }

    private static void processCommandWithZk(String zkConnectString, ConfigCommandOptions opts) throws Exception {
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

    static void alterConfigWithZk(KafkaZkClient zkClient, ConfigCommandOptions opts, AdminZkClient adminZkClient) throws Exception {
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

                boolean perBrokerConfig = !Objects.equals(entityName, ZooKeeperInternals.DEFAULT_STRING);
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
        boolean perBrokerConfig = !Objects.equals(entityName, ZooKeeperInternals.DEFAULT_STRING);
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

    private static String scramCredential(ScramMechanism mechanism, String credentialStr) throws NoSuchAlgorithmException {
        Pattern pattern = Pattern.compile("(?:iterations=([0-9]*),)?password=(.*)");
        Matcher matcher = pattern.matcher(credentialStr);
        if (!matcher.matches())
            throw new IllegalArgumentException("Invalid credential property " + mechanism + "=" + credentialStr);

        int iterations = matcher.group(1) != null
            ? Integer.parseInt(matcher.group(1))
            : DEFAULT_SCRAM_ITERATIONS;
        String password = matcher.group(2);

        if (iterations < mechanism.minIterations())
            throw new IllegalArgumentException("Iterations " + iterations + " is less than the minimum " + mechanism.minIterations() + " required for " + mechanism);
        ScramCredential credential = new ScramFormatter(mechanism).generateCredential(password, iterations);
        return ScramCredentialUtils.credentialToString(credential);
    }

    private static void preProcessScramCredentials(Properties configsToBeAdded) throws NoSuchAlgorithmException {
        for (ScramMechanism mechanism : ScramMechanism.values()) {
            String value = configsToBeAdded.getProperty(mechanism.mechanismName());
            if (value != null)
                configsToBeAdded.setProperty(mechanism.mechanismName(), scramCredential(mechanism, value));
        }
    }

    static PasswordEncoder createPasswordEncoder(Map<String, String> encoderConfigs) {
        encoderConfigs.get(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG);
        String encoderSecret = encoderConfigs.get(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG);
        if (encoderSecret == null)
            throw new IllegalArgumentException("Password encoder secret not specified");
        String keyLength = encoderConfigs.get(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG);
        String iteration = encoderConfigs.get(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG);
        return PasswordEncoder.encrypting(new Password(encoderSecret),
                null,
                encoderConfigs.getOrDefault(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG, PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_DEFAULT),
                keyLength != null ? Integer.parseInt(keyLength) : PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_DEFAULT,
                iteration != null ? Integer.parseInt(iteration) : PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_DEFAULT);
    }

    /**
     * Pre-process broker configs provided to convert them to persistent format.
     * Password configs are encrypted using the secret `PasswordEncoderConfigs.SECRET`.
     * The secret is removed from `configsToBeAdded` and will not be persisted in ZooKeeper.
     */
    private static void preProcessBrokerConfigs(Properties configsToBeAdded, boolean perBrokerConfig) {
        Map<String, String> passwordEncoderConfigs = new HashMap<>();
        configsToBeAdded.stringPropertyNames().forEach(key -> {
            if (key.startsWith("password.encoder."))
                passwordEncoderConfigs.put(key, (String) configsToBeAdded.get(key));
        });

        if (!passwordEncoderConfigs.isEmpty()) {
            LOGGER.info("Password encoder configs " + passwordEncoderConfigs.keySet() + " will be used for encrypting" +
                    " passwords, but will not be stored in ZooKeeper.");
            passwordEncoderConfigs.keySet().forEach(configsToBeAdded::remove);
        }

        DynamicBrokerConfig.validateConfigs(configsToBeAdded, perBrokerConfig);
        List<String> passwordConfigs = configsToBeAdded.stringPropertyNames().stream().filter(DynamicBrokerConfig::isPasswordConfig).collect(Collectors.toList());
        if (!passwordConfigs.isEmpty()) {
            require(passwordEncoderConfigs.containsKey(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG),
                PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG + " must be specified to update " + passwordConfigs + "." +
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

    static void describeConfigWithZk(KafkaZkClient zkClient, ConfigCommandOptions opts, AdminZkClient adminZkClient) {
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

    static Properties parseConfigsToBeAdded(ConfigCommandOptions opts) throws Exception {
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
                    .map(c -> c.split("\\s*=\\s*" + pattern, -1))
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

    static List<String> parseConfigsToBeDeleted(ConfigCommandOptions opts) {
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

    private static void processCommand(ConfigCommandOptions opts) throws Exception {
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

    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity", "JavaNCSS", "MethodLength"})
    static void alterConfig(Admin adminClient, ConfigCommandOptions opts) throws Exception {
        List<String> entityTypes = opts.entityTypes();
        List<String> entityNames = opts.entityNames();
        String entityTypeHead = entityTypes.get(0);
        String entityNameHead = entityNames.get(0);
        Map<String, String> configsToBeAddedMap = new HashMap<>();
        parseConfigsToBeAdded(opts).forEach((k, v) -> configsToBeAddedMap.put((String) k, (String) v));
        Map<String, ConfigEntry> configsToBeAdded = configsToBeAddedMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new ConfigEntry(e.getKey(), e.getValue())));
        List<String> configsToBeDeleted = parseConfigsToBeDeleted(opts);

        Map<String, ConfigEntry> oldConfig;
        List<String> invalidConfigs;
        ConfigResource configResource;
        AlterConfigsOptions alterOptions;
        List<AlterConfigOp> alterEntries;

        switch (entityTypeHead) {
            case ConfigType.TOPIC:
                oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, false, false)
                    .stream()
                    .collect(Collectors.toMap(ConfigEntry::name, Function.identity()));

                // fail the command if any of the configs to be deleted does not exist
                invalidConfigs = configsToBeDeleted.stream().filter(cfg -> !oldConfig.containsKey(cfg))
                    .collect(Collectors.toList());
                if (!invalidConfigs.isEmpty())
                    throw new InvalidConfigurationException("Invalid config(s): " + String.join(",", invalidConfigs));

                configResource = new ConfigResource(ConfigResource.Type.TOPIC, entityNameHead);
                alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false);
                alterEntries = configsToBeAdded.values().stream()
                    .map(v -> new AlterConfigOp(v, AlterConfigOp.OpType.SET))
                    .collect(Collectors.toList());

                configsToBeDeleted.stream()
                    .map(k -> new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE))
                    .forEach(alterEntries::add);
                adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, alterEntries), alterOptions).all().get(60, TimeUnit.SECONDS);
                break;
            case ConfigType.BROKER:
                oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, false, false)
                    .stream()
                    .collect(Collectors.toMap(ConfigEntry::name, Function.identity()));

                // fail the command if any of the configs to be deleted does not exist
                invalidConfigs = configsToBeDeleted.stream().filter(cfg -> !oldConfig.containsKey(cfg))
                    .collect(Collectors.toList());
                if (!invalidConfigs.isEmpty())
                    throw new InvalidConfigurationException("Invalid config(s): " + String.join(",", invalidConfigs));

                Map<String, ConfigEntry> newEntries = new HashMap<>(oldConfig);

                newEntries.putAll(configsToBeAdded);
                configsToBeDeleted.forEach(newEntries::remove);

                Map<String, ConfigEntry> sensitiveEntries = newEntries.entrySet().stream()
                    .filter(e -> e.getValue().value() == null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (!sensitiveEntries.isEmpty())
                    throw new InvalidConfigurationException("All sensitive broker config entries must be specified for --alter, missing entries: " + sensitiveEntries.keySet());
                Config newConfig = new Config(newEntries.values());

                configResource = new ConfigResource(ConfigResource.Type.BROKER, entityNameHead);
                alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false);
                adminClient.alterConfigs(Collections.singletonMap(configResource, newConfig), alterOptions).all().get(60, TimeUnit.SECONDS);
                break;

            case BROKER_LOGGER_CONFIG_TYPE:
                List<String> validLoggers = getResourceConfig(adminClient, entityTypeHead, entityNameHead, true, false)
                    .stream()
                    .map(ConfigEntry::name)
                    .collect(Collectors.toList());
                // fail the command if any of the configured broker loggers do not exist
                List<String> invalidBrokerLoggers = configsToBeDeleted.stream()
                    .filter(l -> !validLoggers.contains(l))
                    .collect(Collectors.toList());
                configsToBeAdded.keySet().stream()
                    .filter(cfg -> !validLoggers.contains(cfg))
                    .forEach(invalidBrokerLoggers::add);
                if (!invalidBrokerLoggers.isEmpty())
                    throw new InvalidConfigurationException("Invalid broker logger(s): " + String.join(",", invalidBrokerLoggers));

                configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, entityNameHead);
                alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false);

                List<AlterConfigOp> alterLogLevelEntries = configsToBeAdded.values().stream()
                    .map(cfg -> new AlterConfigOp(cfg, AlterConfigOp.OpType.SET))
                    .collect(Collectors.toList());

                configsToBeDeleted.stream()
                    .map(k -> new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE))
                    .forEach(alterLogLevelEntries::add);

                adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, alterLogLevelEntries), alterOptions).all().get(60, TimeUnit.SECONDS);
                break;

            case ConfigType.USER:
            case ConfigType.CLIENT:
                boolean hasQuotaConfigsToAdd = configsToBeAdded.keySet().stream().anyMatch(QuotaConfigs::isClientOrUserQuotaConfig);
                Map<String, ConfigEntry> scramConfigsToAddMap = configsToBeAdded.entrySet().stream()
                    .filter(entry -> ScramMechanism.isScram(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                List<String> unknownConfigsToAdd = configsToBeAdded.keySet().stream()
                    .filter(key -> !(ScramMechanism.isScram(key) || QuotaConfigs.isClientOrUserQuotaConfig(key)))
                    .collect(Collectors.toList());
                boolean hasQuotaConfigsToDelete = configsToBeDeleted.stream().anyMatch(QuotaConfigs::isClientOrUserQuotaConfig);
                List<String> scramConfigsToDelete = configsToBeDeleted.stream()
                    .filter(ScramMechanism::isScram)
                    .collect(Collectors.toList());
                List<String> unknownConfigsToDelete = configsToBeDeleted
                    .stream()
                    .filter(key -> !(ScramMechanism.isScram(key) || QuotaConfigs.isClientOrUserQuotaConfig(key)))
                    .collect(Collectors.toList());

                if (entityTypeHead.equals(ConfigType.CLIENT) || entityTypes.size() == 2) { // size==2 for case where users is specified first on the command line, before clients
                    // either just a client or both a user and a client
                    if (!unknownConfigsToAdd.isEmpty() || !scramConfigsToAddMap.isEmpty()) {
                        unknownConfigsToAdd.addAll(scramConfigsToAddMap.keySet());
                        throw new IllegalArgumentException("Only quota configs can be added for '" + ConfigType.CLIENT + "' using --bootstrap-server. Unexpected config names: " + unknownConfigsToAdd);
                    }
                    if (!unknownConfigsToDelete.isEmpty() || !scramConfigsToDelete.isEmpty()) {
                        unknownConfigsToDelete.addAll(scramConfigsToDelete);
                        throw new IllegalArgumentException("Only quota configs can be deleted for '" + ConfigType.CLIENT + "' using --bootstrap-server. Unexpected config names: " + unknownConfigsToDelete);
                    }
                } else { // ConfigType.User
                    if (!unknownConfigsToAdd.isEmpty())
                        throw new IllegalArgumentException("Only quota and SCRAM credential configs can be added for '" + ConfigType.USER + "' using --bootstrap-server. Unexpected config names: " + unknownConfigsToAdd);
                    if (!unknownConfigsToDelete.isEmpty())
                        throw new IllegalArgumentException("Only quota and SCRAM credential configs can be deleted for '" + ConfigType.USER + "' using --bootstrap-server. Unexpected config names: " + unknownConfigsToDelete);
                    if (!scramConfigsToAddMap.isEmpty() || !scramConfigsToDelete.isEmpty()) {
                        if (entityNames.stream().anyMatch(String::isEmpty)) // either --entity-type users --entity-default or --user-defaults
                            throw new IllegalArgumentException("The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.");
                        if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete)
                            throw new IllegalArgumentException("Cannot alter both quota and SCRAM credential configs simultaneously for '" + ConfigType.USER + "' using --bootstrap-server.");
                    }
                }

                if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete) {
                    alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted);
                } else {
                    // handle altering user SCRAM credential configs
                    if (entityNames.size() != 1)
                        // should never happen, if we get here then it is a bug
                        throw new IllegalStateException("Altering user SCRAM credentials should never occur for more zero or multiple users: " + entityNames);
                    alterUserScramCredentialConfigs(adminClient, entityNames.get(0), scramConfigsToAddMap, scramConfigsToDelete);
                }

                break;

            case ConfigType.IP:
                List<String> unknownConfigs = Stream.concat(configsToBeAdded.keySet().stream(), configsToBeDeleted.stream())
                    .filter(key -> !DynamicConfig.Ip$.MODULE$.names().contains(key))
                    .collect(Collectors.toList());
                if (!unknownConfigs.isEmpty())
                    throw new IllegalArgumentException("Only connection quota configs can be added for '" + ConfigType.IP + "' using --bootstrap-server. Unexpected config names: " + String.join(",", unknownConfigs));
                alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted);
                break;

            case ConfigType.CLIENT_METRICS:
                oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, false, false)
                    .stream()
                    .collect(Collectors.toMap(ConfigEntry::name, Function.identity()));

                // fail the command if any of the configs to be deleted does not exist
                invalidConfigs = configsToBeDeleted.stream().filter(cfg -> !oldConfig.containsKey(cfg))
                    .collect(Collectors.toList());
                if (!invalidConfigs.isEmpty())
                    throw new InvalidConfigurationException("Invalid config(s): " + String.join(",", invalidConfigs));

                configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, entityNameHead);
                alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false);
                alterEntries = configsToBeAdded.values()
                    .stream()
                    .map(cfg -> new AlterConfigOp(cfg, AlterConfigOp.OpType.SET))
                    .collect(Collectors.toList());
                configsToBeDeleted.stream()
                    .map(k -> new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE))
                    .forEach(alterEntries::add);

                adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, alterEntries), alterOptions).all().get(60, TimeUnit.SECONDS);
                break;

            default:
                throw new IllegalArgumentException("Unsupported entity type: " + entityTypeHead);
        }

        if (!entityNameHead.isEmpty())
            System.out.println("Completed updating config for " + entityTypeHead.substring(0, entityTypeHead.length() - 1) + " " + entityNameHead + ".");
        else
            System.out.println("Completed updating default config for " + entityTypeHead + " in the cluster.");
    }

    public static Tuple2<Integer, byte[]> iterationsAndPasswordBytes(ScramMechanism mechanism, String credentialStr) {
        Pattern pattern = Pattern.compile("(?:iterations=(\\-?[0-9]*),)?password=(.*)");
        Matcher matcher = pattern.matcher(credentialStr);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid credential property " + mechanism + "=" + credentialStr);
        }

        int iterations = matcher.group(1) != null
            ? Integer.parseInt(matcher.group(1))
            : DEFAULT_SCRAM_ITERATIONS;
        String password = matcher.group(2);

        if (iterations < mechanism.minIterations())
            throw new IllegalArgumentException("Iterations " + iterations + " is less than the minimum " + mechanism.minIterations() + " required for " + mechanism.mechanismName());
        return new Tuple2<>(iterations, password.getBytes(StandardCharsets.UTF_8));
    }

    private static void alterUserScramCredentialConfigs(Admin adminClient, String user, Map<String, ConfigEntry> scramConfigsToAddMap, List<String> scramConfigsToDelete) throws Exception {
        List<UserScramCredentialAlteration> deletions = scramConfigsToDelete.stream()
            .map(mechanismName -> new UserScramCredentialDeletion(
                user,
                org.apache.kafka.clients.admin.ScramMechanism.fromMechanismName(mechanismName)))
            .collect(Collectors.toList());

        List<UserScramCredentialUpsertion> upsertions = scramConfigsToAddMap.entrySet().stream().map(e -> {
            String mechanismName = e.getKey();
            ConfigEntry configEntry = e.getValue();
            Tuple2<Integer, byte[]> t = iterationsAndPasswordBytes(ScramMechanism.forMechanismName(mechanismName), configEntry.value());
            Integer iterations = t.v1;
            byte[] passwordBytes = t.v2;
            return new UserScramCredentialUpsertion(
                user,
                new ScramCredentialInfo(org.apache.kafka.clients.admin.ScramMechanism.fromMechanismName(mechanismName), iterations), passwordBytes);
        }).collect(Collectors.toList());
        // we are altering only a single user by definition, so we don't have to worry about one user succeeding and another
        // failing; therefore just check the success of all the futures (since there will only be 1)
        deletions.addAll(upsertions);
        adminClient.alterUserScramCredentials(deletions).all().get(60, TimeUnit.SECONDS);
    }

    private static void alterQuotaConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames, Map<String, String> configsToBeAddedMap, List<String> configsToBeDeleted) throws Exception {
        // handle altering client/user quota configs
        Map<String, Double> oldConfig = getClientQuotasConfig(adminClient, entityTypes, entityNames);

        List<String> invalidConfigs = configsToBeDeleted.stream()
            .filter(cfg -> !oldConfig.containsKey(cfg))
            .collect(Collectors.toList());
        if (!invalidConfigs.isEmpty())
            throw new InvalidConfigurationException("Invalid config(s): " + String.join(",", invalidConfigs));

        List<String> alterEntityTypes = entityTypes.stream().map(entType -> {
            switch (entType) {
                case ConfigType.USER:
                    return ClientQuotaEntity.USER;
                case ConfigType.CLIENT:
                    return ClientQuotaEntity.CLIENT_ID;
                case ConfigType.IP:
                    return ClientQuotaEntity.IP;
                default:
                    throw new IllegalArgumentException("Unexpected entity type: " + entType);
            }
        }).collect(Collectors.toList());

        List<String> alterEntityNames = entityNames.stream()
            .map(en -> !en.isEmpty() ? en : null)
            .collect(Collectors.toList());

        // Explicitly populate a HashMap to ensure nulls are recorded properly.
        Map<String, String> alterEntityMap = new HashMap<>();
        for (int i = 0; i < alterEntityTypes.size(); i++) {
            alterEntityMap.put(alterEntityTypes.get(i), alterEntityNames.get(i));
        }
        ClientQuotaEntity entity = new ClientQuotaEntity(alterEntityMap);

        AlterClientQuotasOptions alterOptions = new AlterClientQuotasOptions().validateOnly(false);
        List<ClientQuotaAlteration.Op> alterOps = configsToBeAddedMap.entrySet().stream().map(e -> {
            double doubleValue;
            try {
                doubleValue = Double.parseDouble(e.getValue());
            } catch (NumberFormatException __) {
                throw new IllegalArgumentException("Cannot parse quota configuration value for " + e.getKey() + ": " + e.getValue());
            }
            return new ClientQuotaAlteration.Op(e.getKey(), doubleValue);
        }).collect(Collectors.toList());

        configsToBeDeleted.stream()
            .map(key -> new ClientQuotaAlteration.Op(key, null))
            .forEach(alterOps::add);

        adminClient.alterClientQuotas(Collections.singleton(new ClientQuotaAlteration(entity, alterOps)), alterOptions)
            .all().get(60, TimeUnit.SECONDS);
    }

    static void describeConfig(Admin adminClient, ConfigCommandOptions opts) throws Exception {
        List<String> entityTypes = opts.entityTypes();
        List<String> entityNames = opts.entityNames();
        boolean describeAll = opts.options.has(opts.allOpt);

        switch (entityTypes.get(0)) {
            case ConfigType.TOPIC:
            case ConfigType.BROKER:
            case BROKER_LOGGER_CONFIG_TYPE:
            case ConfigType.CLIENT_METRICS:
                describeResourceConfig(
                    adminClient,
                    entityTypes.get(0),
                    entityNames.isEmpty() ? Optional.empty() : Optional.of(entityNames.get(0)),
                    describeAll);
                break;
            case ConfigType.USER:
            case ConfigType.CLIENT:
                describeClientQuotaAndUserScramCredentialConfigs(adminClient, entityTypes, entityNames);
                break;
            case ConfigType.IP:
                describeQuotaConfigs(adminClient, entityTypes, entityNames);
                break;
            default:
                throw new IllegalArgumentException("Invalid entity type: " + entityTypes.get(0));
        }
    }

    private static void describeResourceConfig(Admin adminClient, String entityType, Optional<String> entityName, boolean describeAll) {
        Collection<String> entities = entityName
            .map(e -> (Collection<String>) Collections.singletonList(e))
            .orElseGet(() -> {
                try {
                    switch (entityType) {
                        case ConfigType.TOPIC:
                            return adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
                        case ConfigType.BROKER:
                        case BROKER_LOGGER_CONFIG_TYPE:
                            List<String> res = adminClient.describeCluster(new DescribeClusterOptions()).nodes().get().stream()
                                .map(Node::idString)
                                .collect(Collectors.toList());
                            res.add(BROKER_DEFAULT_ENTITY_NAME);
                            return res;
                        case ConfigType.CLIENT_METRICS:
                            return adminClient.listClientMetricsResources().all().get().stream()
                                .map(ClientMetricsResourceListing::name)
                                .collect(Collectors.toList());
                        default:
                            throw new IllegalArgumentException("Invalid entity type: " + entityType);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

        entities.forEach(entity -> {
            if (entity.equals(BROKER_DEFAULT_ENTITY_NAME)) {
                System.out.println("Default configs for " + entityType + " in the cluster are:");
            } else {
                String configSourceStr = describeAll ? "All" : "Dynamic";
                System.out.println(configSourceStr + " configs for " + entityType.substring(0, entityType.length() - 1) + " " + entity + " are:");
            }
            try {
                getResourceConfig(adminClient, entityType, entity, true, describeAll).forEach(entry -> {
                    String synonyms = entry.synonyms().stream()
                        .map(synonym -> synonym.source() + ":" + synonym.name() + "=" + synonym.value())
                        .collect(Collectors.joining(", "));
                    System.out.println("  " + entry.name() + "=" + entry.value() + " sensitive=" + entry.isSensitive() + " synonyms={" + synonyms + "}");
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static List<ConfigEntry> getResourceConfig(Admin adminClient, String entityType, String entityName, boolean includeSynonyms, boolean describeAll) throws Exception {
        Runnable validateBrokerId = () -> {
            try {
                Integer.parseInt(entityName);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("The entity name for " + entityType + " must be a valid integer broker id, found: " + entityName);
            }
        };

        ConfigResource.Type configResourceType;
        Optional<ConfigEntry.ConfigSource> dynamicConfigSource;

        switch (entityType) {
            case ConfigType.TOPIC:
                if (!entityName.isEmpty())
                    Topic.validate(entityName);
                configResourceType = ConfigResource.Type.TOPIC;
                dynamicConfigSource = Optional.of(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
                break;
            case ConfigType.BROKER:
                if (entityName.equals(BROKER_DEFAULT_ENTITY_NAME)) {
                    configResourceType = ConfigResource.Type.BROKER;
                    dynamicConfigSource = Optional.of(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
                } else {
                    validateBrokerId.run();
                    configResourceType = ConfigResource.Type.BROKER;
                    dynamicConfigSource = Optional.of(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG);
                }
                break;
            case BROKER_LOGGER_CONFIG_TYPE:
                if (!entityName.isEmpty())
                    validateBrokerId.run();
                configResourceType = ConfigResource.Type.BROKER_LOGGER;
                dynamicConfigSource = Optional.empty();
                break;
            case ConfigType.CLIENT_METRICS:
                configResourceType = ConfigResource.Type.CLIENT_METRICS;
                dynamicConfigSource = Optional.of(ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG);
                break;
            default:
                throw new IllegalArgumentException("Invalid entity type: " + entityType);
        }

        Optional<ConfigEntry.ConfigSource> configSourceFilter = describeAll ? Optional.empty() : dynamicConfigSource;

        ConfigResource configResource = new ConfigResource(configResourceType, entityName);
        DescribeConfigsOptions describeOptions = new DescribeConfigsOptions().includeSynonyms(includeSynonyms);
        Map<ConfigResource, Config> configs = adminClient.describeConfigs(Collections.singleton(configResource), describeOptions)
            .all().get(30, TimeUnit.SECONDS);

        return configs.get(configResource).entries().stream()
            .filter(entry -> configSourceFilter
                .map(configSource -> entry.source() == configSource)
                .orElse(true))
            .sorted(Comparator.comparing(ConfigEntry::name))
            .collect(Collectors.toList());
    }

    private static void describeQuotaConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws Exception {
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
                    default:
                        throw new IllegalStateException("Unknown entity type: " + entityType);
                }
                return Optional.of(name != null
                    ? typeStr + " '" + name + "'"
                    : "the default " + typeStr);
            };

            String entityStr = Stream.of(
                    entitySubstr.apply(ClientQuotaEntity.USER),
                    entitySubstr.apply(ClientQuotaEntity.CLIENT_ID),
                    entitySubstr.apply(ClientQuotaEntity.IP))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining(", "));
            String entriesStr = entries.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "));
            System.out.println("Quota configs for " + entityStr + " are " + entriesStr);
        });
    }

    private static void describeClientQuotaAndUserScramCredentialConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws Exception {
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

    private static Map<String, Double> getClientQuotasConfig(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws Exception {
        if (entityTypes.size() != entityNames.size())
            throw new IllegalArgumentException("Exactly one entity name must be specified for every entity type");
        Map<ClientQuotaEntity, Map<String, Double>> cfg = getAllClientQuotasConfigs(adminClient, entityTypes, entityNames);
        return !cfg.isEmpty() ? cfg.values().iterator().next() : Collections.emptyMap();
    }

    private static Map<ClientQuotaEntity, Map<String, Double>> getAllClientQuotasConfigs(Admin adminClient, List<String> entityTypes, List<String> entityNames) throws Exception {
        List<ClientQuotaFilterComponent> components = new ArrayList<>();
        int length = Math.max(entityTypes.size(), entityNames.size());
        for (int i = 0; i < length; i++) {
            String entityTypeStr = i < entityTypes.size() ? entityTypes.get(i) : null;
            String entityNameStr = i < entityNames.size() ? entityNames.get(i) : null;

            if (entityTypeStr == null)
                throw new IllegalArgumentException("More entity names specified than entity types");

            String entityType;
            switch (entityTypeStr) {
                case ConfigType.USER:
                    entityType = ClientQuotaEntity.USER;
                    break;
                case ConfigType.CLIENT:
                    entityType = ClientQuotaEntity.CLIENT_ID;
                    break;
                case ConfigType.IP:
                    entityType = ClientQuotaEntity.IP;
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected entity type " + entityTypeStr);
            }

            ClientQuotaFilterComponent entityName;
            if (entityNameStr == null) {
                entityName = ClientQuotaFilterComponent.ofEntityType(entityType);
            } else if (entityNameStr.isEmpty()) {
                entityName = ClientQuotaFilterComponent.ofDefaultEntity(entityType);
            } else {
                entityName = ClientQuotaFilterComponent.ofEntity(entityType, entityNameStr);
            }

            components.add(entityName);
        }

        return adminClient.describeClientQuotas(ClientQuotaFilter.containsOnly(components)).entities().get(30, TimeUnit.SECONDS);
    }

    static ConfigEntity parseEntity(ConfigCommandOptions opts) {
        List<String> entityTypes = opts.entityTypes();
        List<String> entityNames = opts.entityNames();
        if (Objects.equals(entityTypes.get(0), ConfigType.USER) || Objects.equals(entityTypes.get(0), ConfigType.CLIENT))
            return parseClientQuotaEntity(opts, entityTypes, entityNames);
        else {
            // Exactly one entity type and at-most one entity name expected for other entities
            Optional<String> name = entityNames.isEmpty()
                ? Optional.empty()
                : Optional.of(entityNames.get(0).isEmpty() ? ZooKeeperInternals.DEFAULT_STRING : entityNames.get(0));
            return new ConfigEntity(new Entity(entityTypes.get(0), name), Optional.empty());
        }
    }

    private static String sanitizeName(String entityType, String name) {
        if (name.isEmpty())
            return ZooKeeperInternals.DEFAULT_STRING;
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
            res.add(list.get(list.size() - i - 1));
        }
        return res;
    }

    public static void require(boolean requirement, String message) {
        if (!requirement)
            throw new IllegalArgumentException("requirement failed: " + message);
    }
}
