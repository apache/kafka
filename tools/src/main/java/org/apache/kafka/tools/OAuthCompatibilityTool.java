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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils.getConfiguredInstance;

public class OAuthCompatibilityTool {

    private static final String CLIENT_CONFIG_ARG = "client-config";
    private static final String BROKER_CONFIG_ARG = "broker-config";
    private static final ConfigDef SASL_CONFIG_DEF = saslConfigs();
    private static final ConfigDef SSL_CONFIG_DEF = sslConfigs();

    public static void main(String[] args) {
        ArgsHandler argsHandler = new ArgsHandler();
        Namespace namespace;

        try {
            namespace = argsHandler.parseArgs(args);
        } catch (ArgumentParserException e) {
            Exit.exit(1);
            return;
        }

        Properties clientFileProps = loadConfigFile(namespace, CLIENT_CONFIG_ARG);
        Properties brokerFileProps = loadConfigFile(namespace, BROKER_CONFIG_ARG);

        ConfigHandler clientConfigHandler = new ConfigHandler(namespace, clientFileProps);
        ConfigHandler brokerConfigHandler = new ConfigHandler(namespace, brokerFileProps);

        try {
            String jwt;

            {
                // Client side...
                try (JwtRetriever retriever = createRetriever(clientConfigHandler)) {
                    try (JwtValidator validator = createValidator(clientConfigHandler)) {
                        System.out.println("PASSED 1/5: client configuration");

                        jwt = retriever.retrieve();
                        System.out.println("PASSED 2/5: client JWT retrieval");

                        validator.validate(jwt);
                        System.out.println("PASSED 3/5: client JWT validation");
                    }
                }
            }

            {
                // Broker side...
                try (JwtValidator validator = createValidator(brokerConfigHandler)) {
                    System.out.println("PASSED 4/5: broker configuration");

                    validator.validate(jwt);
                    System.out.println("PASSED 5/5: broker JWT validation");
                }
            }

            System.out.println("SUCCESS");
            Exit.exit(0);
        } catch (Throwable t) {
            System.out.println("FAILED:");
            t.printStackTrace();

            if (t instanceof ConfigException) {
                System.out.printf("%n");
                argsHandler.parser.printHelp();
            }

            Exit.exit(1);
        }
    }

    private static Properties loadConfigFile(Namespace namespace, String argName) {
        String path = namespace.getString(argName);

        try {
            if (path == null || path.isEmpty())
                return new Properties();

            return Utils.loadProps(path);
        } catch (IOException e) {
            throw new KafkaException("Failed to load config file for --" + argName + ": " + path, e);
        }
    }

    private static JwtRetriever createRetriever(ConfigHandler configHandler) {
        return createConfiguredInstance(
                configHandler,
                SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS,
                JwtRetriever.class
        );
    }

    private static JwtValidator createValidator(ConfigHandler configHandler) {
        return createConfiguredInstance(
                configHandler,
                SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
                JwtValidator.class
        );
    }

    private static <T> T createConfiguredInstance(
            ConfigHandler configHandler,
            String configKey,
            Class<T> clazz
    ) {
        List<AppConfigurationEntry> jaasConfigEntries = List.of(
                new AppConfigurationEntry(
                        OAuthBearerLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        configHandler.getConfigs(SSL_CONFIG_DEF)
                )
        );

        return getConfiguredInstance(
                configHandler.getConfigs(SASL_CONFIG_DEF),
                OAUTHBEARER_MECHANISM,
                jaasConfigEntries,
                configKey,
                clazz
        );
    }

    private static ConfigDef saslConfigs() {
        ConfigDef allSaslConfigs = new ConfigDef();
        SaslConfigs.addClientSaslSupport(allSaslConfigs);

        ConfigDef filteredSaslConfigs = new ConfigDef();
        allSaslConfigs.configKeys().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("sasl.oauthbearer") ||
                        entry.getKey().startsWith("sasl.login"))
                .forEach(entry -> filteredSaslConfigs.define(entry.getValue()));

        return filteredSaslConfigs;
    }

    private static ConfigDef sslConfigs() {
        ConfigDef cd = new ConfigDef();
        SslConfigs.addClientSslSupport(cd);

        return cd;
    }

    static class ArgsHandler {

        private static final String DESCRIPTION = String.format(
            "This tool is used to verify OAuth/OIDC provider compatibility.%n%n" +
            "Run the following script to determine the configuration options:%n%n" +
                "    ./bin/kafka-run-class.sh %s --help",
            OAuthCompatibilityTool.class.getName());

        private final ArgumentParser parser;

        ArgsHandler() {
            this.parser = ArgumentParsers
                .newArgumentParser("oauth-compatibility-tool")
                .defaultHelp(true)
                .description(DESCRIPTION);
        }

        Namespace parseArgs(String[] args) throws ArgumentParserException {
            parser.addArgument("--" + CLIENT_CONFIG_ARG)
                    .metavar("path")
                    .dest(CLIENT_CONFIG_ARG)
                    .help("Path to a .properties file containing the client's OAuth/SSL configuration. " +
                            "Explicit command line options override any matching keys in this file.");

            parser.addArgument("--" + BROKER_CONFIG_ARG)
                    .metavar("path")
                    .dest(BROKER_CONFIG_ARG)
                    .help("Path to a .properties file containing the broker's OAuth/SSL configuration. " +
                            "Explicit command line options override any matching keys in this file.");

            Map<String, ConfigDef.ConfigKey> configs = new TreeMap<>();
            configs.putAll(SASL_CONFIG_DEF.configKeys());
            configs.putAll(SSL_CONFIG_DEF.configKeys());
            configs.forEach((key, value) -> addArgument(key, value.documentation));

            try {
                return parser.parseArgs(args);
            } catch (ArgumentParserException e) {
                parser.handleError(e);
                throw e;
            }
        }

        private void addArgument(String option, String help) {
            String name = "--" + option;

            parser.addArgument(name)
                .type(String.class)
                .metavar(option)
                .dest(option)
                .help(help);
        }
    }

    static class ConfigHandler {
        private final Namespace namespace;
        private final Properties fileProps;

        public ConfigHandler(Namespace namespace, Properties fileProps) {
            this.namespace = namespace;
            this.fileProps = fileProps;

            for (String key : fileProps.stringPropertyNames())
                if (namespace.getString(key) != null)
                    System.err.println("WARNING: command-line option --" + key + " overrides value from configuration file");
        }

        Map<String, ?> getConfigs(ConfigDef cd) {
            Map<String, Object> m = new HashMap<>();

            for (Map.Entry<String, ConfigDef.ConfigKey> entry : cd.configKeys().entrySet())
                maybeAdd(m, entry.getKey());

            return new AbstractConfig(cd, m).values();
        }

        private Optional<String> resolve(String key) {
            String cmdValue = namespace.getString(key);
            String fileValue = fileProps.getProperty(key);

            if (cmdValue != null)
                return Optional.of(cmdValue);

            return Optional.ofNullable(fileValue);
        }

        private void maybeAdd(Map<String, Object> m, String key) {
            resolve(key).ifPresent(v -> m.put(key, v));
        }
    }
}