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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.oauthbearer.JwtRetriever;
import org.apache.kafka.common.security.oauthbearer.JwtValidator;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.common.utils.Utils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROVIDER_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROVIDER_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_DOC;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_DOC;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_DOC;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_DOC;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils.getConfiguredInstance;

public class OAuthCompatibilityTool {

    private static final String CLIENT_CONFIG_ARG = "client-config";
    private static final String BROKER_CONFIG_ARG = "broker-config";
    private static final String CLIENT_ID_ARG = "client-id";
    private static final String CLIENT_SECRET_ARG = "client-secret";

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
                        configHandler.getJaasOptions()
                )
        );

        return getConfiguredInstance(
                configHandler.getConfigs(),
                OAUTHBEARER_MECHANISM,
                jaasConfigEntries,
                configKey,
                clazz
        );
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
            // File-based config options
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

            // SASL/OAuth
            addArgument(SASL_LOGIN_CONNECT_TIMEOUT_MS, SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC, Integer.class);
            addArgument(SASL_LOGIN_READ_TIMEOUT_MS, SASL_LOGIN_READ_TIMEOUT_MS_DOC, Integer.class);
            addArgument(SASL_LOGIN_RETRY_BACKOFF_MAX_MS, SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC, Long.class);
            addArgument(SASL_LOGIN_RETRY_BACKOFF_MS, SASL_LOGIN_RETRY_BACKOFF_MS_DOC, Long.class);
            addArgument(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC, Integer.class);
            addArgument(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC)
                .action(Arguments.append());
            addArgument(SASL_OAUTHBEARER_EXPECTED_ISSUER, SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC);
            addArgument(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC, Long.class);
            addArgument(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC, Long.class);
            addArgument(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC, Long.class);
            addArgument(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC);
            addArgument(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC);
            addArgument(SASL_OAUTHBEARER_SUB_CLAIM_NAME, SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC);
            addArgument(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC);
            addArgument(SASL_OAUTHBEARER_SCOPE, SASL_OAUTHBEARER_SCOPE_DOC);
            addArgument(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC);
            addArgument(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC);

            // SSL
            addArgument(SSL_CIPHER_SUITES_CONFIG, SSL_CIPHER_SUITES_DOC)
                .action(Arguments.append());
            addArgument(SSL_ENABLED_PROTOCOLS_CONFIG, SSL_ENABLED_PROTOCOLS_DOC)
                .action(Arguments.append());
            addArgument(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC);
            addArgument(SSL_ENGINE_FACTORY_CLASS_CONFIG, SSL_ENGINE_FACTORY_CLASS_DOC);
            addArgument(SSL_KEYMANAGER_ALGORITHM_CONFIG, SSL_KEYMANAGER_ALGORITHM_DOC);
            addArgument(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC);
            addArgument(SSL_KEYSTORE_KEY_CONFIG, SSL_KEYSTORE_KEY_DOC);
            addArgument(SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_LOCATION_DOC);
            addArgument(SSL_KEYSTORE_PASSWORD_CONFIG, SSL_KEYSTORE_PASSWORD_DOC);
            addArgument(SSL_KEYSTORE_TYPE_CONFIG, SSL_KEYSTORE_TYPE_DOC);
            addArgument(SSL_KEY_PASSWORD_CONFIG, SSL_KEY_PASSWORD_DOC);
            addArgument(SSL_PROTOCOL_CONFIG, SSL_PROTOCOL_DOC);
            addArgument(SSL_PROVIDER_CONFIG, SSL_PROVIDER_DOC);
            addArgument(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, SSL_SECURE_RANDOM_IMPLEMENTATION_DOC);
            addArgument(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, SSL_TRUSTMANAGER_ALGORITHM_DOC);
            addArgument(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, SSL_TRUSTSTORE_CERTIFICATES_DOC);
            addArgument(SSL_TRUSTSTORE_LOCATION_CONFIG, SSL_TRUSTSTORE_LOCATION_DOC);
            addArgument(SSL_TRUSTSTORE_PASSWORD_CONFIG, SSL_TRUSTSTORE_PASSWORD_DOC);
            addArgument(SSL_TRUSTSTORE_TYPE_CONFIG, SSL_TRUSTSTORE_TYPE_DOC);

            // JAAS options...
            parser.addArgument("--" + CLIENT_ID_ARG)
                    .metavar(CLIENT_ID_ARG)
                    .dest(CLIENT_ID_CONFIG)
                    .help(CLIENT_ID_DOC);
            parser.addArgument("--" + CLIENT_SECRET_ARG)
                    .metavar(CLIENT_SECRET_ARG)
                    .dest(CLIENT_SECRET_CONFIG)
                    .help(CLIENT_SECRET_DOC);
            addArgument(SCOPE_CONFIG, SCOPE_DOC);
            addArgument(SASL_JAAS_CONFIG, SASL_JAAS_CONFIG_DOC);

            try {
                return parser.parseArgs(args);
            } catch (ArgumentParserException e) {
                parser.handleError(e);
                throw e;
            }
        }

        private Argument addArgument(String option, String help) {
            return addArgument(option, help, String.class);
        }

        private Argument addArgument(String option, String help, Class<?> clazz) {
            String name = "--" + option;

            return parser.addArgument(name)
                .type(clazz)
                .metavar(option)
                .dest(option)
                .help(help);
        }

    }

    static class ConfigHandler {
        private final Namespace namespace;
        private final Properties fileProps;

        private final Map<String, BiConsumer<Map<String, Object>, String>> saslAdders = Map.ofEntries(
                Map.entry(SASL_LOGIN_CONNECT_TIMEOUT_MS, this::maybeAddInt),
                Map.entry(SASL_LOGIN_READ_TIMEOUT_MS, this::maybeAddInt),
                Map.entry(SASL_LOGIN_RETRY_BACKOFF_MS, this::maybeAddLong),
                Map.entry(SASL_LOGIN_RETRY_BACKOFF_MAX_MS, this::maybeAddLong),
                Map.entry(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_SUB_CLAIM_NAME, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS, this::maybeAddLong),
                Map.entry(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS, this::maybeAddLong),
                Map.entry(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS, this::maybeAddLong),
                Map.entry(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, this::maybeAddInt),
                Map.entry(SASL_OAUTHBEARER_EXPECTED_AUDIENCE, this::maybeAddStringList),
                Map.entry(SASL_OAUTHBEARER_EXPECTED_ISSUER, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_SCOPE, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID, this::maybeAddString),
                Map.entry(SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET, this::maybeAddPassword)
        );
        private final Map<String, BiConsumer<Map<String, Object>, String>> jaasOptionAdders = Map.ofEntries(
                // SASL/OAuth
                Map.entry(CLIENT_ID_CONFIG, this::maybeAddStringCliOnly),
                Map.entry(CLIENT_SECRET_CONFIG, this::maybeAddStringCliOnly),
                Map.entry(SCOPE_CONFIG, this::maybeAddString),
                Map.entry(SASL_JAAS_CONFIG, this::maybeAddJaasConfig),
                // SSL
                Map.entry(SSL_CIPHER_SUITES_CONFIG, this::maybeAddStringList),
                Map.entry(SSL_ENABLED_PROTOCOLS_CONFIG, this::maybeAddStringList),
                Map.entry(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, this::maybeAddString),
                Map.entry(SSL_ENGINE_FACTORY_CLASS_CONFIG, this::maybeAddClass),
                Map.entry(SSL_KEYMANAGER_ALGORITHM_CONFIG, this::maybeAddString),
                Map.entry(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, this::maybeAddPassword),
                Map.entry(SSL_KEYSTORE_KEY_CONFIG, this::maybeAddPassword),
                Map.entry(SSL_KEYSTORE_LOCATION_CONFIG, this::maybeAddString),
                Map.entry(SSL_KEYSTORE_PASSWORD_CONFIG, this::maybeAddPassword),
                Map.entry(SSL_KEYSTORE_TYPE_CONFIG, this::maybeAddString),
                Map.entry(SSL_KEY_PASSWORD_CONFIG, this::maybeAddPassword),
                Map.entry(SSL_PROTOCOL_CONFIG, this::maybeAddString),
                Map.entry(SSL_PROVIDER_CONFIG, this::maybeAddString),
                Map.entry(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, this::maybeAddString),
                Map.entry(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, this::maybeAddString),
                Map.entry(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, this::maybeAddPassword),
                Map.entry(SSL_TRUSTSTORE_LOCATION_CONFIG, this::maybeAddString),
                Map.entry(SSL_TRUSTSTORE_PASSWORD_CONFIG, this::maybeAddPassword),
                Map.entry(SSL_TRUSTSTORE_TYPE_CONFIG, this::maybeAddString)
        );

        public ConfigHandler(Namespace namespace, Properties fileProps) {
            this.namespace = namespace;
            this.fileProps = fileProps;
        }

        Map<String, ?> getConfigs() {
            Map<String, Object> m = new HashMap<>();

            for (Map.Entry<String, BiConsumer<Map<String, Object>, String>> entry : saslAdders.entrySet())
                entry.getValue().accept(m, entry.getKey());

            ConfigDef cd = new ConfigDef();
            SaslConfigs.addClientSaslSupport(cd);
            SslConfigs.addClientSslSupport(cd);

            return new AbstractConfig(cd, m).values();
        }

        Map<String, Object> getJaasOptions() {
            Map<String, Object> m = new HashMap<>();

            for (Map.Entry<String, BiConsumer<Map<String, Object>, String>> entry : jaasOptionAdders.entrySet())
                entry.getValue().accept(m, entry.getKey());

            return m;
        }

        private Optional<String> resolve(String key) {
            String value = namespace.getString(key);
            if (value == null)
                value = fileProps.getProperty(key);
            return Optional.ofNullable(value);
        }

        private void maybeAddInt(Map<String, Object> m, String key) {
            resolve(key).map(Integer::parseInt).ifPresent(v -> m.put(key, v));
        }

        private void maybeAddLong(Map<String, Object> m, String key) {
            resolve(key).map(Long::parseLong).ifPresent(v -> m.put(key, v));
        }

        private void maybeAddString(Map<String, Object> m, String key) {
            resolve(key).ifPresent(v -> m.put(key, v));
        }

        private void maybeAddPassword(Map<String, Object> m, String key) {
            resolve(key).map(Password::new).ifPresent(v -> m.put(key, v));
        }

        private void maybeAddStringCliOnly(Map<String, Object> m, String option) {
            Optional.ofNullable(namespace.getString(option)).ifPresent(v -> m.put(option, v));
        }

        private void maybeAddJaasConfig(Map<String, Object> m, String option) {
            String str = fileProps.getProperty(option);

            if (str == null)
                return;

            Pattern pattern = Pattern.compile("(\\w+)=\"([^\"]*)\"");
            Matcher matcher = pattern.matcher(str);

            while (matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2);

                if ((key.equals(CLIENT_ID_CONFIG) ||
                        key.equals(CLIENT_SECRET_CONFIG) ||
                        key.equals(SCOPE_CONFIG)) && !m.containsKey(key)) {
                    m.put(key, value);
                }
            }
        }

        private void maybeAddClass(Map<String, Object> m, String option) {
            resolve(option).ifPresent(v -> {
                try {
                    m.put(option, Class.forName(v));
                } catch (ClassNotFoundException e) {
                    throw new KafkaException("Could not find class for " + option, e);
                }
            });
        }

        private void maybeAddStringList(Map<String, Object> m, String option) {
            List<String> value = namespace.getList(option);

            if (value == null) {
                String str = fileProps.getProperty(option);
                if (str != null)
                    value = List.of(str.split("\\s*,\\s*"));
            }

            if (value != null)
                m.put(option, value);
        }
    }

}
