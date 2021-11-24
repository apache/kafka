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

import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS_DOC;
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
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC;
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
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_ID_DOC;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_DOC;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.SCOPE_DOC;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetrieverFactory;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidatorFactory;
import org.apache.kafka.common.security.oauthbearer.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.secured.VerificationKeyResolverFactory;
import org.apache.kafka.common.utils.Exit;

public class OAuthCompatibilityTool {

    public static void main(String[] args) {
        ArgsHandler argsHandler = new ArgsHandler();
        Namespace namespace;

        try {
            namespace = argsHandler.parseArgs(args);
        } catch (ArgumentParserException e) {
            Exit.exit(1);
            return;
        }

        ConfigHandler configHandler = new ConfigHandler(namespace);

        Map<String, ?> configs = configHandler.getConfigs();
        Map<String, Object> jaasConfigs = configHandler.getJaasOptions();

        try {
            String accessToken;

            {
                // Client side...
                try (AccessTokenRetriever atr = AccessTokenRetrieverFactory.create(configs, jaasConfigs)) {
                    atr.init();
                    AccessTokenValidator atv = AccessTokenValidatorFactory.create(configs);
                    System.out.println("PASSED 1/5: client configuration");

                    accessToken = atr.retrieve();
                    System.out.println("PASSED 2/5: client JWT retrieval");

                    atv.validate(accessToken);
                    System.out.println("PASSED 3/5: client JWT validation");
                }
            }

            {
                // Broker side...
                try (CloseableVerificationKeyResolver vkr = VerificationKeyResolverFactory.create(configs, jaasConfigs)) {
                    vkr.init();
                    AccessTokenValidator atv = AccessTokenValidatorFactory.create(configs, vkr);
                    System.out.println("PASSED 4/5: broker configuration");

                    atv.validate(accessToken);
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


    private static class ArgsHandler {

        private static final String DESCRIPTION = String.format(
            "This tool is used to verify OAuth/OIDC provider compatibility.%n%n" +
            "Run the following script to determine the configuration options:%n%n" +
                "    ./bin/kafka-run-class.sh %s --help",
            OAuthCompatibilityTool.class.getName());

        private final ArgumentParser parser;

        private ArgsHandler() {
            this.parser = ArgumentParsers
                .newArgumentParser("oauth-compatibility-tool")
                .defaultHelp(true)
                .description(DESCRIPTION);
        }

        private Namespace parseArgs(String[] args) throws ArgumentParserException {
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
            addArgument(CLIENT_ID_CONFIG, CLIENT_ID_DOC);
            addArgument(CLIENT_SECRET_CONFIG, CLIENT_SECRET_DOC);
            addArgument(SCOPE_CONFIG, SCOPE_DOC);

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
            // Change foo.bar into --foo.bar.
            String name = "--" + option;

            return parser.addArgument(name)
                .type(clazz)
                .metavar(option)
                .dest(option)
                .help(help);
        }

    }

    private static class ConfigHandler {

        private final Namespace namespace;


        private ConfigHandler(Namespace namespace) {
            this.namespace = namespace;
        }

        private Map<String, ?> getConfigs() {
            Map<String, Object> m = new HashMap<>();

            // SASL/OAuth
            maybeAddInt(m, SASL_LOGIN_CONNECT_TIMEOUT_MS);
            maybeAddInt(m, SASL_LOGIN_READ_TIMEOUT_MS);
            maybeAddLong(m, SASL_LOGIN_RETRY_BACKOFF_MS);
            maybeAddLong(m, SASL_LOGIN_RETRY_BACKOFF_MAX_MS);
            maybeAddString(m, SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
            maybeAddString(m, SASL_OAUTHBEARER_SUB_CLAIM_NAME);
            maybeAddString(m, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
            maybeAddString(m, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);
            maybeAddLong(m, SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS);
            maybeAddLong(m, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS);
            maybeAddLong(m, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS);
            maybeAddInt(m, SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS);
            maybeAddStringList(m, SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
            maybeAddString(m, SASL_OAUTHBEARER_EXPECTED_ISSUER);

            // This here is going to fill in all the defaults for the values we don't specify...
            ConfigDef cd = new ConfigDef();
            SaslConfigs.addClientSaslSupport(cd);
            SslConfigs.addClientSslSupport(cd);
            AbstractConfig config = new AbstractConfig(cd, m);
            return config.values();
        }

        private Map<String, Object> getJaasOptions() {
            Map<String, Object> m = new HashMap<>();

            // SASL/OAuth
            maybeAddString(m, CLIENT_ID_CONFIG);
            maybeAddString(m, CLIENT_SECRET_CONFIG);
            maybeAddString(m, SCOPE_CONFIG);

            // SSL
            maybeAddStringList(m, SSL_CIPHER_SUITES_CONFIG);
            maybeAddStringList(m, SSL_ENABLED_PROTOCOLS_CONFIG);
            maybeAddString(m, SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
            maybeAddClass(m, SSL_ENGINE_FACTORY_CLASS_CONFIG);
            maybeAddString(m, SSL_KEYMANAGER_ALGORITHM_CONFIG);
            maybeAddPassword(m, SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG);
            maybeAddPassword(m, SSL_KEYSTORE_KEY_CONFIG);
            maybeAddString(m, SSL_KEYSTORE_LOCATION_CONFIG);
            maybeAddPassword(m, SSL_KEYSTORE_PASSWORD_CONFIG);
            maybeAddString(m, SSL_KEYSTORE_TYPE_CONFIG);
            maybeAddPassword(m, SSL_KEY_PASSWORD_CONFIG);
            maybeAddString(m, SSL_PROTOCOL_CONFIG);
            maybeAddString(m, SSL_PROVIDER_CONFIG);
            maybeAddString(m, SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
            maybeAddString(m, SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
            maybeAddPassword(m, SSL_TRUSTSTORE_CERTIFICATES_CONFIG);
            maybeAddString(m, SSL_TRUSTSTORE_LOCATION_CONFIG);
            maybeAddPassword(m, SSL_TRUSTSTORE_PASSWORD_CONFIG);
            maybeAddString(m, SSL_TRUSTSTORE_TYPE_CONFIG);

            return m;
        }

        private void maybeAddInt(Map<String, Object> m, String option) {
            Integer value = namespace.getInt(option);

            if (value != null)
                m.put(option, value);
        }

        private void maybeAddLong(Map<String, Object> m, String option) {
            Long value = namespace.getLong(option);

            if (value != null)
                m.put(option, value);
        }

        private void maybeAddString(Map<String, Object> m, String option) {
            String value = namespace.getString(option);

            if (value != null)
                m.put(option, value);
        }

        private void maybeAddPassword(Map<String, Object> m, String option) {
            String value = namespace.getString(option);

            if (value != null)
                m.put(option, new Password(value));
        }

        private void maybeAddClass(Map<String, Object> m, String option) {
            String value = namespace.getString(option);

            if (value != null) {
                try {
                    m.put(option, Class.forName(value));
                } catch (ClassNotFoundException e) {
                    throw new KafkaException("Could not find class for " + option, e);
                }
            }
        }

        private void maybeAddStringList(Map<String, Object> m, String option) {
            List<String> value = namespace.getList(option);

            if (value != null)
                m.put(option, value);
        }

    }

}
