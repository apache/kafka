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
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_ID_DOC;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_DOC;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler.SCOPE_DOC;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenRetrieverFactory;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.secured.AccessTokenValidatorFactory;
import org.apache.kafka.common.security.oauthbearer.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.secured.VerificationKeyResolverFactory;
import org.apache.kafka.common.utils.Exit;

public class OAuthCompatibilityTool {

    public static void main(String[] args) {
        String description = String.format(
            "This tool is used to verify OAuth/OIDC provider compatibility.%n%n" +
            "To use, first export KAFKA_OPTS with Java system properties that match%n" +
            "your OAuth/OIDC configuration. Next, run the following script to%n" +
            "execute the test:%n%n" +
            "    ./bin/kafka-run-class.sh %s" +
            "%n%n" +
            "Please refer to the following source files for OAuth/OIDC client and%n" +
            "broker configuration options:" +
            "%n%n" +
            "    %s%n" +
            "    %s",
            OAuthCompatibilityTool.class.getName(),
            SaslConfigs.class.getName(),
            OAuthBearerLoginCallbackHandler.class.getName());

        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("oauth-compatibility-test")
            .defaultHelp(true)
            .description(description);

        parser.addArgument("--connect-timeout-ms")
            .type(Integer.class)
            .dest("connectTimeoutMs")
            .help(SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC);
        parser.addArgument("--read-timeout-ms")
            .type(Integer.class)
            .dest("readTimeoutMs")
            .help(SASL_LOGIN_READ_TIMEOUT_MS_DOC);
        parser.addArgument("--login-retry-backoff-ms")
            .type(Long.class)
            .dest("loginRetryBackoffMs")
            .help(SASL_LOGIN_RETRY_BACKOFF_MS_DOC);
        parser.addArgument("--login-retry-backoff-max-ms")
            .type(Long.class)
            .dest("loginRetryBackoffMax")
            .help(SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC);
        parser.addArgument("--scope-claim-name")
            .dest("scopeClaimName")
            .help(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC);
        parser.addArgument("--sub-claim-name")
            .dest("subClaimName")
            .help(SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC);
        parser.addArgument("--token-endpoint-url")
            .dest("tokenEndpointUrl")
            .help(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC);
        parser.addArgument("--jwks-endpoint-url")
            .dest("jwksEndpointUrl")
            .help(SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC);
        parser.addArgument("--jwks-endpoint-refresh-ms")
            .type(Long.class)
            .dest("jwksEndpointRefreshMs")
            .help(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC);
        parser.addArgument("--jwks-endpoint-retry-backoff-max-ms")
            .type(Long.class)
            .dest("jwksEndpointRetryBackoffMaxMs")
            .help(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC);
        parser.addArgument("--jwks-endpoint-retry-backoff-ms")
            .type(Long.class)
            .dest("jwksEndpointRetryBackoffMs")
            .help(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC);
        parser.addArgument("--clock-skew-seconds")
            .type(Integer.class)
            .dest("clockSkewSeconds")
            .help(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC);
        parser.addArgument("--expected-audience")
            .dest("expectedAudience")
            .help(SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC);
        parser.addArgument("--expected-issuer")
            .dest("expectedIssuer")
            .help(SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC);

        parser.addArgument("--client-id")
            .dest("clientId")
            .help(CLIENT_ID_DOC);
        parser.addArgument("--client-secret")
            .dest("clientSecret")
            .help(CLIENT_SECRET_DOC);
        parser.addArgument("--scope")
            .dest("scope")
            .help(SCOPE_DOC);

        Namespace namespace;

        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
            return;
        }

        Map<String, ?> configs = getConfigs(namespace);
        Map<String, Object> jaasConfigs = getJaasConfigs(namespace);

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
                parser.printHelp();
            }

            Exit.exit(1);
        }
    }

    private static Map<String, ?> getConfigs(Namespace namespace) {
        Map<String, Object> c = new HashMap<>();
        maybeAddInt(namespace, "connectTimeoutMs", c, SASL_LOGIN_CONNECT_TIMEOUT_MS);
        maybeAddInt(namespace, "readTimeoutMs", c, SASL_LOGIN_READ_TIMEOUT_MS);
        maybeAddLong(namespace, "loginRetryBackoffMs", c, SASL_LOGIN_RETRY_BACKOFF_MS);
        maybeAddLong(namespace, "loginRetryBackoffMax", c, SASL_LOGIN_RETRY_BACKOFF_MAX_MS);
        maybeAddString(namespace, "scopeClaimName", c, SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        maybeAddString(namespace, "subClaimName", c, SASL_OAUTHBEARER_SUB_CLAIM_NAME);
        maybeAddString(namespace, "tokenEndpointUrl", c, SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);
        maybeAddString(namespace, "jwksEndpointUrl", c, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);
        maybeAddLong(namespace, "jwksEndpdointRefreshMs", c, SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS);
        maybeAddLong(namespace, "jwksEndpdointRetryBackoffMaxMs", c, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS);
        maybeAddLong(namespace, "jwksEndpdointRetryBackoffMs", c, SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS);
        maybeAddInt(namespace, "clockSkewSeconds", c, SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS);
        maybeAddStringList(namespace, "expectedAudience", c, SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
        maybeAddString(namespace, "expectedIssuer", c, SASL_OAUTHBEARER_EXPECTED_ISSUER);

        // This here is going to fill in all the defaults for the values we don't specify...
        ConfigDef cd = new ConfigDef();
        SaslConfigs.addClientSaslSupport(cd);
        AbstractConfig config = new AbstractConfig(cd, c);
        return config.values();
    }

    private static void maybeAddInt(Namespace namespace, String namespaceKey, Map<String, Object> configs, String configsKey) {
        Integer value = namespace.getInt(namespaceKey);

        if (value != null)
            configs.put(configsKey, value);
    }

    private static void maybeAddLong(Namespace namespace, String namespaceKey, Map<String, Object> configs, String configsKey) {
        Long value = namespace.getLong(namespaceKey);

        if (value != null)
            configs.put(configsKey, value);
    }

    private static void maybeAddString(Namespace namespace, String namespaceKey, Map<String, Object> configs, String configsKey) {
        String value = namespace.getString(namespaceKey);

        if (value != null)
            configs.put(configsKey, value);
    }

    private static void maybeAddStringList(Namespace namespace, String namespaceKey, Map<String, Object> configs, String configsKey) {
        String value = namespace.getString(namespaceKey);

        if (value != null)
            configs.put(configsKey, Arrays.asList(value.split(",")));
    }

    private static Map<String, Object> getJaasConfigs(Namespace namespace) {
        Map<String, Object> c = new HashMap<>();
        c.put(CLIENT_ID_CONFIG, namespace.getString("clientId"));
        c.put(CLIENT_SECRET_CONFIG, namespace.getString("clientSecret"));
        c.put(SCOPE_CONFIG, namespace.getString("scope"));
        return c;
    }

}
