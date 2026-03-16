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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler.SCOPE_CONFIG;

/**
 * Utility class that retrieves OAuth configuration values from either modern configuration properties
 * or legacy JAAS options, providing a unified interface with proper deprecation handling.
 *
 * <p>
 * Kafka historically configured OAuth parameters (client ID, client secret, scope) via JAAS options.
 * These have been deprecated in favor of dedicated configuration properties. This class:
 * <ul>
 *   <li>Checks modern configuration first</li>
 *   <li>Falls back to JAAS options if not found (with deprecation warnings)</li>
 *   <li>Throws {@link ConfigException} if required values are missing from both sources</li>
 *   <li>Logs warnings when both are present (modern config takes precedence)</li>
 * </ul>
 * </p>
 *
 * <p>
 * This approach maintains backward compatibility while guiding users toward the recommended
 * configuration method.
 * </p>
 *
 * @see ConfigurationUtils
 * @see JaasOptionsUtils
 */
class ConfigOrJaas {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigOrJaas.class);

    private final ConfigurationUtils cu;
    private final JaasOptionsUtils jou;

    /**
     * Creates a new {@code ConfigOrJaas} instance.
     *
     * @param cu  Utilities for accessing configuration properties
     * @param jou Utilities for accessing JAAS options
     */
    ConfigOrJaas(ConfigurationUtils cu, JaasOptionsUtils jou) {
        this.cu = cu;
        this.jou = jou;
    }

    /**
     * Retrieves the OAuth client ID from configuration or JAAS options.
     *
     * <p>
     * This is a convenience method that requires the client ID to be present. If the client ID
     * might be optional (e.g., when using client assertions that contain the client ID),
     * use {@link #clientId(boolean)} instead.
     * </p>
     *
     * @return The client ID
     * @throws ConfigException if the client ID is not found in either configuration or JAAS options
     */
    public String clientId() {
        return clientId(true);
    }

    /**
     * Retrieves the OAuth client ID from configuration or JAAS options.
     *
     * @param isRequired If {@code true}, throws {@link ConfigException} when the client ID is not found;
     *                   if {@code false}, returns {@code null} when not found
     * @return The client ID, or {@code null} if not found and not required
     * @throws ConfigException if {@code isRequired} is {@code true} and the client ID is not found
     */
    public String clientId(boolean isRequired) {
        return getValue(
                SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID,
                CLIENT_ID_CONFIG,
                isRequired,
                name -> cu.validateString(name, isRequired),
                name -> jou.validateString(name, isRequired)
        );
    }

    /**
     * Retrieves the OAuth client secret from configuration or JAAS options.
     *
     * <p>
     * The client secret is always required when using client secret authentication.
     * When using configuration properties, the value is retrieved as a Password type for security.
     * </p>
     *
     * @return The client secret
     * @throws ConfigException if the client secret is not found in either configuration or JAAS options
     */
    public String clientSecret() {
        return getValue(
                SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET,
                CLIENT_SECRET_CONFIG,
                true,
                cu::validatePassword,
                jou::validateString
        );
    }

    /**
     * Retrieves the OAuth scope from configuration or JAAS options.
     *
     * <p>
     * The scope parameter is optional in OAuth. If not specified, the OAuth provider will
     * typically grant default scopes associated with the client.
     * </p>
     *
     * @return The OAuth scope string, or {@code null} if not configured
     */
    public String scope() {
        return getValue(
                SASL_OAUTHBEARER_SCOPE,
                SCOPE_CONFIG,
                false,
                name -> cu.validateString(name, false),
                name -> jou.validateString(name, false)
        );
    }

    private String getValue(String configName,
                            String jaasName,
                            boolean isRequired,
                            Function<String, String> configValueGetter,
                            Function<String, String> jaasValueGetter) {
        boolean isPresentInConfig = cu.containsKey(configName);
        boolean isPresentInJaas = jou.containsKey(jaasName);

        if (isPresentInConfig) {
            if (isPresentInJaas) {
                // Log if the user is using the deprecated JAAS option.
                LOG.warn(
                        "Both the OAuth configuration {} as well as the JAAS option {} (from the {} configuration) were provided. " +
                                "Since the {} JAAS option is deprecated, it will be ignored and the value from the {} configuration will be used. " +
                                "Please update your configuration to only use {}.",
                        configName,
                        jaasName,
                        SASL_JAAS_CONFIG,
                        jaasName,
                        configName,
                        configName
                );
            }

            return configValueGetter.apply(configName);
        } else if (isPresentInJaas) {
            String value = jaasValueGetter.apply(jaasName);

            // Log if the user is using the deprecated JAAS option.
            LOG.warn(
                    "The OAuth JAAS option {} was configured in {}, but that JAAS option is deprecated and will be removed. " +
                            "Please update your configuration to use the {} configuration instead.",
                    jaasName,
                    SASL_JAAS_CONFIG,
                    configName
            );

            return value;
        } else if (isRequired) {
            throw new ConfigException(configName, null);
        } else {
            return null;
        }
    }
}
