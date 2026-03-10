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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionSupplierFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.CloseableSupplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_ASSERTION_FILE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;

/**
 * Factory class for creating {@link HttpRequestFormatter} instances based on the OAuth authentication
 * method configured.
 *
 * <p>
 * This factory implements a three-tier fallback mechanism: file-based assertion (first), locally-generated
 * assertion (second), or client secret (third/fallback). When multiple authentication methods are configured,
 * the first preference takes precedence and other configurations are ignored with a WARN log message.
 * </p>
 *
 * <p>
 * The factory handles reading configuration values from both the main configuration and JAAS options
 * (for backward compatibility) using {@link ConfigOrJaas}.
 * </p>
 */
public class ClientCredentialsRequestFormatterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCredentialsRequestFormatterFactory.class);

    /**
     * Creates an appropriate {@link HttpRequestFormatter} based on the provided configuration using a
     * three-tier fallback mechanism:
     * <ol>
     *   <li>File-based assertion ({@code sasl.oauthbearer.assertion.file})</li>
     *   <li>Locally-generated assertion ({@code sasl.oauthbearer.assertion.claim.iss})</li>
     *   <li>Client secret (fallback)</li>
     * </ol>
     *
     * <p>
     * The method logs INFO/WARN messages to indicate which authentication method was selected and whether
     * any configurations were ignored due to precedence rules.
     * </p>
     *
     * @param cu  The configuration utilities containing OAuth configuration values
     * @param jou The JAAS options utilities for reading deprecated JAAS-based configuration
     * @return An {@link HttpRequestFormatter} appropriate for the configured authentication method
     * @throws org.apache.kafka.common.config.ConfigException if required configuration is missing or invalid
     */
    public static HttpRequestFormatter create(ConfigurationUtils cu, JaasOptionsUtils jou) {
        return create(cu, jou, Time.SYSTEM);
    }

    public static HttpRequestFormatter create(ConfigurationUtils cu, JaasOptionsUtils jou, Time time) {
        ConfigOrJaas configOrJaas = new ConfigOrJaas(cu, jou);
        String scope = configOrJaas.scope();
        boolean urlEncodeHeader = validateUrlEncodeHeader(cu);

        String assertionFile = cu.validateString(SASL_OAUTHBEARER_ASSERTION_FILE, false);
        if (cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS) || assertionFile != null) {
            // Check for config conflicts and warn if both file and local generation configs are present
            if (assertionFile != null && cu.containsKey(SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS)) {
                LOG.warn("Both {} and {} are configured. Using file-based assertion (first preference); " +
                         "locally-generated assertion configs will be ignored.",
                    SASL_OAUTHBEARER_ASSERTION_FILE, SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS);
            }

            // Log which assertion path is being used
            if (assertionFile != null) {
                LOG.info("Using client assertion authentication with file-based assertion (first preference)");
            } else {
                LOG.info("Using client assertion authentication with dynamically-generated assertion (second preference)");
            }

            CloseableSupplier<String> assertionSupplier = AssertionSupplierFactory.create(cu, time);
            try {
                return new ClientAssertionRequestFormatter(configOrJaas.clientId(false), scope, assertionSupplier);
            } catch (Exception e) {
                Utils.closeQuietly(assertionSupplier, "assertion supplier");
                throw e;
            }
        } else {
            LOG.info("Using client secret authentication (third preference/fallback)");
            return new ClientSecretRequestFormatter(configOrJaas.clientId(), configOrJaas.clientSecret(), scope, urlEncodeHeader);
        }
    }

    /**
    * In some cases, the incoming {@link Map} doesn't contain a value for
    * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
    * will cause a {@link NullPointerException} when it is later unboxed.
    * <p>
    * This utility method ensures that we have a non-{@code null} value to use in the
    * {@link ClientSecretRequestFormatter} constructor.
    */
    public static boolean validateUrlEncodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlEncodeHeader = configurationUtils.get(SASL_OAUTHBEARER_HEADER_URLENCODE);
        return Objects.requireNonNullElse(urlEncodeHeader, DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE);
    }
}
