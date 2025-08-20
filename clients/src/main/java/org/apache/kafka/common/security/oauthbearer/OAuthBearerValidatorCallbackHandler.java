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

package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils.getConfiguredInstance;

/**
 * <p>
 * <code>OAuthBearerValidatorCallbackHandler</code> is an {@link AuthenticateCallbackHandler} that
 * accepts {@link OAuthBearerValidatorCallback} and {@link OAuthBearerExtensionsValidatorCallback}
 * callbacks to implement OAuth/OIDC validation. This callback handler is intended only to be used
 * on the Kafka broker side as it will receive a {@link OAuthBearerValidatorCallback} that includes
 * the JWT provided by the Kafka client. That JWT is validated in terms of format, expiration,
 * signature, and audience and issuer (if desired). This callback handler is the broker side of the
 * OAuth functionality, whereas {@link OAuthBearerLoginCallbackHandler} is used by clients.
 * </p>
 *
 * <p>
 * This {@link AuthenticateCallbackHandler} is enabled in the broker configuration by setting the
 * {@link org.apache.kafka.common.config.internals.BrokerSecurityConfigs#SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG}
 * like so:
 *
 * <code>
 * listener.name.<listener name>.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
 * </code>
 * </p>
 *
 * <p>
 * The JAAS configuration for OAuth is also needed. If using OAuth for inter-broker communication,
 * the options are those specified in {@link OAuthBearerLoginCallbackHandler}.
 * </p>
 *
 * <p>
 * The configuration option
 * {@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_JWKS_ENDPOINT_URL}
 * is also required in order to contact the OAuth/OIDC provider to retrieve the JWKS for use in
 * JWT signature validation. For example:
 *
 * <code>
 * listener.name.<listener name>.oauthbearer.sasl.oauthbearer.jwks.endpoint.url=https://example.com/oauth2/v1/keys
 * </code>
 *
 * Please see the OAuth/OIDC providers documentation for the JWKS endpoint URL.
 * </p>
 *
 * <p>
 * The following is a list of all the configuration options that are available for the broker
 * validation callback handler:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.common.config.internals.BrokerSecurityConfigs#SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_JAAS_CONFIG}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_EXPECTED_AUDIENCE}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_EXPECTED_ISSUER}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_JWKS_ENDPOINT_URL}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_SCOPE_CLAIM_NAME}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_SUB_CLAIM_NAME}</li>
 * </ul>
 * </p>
 */

public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler.class);

    private CloseableVerificationKeyResolver verificationKeyResolver;

    private JwtValidator jwtValidator;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        jwtValidator = getConfiguredInstance(
            configs,
            saslMechanism,
            jaasConfigEntries,
            SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
            JwtValidator.class
        );
    }

    /*
     * Package-visible for testing.
     */
    void configure(Map<String, ?> configs,
                   String saslMechanism,
                   List<AppConfigurationEntry> jaasConfigEntries,
                   CloseableVerificationKeyResolver verificationKeyResolver,
                   JwtValidator jwtValidator) {
        this.verificationKeyResolver = verificationKeyResolver;
        this.verificationKeyResolver.configure(configs, saslMechanism, jaasConfigEntries);

        this.jwtValidator = jwtValidator;
        this.jwtValidator.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public void close() {
        Utils.closeQuietly(jwtValidator, "JWT validator");
        Utils.closeQuietly(verificationKeyResolver, "JWT verification key resolver");
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        checkConfigured();

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                handleValidatorCallback((OAuthBearerValidatorCallback) callback);
            } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                handleExtensionsValidatorCallback((OAuthBearerExtensionsValidatorCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleValidatorCallback(OAuthBearerValidatorCallback callback) {
        checkConfigured();

        OAuthBearerToken token;

        try {
            token = jwtValidator.validate(callback.tokenValue());
            callback.token(token);
        } catch (JwtValidatorException e) {
            log.warn(e.getMessage(), e);
            callback.error("invalid_token", null, null);
        }
    }

    private void handleExtensionsValidatorCallback(OAuthBearerExtensionsValidatorCallback extensionsValidatorCallback) {
        checkConfigured();

        extensionsValidatorCallback.inputExtensions().map().forEach((extensionName, v) -> extensionsValidatorCallback.valid(extensionName));
    }

    private void checkConfigured() {
        if (jwtValidator == null)
            throw new IllegalStateException(String.format("To use %s, first call the configure method", getClass().getSimpleName()));
    }
}
