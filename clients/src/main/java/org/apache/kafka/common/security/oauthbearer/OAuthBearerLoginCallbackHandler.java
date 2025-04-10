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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.getConfiguredInstanceOrDefault;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.jaasOptions;

/**
 * <p>
 * <code>OAuthBearerLoginCallbackHandler</code> is an {@link AuthenticateCallbackHandler} that
 * accepts {@link OAuthBearerTokenCallback} and {@link SaslExtensionsCallback} callbacks to
 * perform the steps to request a JWT from an OAuth/OIDC provider. The OAuth grant types that
 * are supported include:
 *
 * <ul>
 *     <li>client_credentials</li>
 *     <li>jwt-bearer</li>
 * </ul>
 *
 * These grant types are commonly used for non-interactive "service accounts" where there is
 * no user available to interactively supply credentials.
 * </p>
 *
 * <p>
 * The <code>OAuthBearerLoginCallbackHandler</code> is used on the client side to retrieve a JWT
 * and the {@link OAuthBearerValidatorCallbackHandler} is used on the broker to validate the JWT
 * that was sent to it by the client to allow access. Both the brokers and clients will need to
 * be configured with their appropriate callback handlers and respective configuration for OAuth
 * functionality to work.
 * </p>
 *
 * <p>
 * Note that while this callback handler class must be specified for a Kafka client that wants to
 * use OAuth functionality, in the case of OAuth-based inter-broker communication, the callback
 * handler must be used on the Kafka broker side as well.
 * </p>
 *
 * <p>
 * This {@link AuthenticateCallbackHandler} is enabled by specifying its class name in the Kafka
 * configuration. For client use, specify the class name in the
 * {@link org.apache.kafka.common.config.SaslConfigs#SASL_LOGIN_CALLBACK_HANDLER_CLASS}
 * configuration like so:
 *
 * <code>
 * sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
 * </code>
 * </p>
 *
 * <p>
 * If using OAuth login on the broker side (for inter-broker communication), the callback handler
 * class will be specified with a listener-based property:
 * <code>listener.name.<listener name>.oauthbearer.sasl.login.callback.handler.class</code> like so:
 *
 * <code>
 * listener.name.<listener name>.oauthbearer.sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
 * </code>
 * </p>
 *
 * <p>
 * The Kafka configuration must also include JAAS configuration which includes OAuth-specific options.
 * For <code>client_credentials</code>, use:
 *
 * <ul>
 *     <li><code>clientId</code>OAuth client ID (required)</li>
 *     <li><code>clientSecret</code>OAuth client secret (required)</li>
 *     <li><code>scope</code>OAuth scope (optional)</li>
 * </ul>
 *
 * For the <code>jwt-bearer</code> grant type, use:
 *
 * <ul>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 *     <li><code>XXXXXXXXXXXX</code>XXXXXXXXXXXX</li>
 * </ul>
 * </p>
 *
 * <p>
 * The JAAS configuration can also include any SSL options that are needed. The configuration
 * options are the same as those specified by the configuration in
 * {@link SslConfigs#addClientSslSupport(ConfigDef)}.
 * </p>
 *
 * <p>
 * Here's an example of the JAAS configuration for a Kafka client using the
 * <code>client_credentials</code> grant type:
 *
 * <code>
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
 *   clientId="foo" \
 *   clientSecret="bar" \
 *   scope="baz" \
 *   ssl.protocol="SSL" ;
 * </code>
 * </p>
 *
 * <p>
 * The configuration option {@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}
 * is also required in order for the client to contact the OAuth/OIDC provider. For example:
 *
 * <code>
 * sasl.oauthbearer.token.endpoint.url=https://example.com/oauth2/v1/token
 * </code>
 *
 * Please see the OAuth/OIDC provider's documentation for the token endpoint URL.
 * </p>
 *
 * <p>
 * The following is a list of all the configuration options that are available for the login
 * callback handler:
 *
 * <ul>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_LOGIN_CALLBACK_HANDLER_CLASS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_LOGIN_CONNECT_TIMEOUT_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_LOGIN_READ_TIMEOUT_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_LOGIN_RETRY_BACKOFF_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_LOGIN_RETRY_BACKOFF_MAX_MS}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_JAAS_CONFIG}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_SCOPE_CLAIM_NAME}</li>
 *   <li>{@link org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_SUB_CLAIM_NAME}</li>
 * </ul>
 * </p>
 */
public class OAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler, Closeable {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler.class);

    private static final String EXTENSION_PREFIX = "extension_";

    private Map<String, Object> moduleOptions;

    private JwtValidator jwtValidator;

    private boolean isInitialized = false;

    protected JwtRetriever jwtRetriever;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        moduleOptions = jaasOptions(saslMechanism, jaasConfigEntries);

        this.jwtRetriever = getConfiguredInstanceOrDefault(
            configs,
            saslMechanism,
            jaasConfigEntries,
            SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS,
            JwtRetriever.class
        );

        this.jwtValidator = getConfiguredInstanceOrDefault(
            configs,
            saslMechanism,
            jaasConfigEntries,
            SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
            JwtValidator.class
        );

        this.isInitialized = true;
    }

    void configure(JwtRetriever jwtRetriever,
                   JwtValidator jwtValidator,
                   Map<String, ?> configs,
                   String saslMechanism,
                   List<AppConfigurationEntry> jaasConfigEntries) {
        this.jwtRetriever = jwtRetriever;
        this.jwtValidator = jwtValidator;

        this.jwtRetriever.configure(configs, saslMechanism, jaasConfigEntries);
        this.jwtValidator.configure(configs, saslMechanism, jaasConfigEntries);

        this.isInitialized = true;
    }

    @Override
    public void close() {
        Utils.closeQuietly(jwtRetriever, "jwtRetriever");
        Utils.closeQuietly(jwtValidator, "jwtValidator");
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        checkInitialized();

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handleTokenCallback((OAuthBearerTokenCallback) callback);
            } else if (callback instanceof SaslExtensionsCallback) {
                handleExtensionsCallback((SaslExtensionsCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    protected void handleTokenCallback(OAuthBearerTokenCallback callback) throws IOException {
        checkInitialized();
        String jwt = jwtRetriever.retrieve();

        try {
            OAuthBearerToken token = jwtValidator.validate(jwt);
            callback.token(token);
        } catch (JwtValidatorException e) {
            log.warn(e.getMessage(), e);
            callback.error("invalid_token", e.getMessage(), null);
        }
    }

    protected void handleExtensionsCallback(SaslExtensionsCallback callback) {
        checkInitialized();

        Map<String, String> extensions = new HashMap<>();

        for (Map.Entry<String, Object> configEntry : this.moduleOptions.entrySet()) {
            String key = configEntry.getKey();

            if (!key.startsWith(EXTENSION_PREFIX))
                continue;

            Object valueRaw = configEntry.getValue();
            String value;

            if (valueRaw instanceof String)
                value = (String) valueRaw;
            else
                value = String.valueOf(valueRaw);

            extensions.put(key.substring(EXTENSION_PREFIX.length()), value);
        }

        SaslExtensions saslExtensions = new SaslExtensions(extensions);

        try {
            OAuthBearerClientInitialResponse.validateExtensions(saslExtensions);
        } catch (SaslException e) {
            throw new ConfigException(e.getMessage());
        }

        callback.extensions(saslExtensions);
    }

    protected void checkInitialized() {
        if (!isInitialized)
            throw new IllegalStateException(String.format("To use %s, first call the configure method", getClass().getSimpleName()));
    }
}
