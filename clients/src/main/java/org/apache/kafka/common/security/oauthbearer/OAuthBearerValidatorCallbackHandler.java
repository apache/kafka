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

import java.io.IOException;
import java.security.Key;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwksVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * {@link org.apache.kafka.common.config.internals.BrokerSecurityConfigs#SASL_SERVER_CALLBACK_HANDLER_CLASS}
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
 *   <li>{@link org.apache.kafka.common.config.internals.BrokerSecurityConfigs#SASL_SERVER_CALLBACK_HANDLER_CLASS}</li>
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

    /**
     * Because a {@link CloseableVerificationKeyResolver} instance can spawn threads and issue
     * HTTP(S) calls ({@link RefreshingHttpsJwksVerificationKeyResolver}), we only want to create
     * a new instance for each particular set of configuration. Because each set of configuration
     * may have multiple instances, we want to reuse the single instance.
     */

    private static final Map<VerificationKeyResolverKey, CloseableVerificationKeyResolver> VERIFICATION_KEY_RESOLVER_CACHE = new HashMap<>();

    private CloseableVerificationKeyResolver verificationKeyResolver;

    private AccessTokenValidator accessTokenValidator;

    private boolean isInitialized = false;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Map<String, Object> moduleOptions = JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries);
        CloseableVerificationKeyResolver verificationKeyResolver;

        // Here's the logic which keeps our VerificationKeyResolvers down to a single instance.
        synchronized (VERIFICATION_KEY_RESOLVER_CACHE) {
            VerificationKeyResolverKey key = new VerificationKeyResolverKey(configs, moduleOptions);
            verificationKeyResolver = VERIFICATION_KEY_RESOLVER_CACHE.computeIfAbsent(key, k ->
                new RefCountingVerificationKeyResolver(VerificationKeyResolverFactory.create(configs, saslMechanism, moduleOptions)));
        }

        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(configs, saslMechanism, verificationKeyResolver);
        init(verificationKeyResolver, accessTokenValidator);
    }

    public void init(CloseableVerificationKeyResolver verificationKeyResolver, AccessTokenValidator accessTokenValidator) {
        this.verificationKeyResolver = verificationKeyResolver;
        this.accessTokenValidator = accessTokenValidator;

        try {
            verificationKeyResolver.init();
        } catch (Exception e) {
            throw new KafkaException("The OAuth validator configuration encountered an error when initializing the VerificationKeyResolver", e);
        }

        isInitialized = true;
    }

    @Override
    public void close() {
        if (verificationKeyResolver != null) {
            try {
                verificationKeyResolver.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        checkInitialized();

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
        checkInitialized();

        OAuthBearerToken token;

        try {
            token = accessTokenValidator.validate(callback.tokenValue());
            callback.token(token);
        } catch (ValidateException e) {
            log.warn(e.getMessage(), e);
            callback.error("invalid_token", null, null);
        }
    }

    private void handleExtensionsValidatorCallback(OAuthBearerExtensionsValidatorCallback extensionsValidatorCallback) {
        checkInitialized();

        extensionsValidatorCallback.inputExtensions().map().forEach((extensionName, v) -> extensionsValidatorCallback.valid(extensionName));
    }

    private void checkInitialized() {
        if (!isInitialized)
            throw new IllegalStateException(String.format("To use %s, first call the configure or init method", getClass().getSimpleName()));
    }

    /**
     * <code>VkrKey</code> is a simple structure which encapsulates the criteria for different
     * sets of configuration. This will allow us to use this object as a key in a {@link Map}
     * to keep a single instance per key.
     */

    private static class VerificationKeyResolverKey {

        private final Map<String, ?> configs;

        private final Map<String, Object> moduleOptions;

        public VerificationKeyResolverKey(Map<String, ?> configs, Map<String, Object> moduleOptions) {
            this.configs = configs;
            this.moduleOptions = moduleOptions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            VerificationKeyResolverKey that = (VerificationKeyResolverKey) o;
            return configs.equals(that.configs) && moduleOptions.equals(that.moduleOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(configs, moduleOptions);
        }

    }

    /**
     * <code>RefCountingVerificationKeyResolver</code> allows us to share a single
     * {@link CloseableVerificationKeyResolver} instance between multiple
     * {@link AuthenticateCallbackHandler} instances and perform the lifecycle methods the
     * appropriate number of times.
     */

    private static class RefCountingVerificationKeyResolver implements CloseableVerificationKeyResolver {

        private final CloseableVerificationKeyResolver delegate;

        private final AtomicInteger count = new AtomicInteger(0);

        public RefCountingVerificationKeyResolver(CloseableVerificationKeyResolver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
            return delegate.resolveKey(jws, nestingContext);
        }

        @Override
        public void init() throws IOException {
            if (count.incrementAndGet() == 1)
                delegate.init();
        }

        @Override
        public void close() throws IOException {
            if (count.decrementAndGet() == 0)
                delegate.close();
        }

    }

}
