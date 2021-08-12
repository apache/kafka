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

import java.io.IOException;
import java.util.*;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.*;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.security.oauthbearer.internals.secured.httpclient.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler.class);

    private static final String EXTENSION_PREFIX = "Extension_";

    private Map<String, String> moduleOptions;

    private LoginTokenEndpointHttpClient client;

    private AccessTokenValidator accessTokenValidator;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism,
        final List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));

        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null)
            throw new IllegalArgumentException(
                String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                    jaasConfigEntries.size()));

        moduleOptions = Collections
            .unmodifiableMap((Map<String, String>) jaasConfigEntries.get(0).getOptions());

        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(moduleOptions);

        String clientId = conf.getClientId();
        String clientSecret = conf.getClientSecret();
        String scope = conf.getScope();
        String scopeClaimName = conf.getScopeClaimName();
        String subClaimName = conf.getSubClaimName();
        String tokenEndpointUri = conf.getTokenEndpointUri();
        long loginConnectTimeoutMs = conf.getLoginConnectTimeoutMs();
        long loginReadTimeoutMs = conf.getLoginReadTimeoutMs();
        Retry<String> retry = new Retry<>(tokenEndpointUri,
                conf.getLoginAttempts(),
                conf.getLoginRetryWaitMs(),
                conf.getLoginRetryMaxWaitMs());
        client = new LoginTokenEndpointHttpClient(clientId,
            clientSecret,
            scope,
            tokenEndpointUri,
            loginConnectTimeoutMs,
            loginReadTimeoutMs,
            retry);
        accessTokenValidator = new LoginAccessTokenValidator(scopeClaimName, subClaimName);
    }

    @Override
    public void close() {

    }

    @Override
    public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                handle((OAuthBearerTokenCallback) callback);
            } else if (callback instanceof SaslExtensionsCallback) {
                handle((SaslExtensionsCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handle(OAuthBearerTokenCallback callback) throws IOException {
        String accessToken = client.getAccessToken();
        log.debug("handle - accessToken: {}", accessToken);

        OAuthBearerToken token;

        try {
            token = accessTokenValidator.validate(accessToken);
        } catch (ValidateException e) {
            throw new IllegalStateException("Could not validate OAuth access token", e);
        }

        log.debug("handle - token: {}", token);
        callback.token(token);
    }

    private void handle(SaslExtensionsCallback callback) {
        Map<String, String> extensions = new HashMap<>();

        for (Map.Entry<String, String> configEntry : this.moduleOptions.entrySet()) {
            String key = configEntry.getKey();
            if (!key.startsWith(EXTENSION_PREFIX))
                continue;

            extensions.put(key.substring(EXTENSION_PREFIX.length()), configEntry.getValue());
        }

        SaslExtensions saslExtensions = new SaslExtensions(extensions);

        try {
            OAuthBearerClientInitialResponse.validateExtensions(saslExtensions);
        } catch (SaslException e) {
            throw new ConfigException(e.getMessage());
        }

        callback.extensions(saslExtensions);
    }

}
