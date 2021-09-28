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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler.class);

    private static final String EXTENSION_PREFIX = "Extension_";

    private Map<String, Object> moduleOptions;

    private AccessTokenRetriever accessTokenRetriever;

    private AccessTokenValidator accessTokenValidator;

    private boolean isConfigured = false;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        moduleOptions = ConfigurationUtils.getModuleOptions(saslMechanism, jaasConfigEntries);

        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(moduleOptions);
        AccessTokenRetriever accessTokenRetriever = AccessTokenRetrieverFactory.create(conf);
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(conf);

        configure(accessTokenRetriever, accessTokenValidator);
    }

    public void configure(AccessTokenRetriever accessTokenRetriever, AccessTokenValidator accessTokenValidator) {
        this.accessTokenRetriever = accessTokenRetriever;
        this.accessTokenValidator = accessTokenValidator;

        try {
            this.accessTokenRetriever.init();
        } catch (IOException e) {
            throw new KafkaException("The OAuth login configuration encountered an error when initializing the AccessTokenRetriever", e);
        }

        isConfigured = true;
    }

    @Override
    public void close() {
        if (accessTokenRetriever != null) {
            try {
                this.accessTokenRetriever.close();
            } catch (IOException e) {
                log.warn("The OAuth login configuration encountered an error when closing the AccessTokenRetriever", e);
            }
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        checkConfigured();

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

    void handle(OAuthBearerTokenCallback callback) throws IOException {
        checkConfigured();

        String accessToken = accessTokenRetriever.retrieve();
        log.debug("handle - accessToken: {}", accessToken);

        try {
            OAuthBearerToken token = accessTokenValidator.validate(accessToken);
            log.debug("handle - token: {}", token);
            callback.token(token);
        } catch (ValidateException e) {
            log.warn(e.getMessage(), e);
            callback.error("invalid_token", e.getMessage(), null);
        }
    }

    void handle(SaslExtensionsCallback callback) {
        checkConfigured();

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

    private void checkConfigured() {
        if (!isConfigured)
            throw new IllegalStateException(String.format("To use %s, first call configure method", getClass().getSimpleName()));
    }

}
