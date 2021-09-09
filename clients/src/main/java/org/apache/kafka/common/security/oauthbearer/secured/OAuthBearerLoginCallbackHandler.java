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

import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.ACCESS_TOKEN_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.ACCESS_TOKEN_FILE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.CLIENT_SECRET_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.LoginCallbackHandlerConfiguration.TOKEN_ENDPOINT_URI_CONFIG;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.net.ssl.SSLSocketFactory;
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

    /**
     * Create an {@link AccessTokenRetriever} from the given
     * {@link LoginCallbackHandlerConfiguration}.
     *
     * <b>Note</b>: the returned <code>AccessTokenRetriever</code> is not initialized here and
     * must be done by the caller.
     *
     * Primarily exposed here for unit testing.
     *
     * @param conf Configuration for {@link javax.security.auth.callback.CallbackHandler}
     *
     * @return Non-<code>null</code> <code>AccessTokenRetriever</code>
     */

    public static AccessTokenRetriever configureAccessTokenRetriever(LoginCallbackHandlerConfiguration conf) {
        String accessToken = conf.getAccessToken();
        String accessTokenFile = conf.getAccessTokenFile();
        String clientId = conf.getClientId();

        long count = Stream.of(accessToken, accessTokenFile, clientId)
            .filter(Objects::nonNull)
            .count();

        if (count != 1) {
            throw new ConfigException(String.format("The OAuth login configuration must include only one of %s, %s, or %s options", ACCESS_TOKEN_CONFIG, ACCESS_TOKEN_FILE_CONFIG, CLIENT_ID_CONFIG));
        } else if (accessToken != null) {
            accessToken = ConfigurationUtils.validateString(ACCESS_TOKEN_CONFIG, accessToken);
            return new StaticAccessTokenRetriever(accessToken);
        } else if (accessTokenFile != null) {
            accessTokenFile = ConfigurationUtils.validateString(ACCESS_TOKEN_FILE_CONFIG, accessTokenFile);
            return new RefreshingFileAccessTokenRetriever(Paths.get(accessTokenFile));
        } else {
            clientId = ConfigurationUtils.validateString(CLIENT_ID_CONFIG, clientId);
            String clientSecret = ConfigurationUtils.validateString(CLIENT_SECRET_CONFIG, conf.getClientSecret());
            String tokenEndpointUri = ConfigurationUtils.validateString(TOKEN_ENDPOINT_URI_CONFIG, conf.getTokenEndpointUri());

            SSLSocketFactory sslSocketFactory = ConfigurationUtils.createSSLSocketFactory(conf.originals(), TOKEN_ENDPOINT_URI_CONFIG);

            return new HttpAccessTokenRetriever(clientId,
                clientSecret,
                conf.getScope(),
                sslSocketFactory,
                tokenEndpointUri,
                conf.getLoginAttempts(),
                conf.getLoginRetryWaitMs(),
                conf.getLoginRetryMaxWaitMs(),
                conf.getLoginConnectTimeoutMs(),
                conf.getLoginReadTimeoutMs());
        }
    }

    public static AccessTokenValidator configureAccessTokenValidator(LoginCallbackHandlerConfiguration conf) {
        return new LoginAccessTokenValidator(conf.getScopeClaimName(), conf.getSubClaimName());
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        moduleOptions = ConfigurationUtils.getModuleOptions(saslMechanism, jaasConfigEntries);

        LoginCallbackHandlerConfiguration conf = new LoginCallbackHandlerConfiguration(moduleOptions);
        AccessTokenRetriever accessTokenRetriever = configureAccessTokenRetriever(conf);
        AccessTokenValidator accessTokenValidator = configureAccessTokenValidator(conf);

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
