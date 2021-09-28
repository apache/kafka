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

import static org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration.JWKS_ENDPOINT_URI_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration.JWKS_FILE_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.secured.ValidatorCallbackHandlerConfiguration.PEM_DIRECTORY_CONFIG;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.jose4j.http.Get;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler.class);

    private CloseableVerificationKeyResolver verificationKeyResolver;

    private AccessTokenValidator accessTokenValidator;

    private boolean isConfigured = false;

    /**
     * Create an {@link AccessTokenRetriever} from the given
     * {@link LoginCallbackHandlerConfiguration}.
     *
     * <b>Note</b>: the returned <code>CloseableVerificationKeyResolver</code> is not
     * initialized here and must be done by the caller.
     *
     * Primarily exposed here for unit testing.
     *
     * @param conf Configuration for {@link javax.security.auth.callback.CallbackHandler}
     *
     * @return Non-<code>null</code> <code>AccessTokenRetriever</code>
     */

    public static CloseableVerificationKeyResolver configureVerificationKeyResolver(ValidatorCallbackHandlerConfiguration conf) {
        String jwksFile = conf.getJwksFile();
        String jwksEndpointUri = conf.getJwksEndpointUri();
        String pemDirectory = conf.getPemDirectory();

        long count = Stream.of(jwksFile, jwksEndpointUri, pemDirectory)
            .filter(Objects::nonNull)
            .count();

        if (count != 1) {
            throw new ConfigException(String.format("The OAuth validator configuration must include only one of %s, %s, or %s options", JWKS_FILE_CONFIG, JWKS_ENDPOINT_URI_CONFIG, PEM_DIRECTORY_CONFIG));
        } else if (jwksFile != null) {
            jwksFile = ConfigurationUtils.validateString(JWKS_FILE_CONFIG, jwksFile);
            return new JwksFileVerificationKeyResolver(Paths.get(jwksFile));
        } else if (jwksEndpointUri != null) {
            jwksEndpointUri = ConfigurationUtils.validateString(JWKS_ENDPOINT_URI_CONFIG, jwksEndpointUri);
            long refreshIntervalMs = conf.getJwksEndpointRefreshIntervalMs();
            RefreshingHttpsJwks httpsJkws = new RefreshingHttpsJwks(jwksEndpointUri, refreshIntervalMs);
            SSLSocketFactory sslSocketFactory = ConfigurationUtils.createSSLSocketFactory(conf.originals(), JWKS_ENDPOINT_URI_CONFIG);

            if (sslSocketFactory != null) {
                Get get = new Get();
                get.setSslSocketFactory(sslSocketFactory);
                httpsJkws.setSimpleHttpGet(get);
            }

            return new RefreshingHttpsJwksVerificationKeyResolver(httpsJkws);
        } else {
            pemDirectory = ConfigurationUtils.validateString(PEM_DIRECTORY_CONFIG, pemDirectory);
            return new PemDirectoryVerificationKeyResolver(Paths.get(pemDirectory));
        }
    }

    public static AccessTokenValidator configureAccessTokenValidator(ValidatorCallbackHandlerConfiguration conf,
        VerificationKeyResolver verificationKeyResolver) {
        List<String> expectedAudiences = conf.getExpectedAudiences();
        Integer clockSkew = conf.getClockSkew();
        String expectedIssuer = conf.getExpectedIssuer();
        String scopeClaimName = conf.getScopeClaimName();
        String subClaimName = conf.getSubClaimName();

        return new ValidatorAccessTokenValidator(clockSkew,
            expectedAudiences != null ? Collections.unmodifiableSet(new HashSet<>(expectedAudiences)) : null,
            expectedIssuer,
            verificationKeyResolver,
            scopeClaimName,
            subClaimName);
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Map<String, Object> moduleOptions = ConfigurationUtils.getModuleOptions(saslMechanism, jaasConfigEntries);

        ValidatorCallbackHandlerConfiguration conf = new ValidatorCallbackHandlerConfiguration(moduleOptions);
        CloseableVerificationKeyResolver verificationKeyResolver = configureVerificationKeyResolver(conf);
        AccessTokenValidator accessTokenValidator = configureAccessTokenValidator(conf, verificationKeyResolver);

        configure(verificationKeyResolver, accessTokenValidator);
    }

    public void configure(CloseableVerificationKeyResolver verificationKeyResolver, AccessTokenValidator accessTokenValidator) {
        this.verificationKeyResolver = verificationKeyResolver;
        this.accessTokenValidator = accessTokenValidator;

        try {
            verificationKeyResolver.init();
        } catch (Exception e) {
            throw new KafkaException("The OAuth validator configuration encountered an error when initializing the VerificationKeyResolver", e);
        }

        isConfigured = true;
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
        checkConfigured();

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                handle((OAuthBearerValidatorCallback) callback);
            } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                OAuthBearerExtensionsValidatorCallback extensionsCallback = (OAuthBearerExtensionsValidatorCallback) callback;
                extensionsCallback.inputExtensions().map().forEach((extensionName, v) -> extensionsCallback.valid(extensionName));
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handle(OAuthBearerValidatorCallback callback) {
        checkConfigured();

        OAuthBearerToken token;

        try {
            token = accessTokenValidator.validate(callback.tokenValue());
            log.debug("handle - token: {}", token);
            callback.token(token);
        } catch (ValidateException e) {
            e.printStackTrace();
            log.warn(e.getMessage(), e);
            callback.error("invalid_token", null, null);
        }
    }

    private void checkConfigured() {
        if (!isConfigured)
            throw new IllegalStateException(String.format("To use %s, first call configure method", getClass().getSimpleName()));
    }

}
