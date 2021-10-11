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
import java.util.List;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler.class);

    private CloseableVerificationKeyResolver verificationKeyResolver;

    private AccessTokenValidator accessTokenValidator;

    private boolean isConfigured = false;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        CloseableVerificationKeyResolver verificationKeyResolver = VerificationKeyResolverFactory.create(configs, saslMechanism);
        AccessTokenValidator accessTokenValidator = AccessTokenValidatorFactory.create(configs, saslMechanism, verificationKeyResolver);
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
