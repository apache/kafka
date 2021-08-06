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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.*;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler.class);

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
        Map<String, String> moduleOptions = Collections
            .unmodifiableMap((Map<String, String>) jaasConfigEntries.get(0).getOptions());

        ValidatorCallbackHandlerConfiguration conf = new ValidatorCallbackHandlerConfiguration(moduleOptions);

        Integer clockSkew = conf.getClockSkew();
        Set<String> expectedAudiences = conf.getExpectedAudiences();
        String expectedIssuer = conf.getExpectedIssuer();
        VerificationKeyResolver verificationKeyResolver = conf.getVerificationKeyResolver();
        String scopeClaimName = conf.getScopeClaimName();
        String subClaimName = conf.getSubClaimName();

        this.accessTokenValidator = new ValidatorAccessTokenValidator(clockSkew,
            expectedAudiences,
            expectedIssuer,
            verificationKeyResolver,
            scopeClaimName,
            subClaimName);
    }

    @Override
    public void close() {

    }

    @Override
    public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
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
        String accessToken = callback.tokenValue();
        log.debug("handle - accessToken: {}", accessToken);

        OAuthBearerToken token;

        try {
            token = accessTokenValidator.validate(accessToken);
            log.debug("handle - token: {}", token);
            callback.token(token);
        } catch (ValidateException e) {
            log.warn(e.getMessage(), e);
            callback.error("invalid_token", null, null);
//            throw new IllegalStateException("Could not validate OAuth access token", e);
        }
    }

}
