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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerValidatorCallbackHandler.class);

    private AccessTokenValidator accessTokenValidator;

    @Override
    public void configure(final Map<String, ?> configs, final String saslMechanism,
        final List<AppConfigurationEntry> jaasConfigEntries) {
        ValidatorCallbackHandlerConfiguration conf = null;

        for (AppConfigurationEntry ace : jaasConfigEntries) {
            Map<String, ?> options = ace.getOptions();
            conf = new ValidatorCallbackHandlerConfiguration(options);
        }

        if (conf == null)
            throw new ConfigException("The OAuth validator callback was not provided any options");

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
        } catch (ValidateException e) {
            throw new IllegalStateException("Could not validate OAuth access token", e);
        }

        log.debug("handle - token: {}", token);
        callback.token(token);
    }

}
