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

package org.apache.kafka.common.security.oauthbearer.internals.expiring;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.security.auth.login.Configuration;

public class OAuthBearerExpiringCredentialRefreshingLogin extends ExpiringCredentialRefreshingLogin {

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerExpiringCredentialRefreshingLogin.class);

    public OAuthBearerExpiringCredentialRefreshingLogin(String contextName, Configuration configuration,
                                                        ExpiringCredentialRefreshConfig expiringCredentialRefreshConfig,
                                                        AuthenticateCallbackHandler callbackHandler,
                                                        Class<?> mandatoryClassToSynchronizeOnPriorToRefresh) {
        super(contextName, configuration, expiringCredentialRefreshConfig, callbackHandler,
                mandatoryClassToSynchronizeOnPriorToRefresh);
    }

    OAuthBearerExpiringCredentialRefreshingLogin(String contextName, Configuration configuration,
                                                        ExpiringCredentialRefreshConfig expiringCredentialRefreshConfig,
                                                        AuthenticateCallbackHandler callbackHandler,
                                                        Class<?> mandatoryClassToSynchronizeOnPriorToRefresh,
                                                        ExpiringCredentialRefreshingLogin.LoginContextFactory loginContextFactory,
                                                        Time time) {
        super(contextName, configuration, expiringCredentialRefreshConfig, callbackHandler,
                mandatoryClassToSynchronizeOnPriorToRefresh, loginContextFactory, time);
    }

    @Override
    public ExpiringCredential expiringCredential() {
        Set<OAuthBearerToken> privateCredentialTokens = this.subject()
                .getPrivateCredentials(OAuthBearerToken.class);
        if (privateCredentialTokens.isEmpty())
            return null;
        final OAuthBearerToken token = privateCredentialTokens.iterator().next();
        if (log.isDebugEnabled())
            log.debug("Found expiring credential with principal '{}'.", token.principalName());
        return new ExpiringCredential() {
            @Override
            public String principalName() {
                return token.principalName();
            }

            @Override
            public Long startTimeMs() {
                return token.startTimeMs();
            }

            @Override
            public long expireTimeMs() {
                return token.lifetimeMs();
            }

            @Override
            public Long absoluteLastRefreshTimeMs() {
                return null;
            }
        };
    }
}
