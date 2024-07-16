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
package org.apache.kafka.common.security.oauthbearer.internals;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredential;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshConfig;
import org.apache.kafka.common.security.oauthbearer.internals.expiring.ExpiringCredentialRefreshingLogin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * This class is responsible for refreshing logins for both Kafka client and
 * server when the credential is an OAuth 2 bearer token communicated over
 * SASL/OAUTHBEARER. An OAuth 2 bearer token has a limited lifetime, and an
 * instance of this class periodically refreshes it so that the client can
 * create new connections to brokers on an ongoing basis.
 * <p>
 * This class does not need to be explicitly set via the
 * {@code sasl.login.class} client configuration property or the
 * {@code listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.login.class}
 * broker configuration property when the SASL mechanism is OAUTHBEARER; it is
 * automatically set by default in that case.
 * <p>
 * The parameters that impact how the refresh algorithm operates are specified
 * as part of the producer/consumer/broker configuration and are as follows. See
 * the documentation for these properties elsewhere for details.
 * <table>
 * <tr>
 * <th>Producer/Consumer/Broker Configuration Property</th>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.window.factor}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.window.jitter}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.min.period.seconds}</td>
 * </tr>
 * <tr>
 * <td>{@code sasl.login.refresh.min.buffer.seconds}</td>
 * </tr>
 * </table>
 * 
 * @see OAuthBearerLoginModule
 * @see SaslConfigs#SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC
 */
public class OAuthBearerRefreshingLogin implements Login {

    private static class OAuthBearerExpiringCredential implements ExpiringCredential {
        private final OAuthBearerToken token;

        public OAuthBearerExpiringCredential(OAuthBearerToken token) {
            this.token = token;
        }

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
    }

    private static final Logger log = LoggerFactory.getLogger(OAuthBearerRefreshingLogin.class);
    private ExpiringCredentialRefreshingLogin expiringCredentialRefreshingLogin = null;

    @Override
    public void configure(Map<String, ?> configs, String contextName, Configuration configuration,
            AuthenticateCallbackHandler loginCallbackHandler) {
        /*
         * Specify this class as the one to synchronize on so that only one OAuth 2
         * Bearer Token is refreshed at a given time. Specify null if we don't mind
         * multiple simultaneously refreshes. Refreshes happen on the order of minutes
         * rather than seconds or milliseconds, and there are typically minutes of
         * lifetime remaining when the refresh occurs, so serializing them seems
         * reasonable.
         */
        Class<OAuthBearerRefreshingLogin> classToSynchronizeOnPriorToRefresh = OAuthBearerRefreshingLogin.class;
        expiringCredentialRefreshingLogin = new ExpiringCredentialRefreshingLogin(contextName, configuration,
                new ExpiringCredentialRefreshConfig(configs, true), loginCallbackHandler,
                classToSynchronizeOnPriorToRefresh) {
            @Override
            public ExpiringCredential expiringCredential() {
                Set<OAuthBearerToken> privateCredentialTokens = expiringCredentialRefreshingLogin.subject()
                        .getPrivateCredentials(OAuthBearerToken.class);
                if (privateCredentialTokens.isEmpty())
                    return null;
                final OAuthBearerToken token = privateCredentialTokens.iterator().next();
                if (log.isDebugEnabled())
                    log.debug("Found expiring credential with principal '{}'.", token.principalName());
                return new OAuthBearerExpiringCredential(token);
            }
        };
    }

    @Override
    public void close() {
        if (expiringCredentialRefreshingLogin != null)
            expiringCredentialRefreshingLogin.close();
    }

    @Override
    public Subject subject() {
        return expiringCredentialRefreshingLogin != null ? expiringCredentialRefreshingLogin.subject() : null;
    }

    @Override
    public String serviceName() {
        return expiringCredentialRefreshingLogin != null ? expiringCredentialRefreshingLogin.serviceName() : null;
    }

    @Override
    public synchronized LoginContext login() throws LoginException {
        if (expiringCredentialRefreshingLogin != null)
            return expiringCredentialRefreshingLogin.login();
        throw new LoginException("Login was not configured properly");
    }
}
