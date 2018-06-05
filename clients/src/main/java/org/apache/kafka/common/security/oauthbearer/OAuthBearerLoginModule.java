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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.Login;
import org.apache.kafka.common.security.oauthbearer.internal.OAuthBearerSaslClientProvider;
import org.apache.kafka.common.security.oauthbearer.internal.OAuthBearerSaslServerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code LoginModule} for the SASL/OAUTHBEARER mechanism. When a client
 * (whether a non-broker client or a broker when SASL/OAUTHBEARER is the
 * inter-broker protocol) connects to Kafka the {@code OAuthBearerLoginModule}
 * instance asks its configured {@link AuthenticateCallbackHandler}
 * implementation to handle an instance of {@link OAuthBearerTokenCallback} and
 * return an instance of {@link OAuthBearerToken}. A default, builtin
 * {@link AuthenticateCallbackHandler} implementation creates an unsecured token
 * as defined by these JAAS module options:
 * <p>
 * <table>
 * <tr>
 * <th>JAAS Module Option for Unsecured Token Retrieval</th>
 * <th>Documentation</th>
 * </tr>
 * <tr>
 * <td>{@code unsecuredLoginStringClaim_<claimname>="value"}</td>
 * <td>Creates a {@code String} claim with the given name and value. Any valid
 * claim name can be specified except '{@code iat}' and '{@code exp}' (these are
 * automatically generated).</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredLoginNumberClaim_<claimname>="value"}</td>
 * <td>Creates a {@code Number} claim with the given name and value. Any valid
 * claim name can be specified except '{@code iat}' and '{@code exp}' (these are
 * automatically generated).</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredLoginListClaim_<claimname>="value"}</td>
 * <td>Creates a {@code String List} claim with the given name and values parsed
 * from the given value where the first character is taken as the delimiter. For
 * example: {@code unsecuredLoginListClaim_fubar="|value1|value2"}. Any valid
 * claim name can be specified except '{@code iat}' and '{@code exp}' (these are
 * automatically generated).</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredLoginPrincipalClaimName}</td>
 * <td>Set to a custom claim name if you wish the name of the {@code String}
 * claim holding the principal name to be something other than
 * '{@code sub}'.</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredLoginLifetimeSeconds}</td>
 * <td>Set to an integer value if the token expiration is to be set to something
 * other than the default value of 3600 seconds (which is 1 hour). The
 * '{@code exp}' claim will be set to reflect the expiration time.</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredLoginScopeClaimName}</td>
 * <td>Set to a custom claim name if you wish the name of the {@code String} or
 * {@code String List} claim holding any token scope to be something other than
 * '{@code scope}'.</td>
 * </tr>
 * </table>
 * <p>
 * Production use cases will require writing an implementation of
 * {@link AuthenticateCallbackHandler} that can handle an instance of
 * {@link OAuthBearerTokenCallback} and declaring it via either the
 * {@code sasl.login.callback.handler.class} configuration option for a
 * non-broker client or via the
 * {@code listener.name.sasl_ssl.oauthbearer.sasl.login.callback.handler.class}
 * configuration option for brokers (when SASL/OAUTHBEARER is the inter-broker
 * protocol).
 * <p>
 * This class stores the retrieved {@link OAuthBearerToken} in the
 * {@code Subject}'s private credentials where the {@code SaslClient} can
 * retrieve it. An appropriate, builtin {@code SaslClient} implementation is
 * automatically used and configured such that it can perform that retrieval.
 * <p>
 * Here is a typical, basic JAAS configuration for a client leveraging unsecured
 * SASL/OAUTHBEARER authentication:
 * 
 * <pre>
 * KafkaClient {
 *      org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *      unsecuredLoginStringClaim_sub="thePrincipalName";
 * };
 * </pre>
 * 
 * An implementation of the {@link Login} interface specific to the
 * {@code OAUTHBEARER} mechanism is automatically applied; it periodically
 * refreshes any token before it expires so that the client can continue to make
 * connections to brokers. The parameters that impact how the refresh algorithm
 * operates are specified as part of the producer/consumer/broker configuration
 * and are as follows. See the documentation for these properties elsewhere for
 * details.
 * <p>
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
 * <p>
 * When a broker accepts a SASL/OAUTHBEARER connection the instance of the
 * builtin {@code SaslServer} implementation asks its configured
 * {@link AuthenticateCallbackHandler} implementation to handle an instance of
 * {@link OAuthBearerValidatorCallback} constructed with the OAuth 2 Bearer
 * Token's compact serialization and return an instance of
 * {@link OAuthBearerToken} if the value validates. A default, builtin
 * {@link AuthenticateCallbackHandler} implementation validates an unsecured
 * token as defined by these JAAS module options:
 * <p>
 * <table>
 * <tr>
 * <th>JAAS Module Option for Unsecured Token Validation</th>
 * <th>Documentation</th>
 * </tr>
 * <tr>
 * <td>{@code unsecuredValidatorPrincipalClaimName="value"}</td>
 * <td>Set to a non-empty value if you wish a particular {@code String} claim
 * holding a principal name to be checked for existence; the default is to check
 * for the existence of the '{@code sub}' claim.</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredValidatorScopeClaimName="value"}</td>
 * <td>Set to a custom claim name if you wish the name of the {@code String} or
 * {@code String List} claim holding any token scope to be something other than
 * '{@code scope}'.</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredValidatorRequiredScope="value"}</td>
 * <td>Set to a space-delimited list of scope values if you wish the
 * {@code String/String List} claim holding the token scope to be checked to
 * make sure it contains certain values.</td>
 * </tr>
 * <tr>
 * <td>{@code unsecuredValidatorAllowableClockSkewMs="value"}</td>
 * <td>Set to a positive integer value if you wish to allow up to some number of
 * positive milliseconds of clock skew (the default is 0).</td>
 * </tr>
 * </table>
 * <p>
 * Here is a typical, basic JAAS configuration for a broker leveraging unsecured
 * SASL/OAUTHBEARER validation:
 * 
 * <pre>
 * KafkaServer {
 *      org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *      unsecuredLoginStringClaim_sub="thePrincipalName";
 * };
 * </pre>
 * 
 * Production use cases will require writing an implementation of
 * {@link AuthenticateCallbackHandler} that can handle an instance of
 * {@link OAuthBearerValidatorCallback} and declaring it via the
 * {@code listener.name.sasl_ssl.oauthbearer.sasl.server.callback.handler.class}
 * broker configuration option.
 * <p>
 * The builtin {@code SaslServer} implementation for SASL/OAUTHBEARER in Kafka
 * makes the instance of {@link OAuthBearerToken} available upon successful
 * authentication via the negotiated property "{@code OAUTHBEARER.token}"; the
 * token could be used in a custom authorizer (to authorize based on JWT claims
 * rather than ACLs, for example).
 * <p>
 * This implementation's {@code logout()} method will logout the specific token
 * that this instance logged in if it's {@code Subject} instance is shared
 * across multiple {@code LoginContext}s and there happen to be multiple tokens
 * on the {@code Subject}. This functionality is useful because it means a new
 * token with a longer lifetime can be created before a soon-to-expire token is
 * actually logged out. Otherwise, if multiple simultaneous tokens were not
 * supported like this, the soon-to-be expired token would have to be logged out
 * first, and then if the new token could not be retrieved (maybe the
 * authorization server is temporarily unavailable, for example) the client
 * would be left without a token and would be unable to create new connections.
 * Better to mitigate this possibility by leaving the existing token (which
 * still has some lifetime left) in place until a new replacement token is
 * actually retrieved. This implementation supports this.
 * 
 * @see SaslConfigs#SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
 * @see SaslConfigs#SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC
 */
public class OAuthBearerLoginModule implements LoginModule {
    /**
     * The SASL Mechanism name for OAuth 2: {@code OAUTHBEARER}
     */
    public static final String OAUTHBEARER_MECHANISM = "OAUTHBEARER";
    private static final Logger log = LoggerFactory.getLogger(OAuthBearerLoginModule.class);
    private Subject subject = null;
    private AuthenticateCallbackHandler callbackHandler = null;
    private OAuthBearerToken tokenRequiringCommit = null;
    private OAuthBearerToken myCommittedToken = null;

    static {
        OAuthBearerSaslClientProvider.initialize(); // not part of public API
        OAuthBearerSaslServerProvider.initialize(); // not part of public API
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState,
            Map<String, ?> options) {
        this.subject = Objects.requireNonNull(subject);
        if (!(Objects.requireNonNull(callbackHandler) instanceof AuthenticateCallbackHandler))
            throw new IllegalArgumentException(String.format("Callback handler must be castable to %s: %s",
                    AuthenticateCallbackHandler.class.getName(), callbackHandler.getClass().getName()));
        this.callbackHandler = (AuthenticateCallbackHandler) callbackHandler;
    }

    @Override
    public boolean login() throws LoginException {
        if (tokenRequiringCommit != null)
            throw new IllegalStateException(String.format(
                    "Already have an uncommitted token with private credential token count=%d", committedTokenCount()));
        if (myCommittedToken != null)
            throw new IllegalStateException(String.format(
                    "Already have a committed token with private credential token count=%d; must login on another login context or logout here first before reusing the same login context",
                    committedTokenCount()));
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        try {
            callbackHandler.handle(new Callback[] {callback});
        } catch (IOException | UnsupportedCallbackException e) {
            log.error(e.getMessage(), e);
            throw new LoginException("An internal error occurred");
        }
        tokenRequiringCommit = callback.token();
        if (tokenRequiringCommit == null) {
            log.info(String.format("Login failed: %s : %s (URI=%s)", callback.errorCode(), callback.errorDescription(),
                    callback.errorUri()));
            throw new LoginException(callback.errorDescription());
        }
        log.info("Login succeeded; invoke commit() to commit it; current committed token count={}",
                committedTokenCount());
        return true;
    }

    @Override
    public boolean logout() {
        if (tokenRequiringCommit != null)
            throw new IllegalStateException(
                    "Cannot call logout() immediately after login(); need to first invoke commit() or abort()");
        if (myCommittedToken == null) {
            if (log.isDebugEnabled())
                log.debug("Nothing here to log out");
            return false;
        }
        log.info("Logging out my token; current committed token count = {}", committedTokenCount());
        for (Iterator<Object> iterator = subject.getPrivateCredentials().iterator(); iterator.hasNext();) {
            Object privateCredential = iterator.next();
            if (privateCredential == myCommittedToken) {
                iterator.remove();
                myCommittedToken = null;
                break;
            }
        }
        log.info("Done logging out my token; committed token count is now {}", committedTokenCount());
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        if (tokenRequiringCommit == null) {
            if (log.isDebugEnabled())
                log.debug("Nothing here to commit");
            return false;
        }
        log.info("Committing my token; current committed token count = {}", committedTokenCount());
        subject.getPrivateCredentials().add(tokenRequiringCommit);
        myCommittedToken = tokenRequiringCommit;
        tokenRequiringCommit = null;
        log.info("Done committing my token; committed token count is now {}", committedTokenCount());
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        if (tokenRequiringCommit != null) {
            log.info("Login aborted");
            tokenRequiringCommit = null;
            return true;
        }
        if (log.isDebugEnabled())
            log.debug("Nothing here to abort");
        return false;
    }

    private int committedTokenCount() {
        return subject.getPrivateCredentials(OAuthBearerToken.class).size();
    }
}
