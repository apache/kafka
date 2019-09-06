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
package org.apache.kafka.common.security.oauthbearer.internals.unsecured;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code CallbackHandler} that recognizes
 * {@link OAuthBearerValidatorCallback} and validates an unsecured OAuth 2
 * bearer token. It requires there to be an <code>"exp" (Expiration Time)</code>
 * claim of type Number. If <code>"iat" (Issued At)</code> or
 * <code>"nbf" (Not Before)</code> claims are present each must be a number that
 * precedes the Expiration Time claim, and if both are present the Not Before
 * claim must not precede the Issued At claim. It also accepts the following
 * options, none of which are required:
 * <ul>
 * <li>{@code unsecuredValidatorPrincipalClaimName} set to a non-empty value if
 * you wish a particular String claim holding a principal name to be checked for
 * existence; the default is to check for the existence of the '{@code sub}'
 * claim</li>
 * <li>{@code unsecuredValidatorScopeClaimName} set to a custom claim name if
 * you wish the name of the String or String List claim holding any token scope
 * to be something other than '{@code scope}'</li>
 * <li>{@code unsecuredValidatorRequiredScope} set to a space-delimited list of
 * scope values if you wish the String/String List claim holding the token scope
 * to be checked to make sure it contains certain values</li>
 * <li>{@code unsecuredValidatorAllowableClockSkewMs} set to a positive integer
 * value if you wish to allow up to some number of positive milliseconds of
 * clock skew (the default is 0)</li>
 * <ul>
 * For example:
 *
 * <pre>
 * KafkaServer {
 *      org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required
 *      unsecuredLoginStringClaim_sub="thePrincipalName"
 *      unsecuredLoginListClaim_scope=",KAFKA_BROKER,LOGIN_TO_KAFKA"
 *      unsecuredValidatorRequiredScope="LOGIN_TO_KAFKA"
 *      unsecuredValidatorAllowableClockSkewMs="3000";
 * };
 * </pre>
 * It also recognizes {@link OAuthBearerExtensionsValidatorCallback} and validates every extension passed to it.
 *
 * This class is the default when the SASL mechanism is OAUTHBEARER and no value
 * is explicitly set via the
 * {@code listener.name.sasl_[plaintext|ssl].oauthbearer.sasl.server.callback.handler.class}
 * broker configuration property.
 * It is worth noting that this class is not suitable for production use due to the use of unsecured JWT tokens and
 * validation of every given extension.
 */
public class OAuthBearerUnsecuredValidatorCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger log = LoggerFactory.getLogger(OAuthBearerUnsecuredValidatorCallbackHandler.class);
    private static final String OPTION_PREFIX = "unsecuredValidator";
    private static final String PRINCIPAL_CLAIM_NAME_OPTION = OPTION_PREFIX + "PrincipalClaimName";
    private static final String SCOPE_CLAIM_NAME_OPTION = OPTION_PREFIX + "ScopeClaimName";
    private static final String REQUIRED_SCOPE_OPTION = OPTION_PREFIX + "RequiredScope";
    private static final String ALLOWABLE_CLOCK_SKEW_MILLIS_OPTION = OPTION_PREFIX + "AllowableClockSkewMs";
    private Time time = Time.SYSTEM;
    private Map<String, String> moduleOptions = null;
    private boolean configured = false;

    /**
     * For testing
     *
     * @param time
     *            the mandatory time to set
     */
    void time(Time time) {
        this.time = Objects.requireNonNull(time);
    }

    /**
     * Return true if this instance has been configured, otherwise false
     *
     * @return true if this instance has been configured, otherwise false
     */
    public boolean configured() {
        return configured;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));
        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null)
            throw new IllegalArgumentException(
                    String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                            jaasConfigEntries.size()));
        final Map<String, String> unmodifiableModuleOptions = Collections
                .unmodifiableMap((Map<String, String>) jaasConfigEntries.get(0).getOptions());
        this.moduleOptions = unmodifiableModuleOptions;
        configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        if (!configured())
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
                try {
                    handleCallback(validationCallback);
                } catch (OAuthBearerIllegalTokenException e) {
                    OAuthBearerValidationResult failureReason = e.reason();
                    String failureScope = failureReason.failureScope();
                    validationCallback.error(failureScope != null ? "insufficient_scope" : "invalid_token",
                            failureScope, failureReason.failureOpenIdConfig());
                }
            } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                OAuthBearerExtensionsValidatorCallback extensionsCallback = (OAuthBearerExtensionsValidatorCallback) callback;
                extensionsCallback.inputExtensions().map().forEach((extensionName, v) -> extensionsCallback.valid(extensionName));
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }

    @Override
    public void close() {
        // empty
    }

    private void handleCallback(OAuthBearerValidatorCallback callback) {
        String tokenValue = callback.tokenValue();
        if (tokenValue == null)
            throw new IllegalArgumentException("Callback missing required token value");
        String principalClaimName = principalClaimName();
        String scopeClaimName = scopeClaimName();
        List<String> requiredScope = requiredScope();
        int allowableClockSkewMs = allowableClockSkewMs();
        OAuthBearerUnsecuredJws unsecuredJwt = new OAuthBearerUnsecuredJws(tokenValue, principalClaimName,
                scopeClaimName);
        long now = time.milliseconds();
        OAuthBearerValidationUtils
                .validateClaimForExistenceAndType(unsecuredJwt, true, principalClaimName, String.class)
                .throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateIssuedAt(unsecuredJwt, false, now, allowableClockSkewMs)
                .throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateExpirationTime(unsecuredJwt, now, allowableClockSkewMs)
                .throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateTimeConsistency(unsecuredJwt).throwExceptionIfFailed();
        OAuthBearerValidationUtils.validateScope(unsecuredJwt, requiredScope).throwExceptionIfFailed();
        log.info("Successfully validated token with principal {}: {}", unsecuredJwt.principalName(),
                unsecuredJwt.claims().toString());
        callback.token(unsecuredJwt);
    }

    private String principalClaimName() {
        String principalClaimNameValue = option(PRINCIPAL_CLAIM_NAME_OPTION);
        String principalClaimName = principalClaimNameValue != null && !principalClaimNameValue.trim().isEmpty()
                ? principalClaimNameValue.trim()
                : "sub";
        return principalClaimName;
    }

    private String scopeClaimName() {
        String scopeClaimNameValue = option(SCOPE_CLAIM_NAME_OPTION);
        String scopeClaimName = scopeClaimNameValue != null && !scopeClaimNameValue.trim().isEmpty()
                ? scopeClaimNameValue.trim()
                : "scope";
        return scopeClaimName;
    }

    private List<String> requiredScope() {
        String requiredSpaceDelimitedScope = option(REQUIRED_SCOPE_OPTION);
        List<String> requiredScope = requiredSpaceDelimitedScope == null || requiredSpaceDelimitedScope.trim().isEmpty()
            ? Collections.emptyList()
            : OAuthBearerScopeUtils.parseScope(requiredSpaceDelimitedScope.trim());
        return requiredScope;
    }

    private int allowableClockSkewMs() {
        String allowableClockSkewMsValue = option(ALLOWABLE_CLOCK_SKEW_MILLIS_OPTION);
        int allowableClockSkewMs = 0;
        try {
            allowableClockSkewMs = allowableClockSkewMsValue == null || allowableClockSkewMsValue.trim().isEmpty() ? 0
                    : Integer.parseInt(allowableClockSkewMsValue.trim());
        } catch (NumberFormatException e) {
            throw new OAuthBearerConfigException(e.getMessage(), e);
        }
        if (allowableClockSkewMs < 0) {
            throw new OAuthBearerConfigException(
                    String.format("Allowable clock skew millis must not be negative: %s", allowableClockSkewMsValue));
        }
        return allowableClockSkewMs;
    }

    private String option(String key) {
        if (!configured)
            throw new IllegalStateException("Callback handler not configured");
        return moduleOptions.get(Objects.requireNonNull(key));
    }
}
