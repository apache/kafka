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

import java.util.Objects;

import javax.security.auth.callback.Callback;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A {@code Callback} for use by the {@code SaslServer} implementation when it
 * needs to provide an OAuth 2 bearer token compact serialization for
 * validation. Callback handlers should use the
 * {@link #error(String, String, String)} method to communicate errors back to
 * the SASL Client as per
 * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
 * 2.0 Authorization Framework</a> and the <a href=
 * "https://www.iana.org/assignments/oauth-parameters/oauth-parameters.xhtml#extensions-error">IANA
 * OAuth Extensions Error Registry</a>. Callback handlers should communicate
 * other problems by raising an {@code IOException}.
 * <p>
 * This class was introduced in 2.0.0 and, while it feels stable, it could
 * evolve. We will try to evolve the API in a compatible manner, but we reserve
 * the right to make breaking changes in minor releases, if necessary. We will
 * update the {@code InterfaceStability} annotation and this notice once the API
 * is considered stable.
 */
@InterfaceStability.Evolving
public class OAuthBearerValidatorCallback implements Callback {
    private final String tokenValue;
    private OAuthBearerToken token = null;
    private String errorStatus = null;
    private String errorScope = null;
    private String errorOpenIDConfiguration = null;

    /**
     * Constructor
     * 
     * @param tokenValue
     *            the mandatory/non-blank token value
     */
    public OAuthBearerValidatorCallback(String tokenValue) {
        if (Objects.requireNonNull(tokenValue).isEmpty())
            throw new IllegalArgumentException("token value must not be empty");
        this.tokenValue = tokenValue;
    }

    /**
     * Return the (always non-null) token value
     * 
     * @return the (always non-null) token value
     */
    public String tokenValue() {
        return tokenValue;
    }

    /**
     * Return the (potentially null) token
     * 
     * @return the (potentially null) token
     */
    public OAuthBearerToken token() {
        return token;
    }

    /**
     * Return the (potentially null) error status value as per
     * <a href="https://tools.ietf.org/html/rfc7628#section-3.2.2">RFC 7628: A Set
     * of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth</a>
     * and the <a href=
     * "https://www.iana.org/assignments/oauth-parameters/oauth-parameters.xhtml#extensions-error">IANA
     * OAuth Extensions Error Registry</a>.
     * 
     * @return the (potentially null) error status value
     */
    public String errorStatus() {
        return errorStatus;
    }

    /**
     * Return the (potentially null) error scope value as per
     * <a href="https://tools.ietf.org/html/rfc7628#section-3.2.2">RFC 7628: A Set
     * of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth</a>.
     * 
     * @return the (potentially null) error scope value
     */
    public String errorScope() {
        return errorScope;
    }

    /**
     * Return the (potentially null) error openid-configuration value as per
     * <a href="https://tools.ietf.org/html/rfc7628#section-3.2.2">RFC 7628: A Set
     * of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth</a>.
     * 
     * @return the (potentially null) error openid-configuration value
     */
    public String errorOpenIDConfiguration() {
        return errorOpenIDConfiguration;
    }

    /**
     * Set the token. The token value is unchanged and is expected to match the
     * provided token's value. All error values are cleared.
     * 
     * @param token
     *            the mandatory token to set
     */
    public void token(OAuthBearerToken token) {
        this.token = Objects.requireNonNull(token);
        this.errorStatus = null;
        this.errorScope = null;
        this.errorOpenIDConfiguration = null;
    }

    /**
     * Set the error values as per
     * <a href="https://tools.ietf.org/html/rfc7628#section-3.2.2">RFC 7628: A Set
     * of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth</a>.
     * Any token is cleared.
     * 
     * @param errorStatus
     *            the mandatory error status value from the <a href=
     *            "https://www.iana.org/assignments/oauth-parameters/oauth-parameters.xhtml#extensions-error">IANA
     *            OAuth Extensions Error Registry</a> to set
     * @param errorScope
     *            the optional error scope value to set
     * @param errorOpenIDConfiguration
     *            the optional error openid-configuration value to set
     */
    public void error(String errorStatus, String errorScope, String errorOpenIDConfiguration) {
        if (Objects.requireNonNull(errorStatus).isEmpty())
            throw new IllegalArgumentException("error status must not be empty");
        this.errorStatus = errorStatus;
        this.errorScope = errorScope;
        this.errorOpenIDConfiguration = errorOpenIDConfiguration;
        this.token = null;
    }
}
