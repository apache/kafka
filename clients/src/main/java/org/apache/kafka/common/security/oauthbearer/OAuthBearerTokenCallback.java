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
 * A {@code Callback} for use by the {@code SaslClient} and {@code Login}
 * implementations when they require an OAuth 2 bearer token. Callback handlers
 * should use the {@link #error(String, String, String)} method to communicate
 * errors returned by the authorization server as per
 * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
 * 2.0 Authorization Framework</a>. Callback handlers should communicate other
 * problems by raising an {@code IOException}.
 * <p>
 * This class was introduced in 2.0.0 and, while it feels stable, it could
 * evolve. We will try to evolve the API in a compatible manner, but we reserve
 * the right to make breaking changes in minor releases, if necessary. We will
 * update the {@code InterfaceStability} annotation and this notice once the API
 * is considered stable.
 */
@InterfaceStability.Evolving
public class OAuthBearerTokenCallback implements Callback {
    private OAuthBearerToken token = null;
    private String errorCode = null;
    private String errorDescription = null;
    private String errorUri = null;

    /**
     * Return the (potentially null) token
     * 
     * @return the (potentially null) token
     */
    public OAuthBearerToken token() {
        return token;
    }

    /**
     * Return the optional (but always non-empty if not null) error code as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>.
     * 
     * @return the optional (but always non-empty if not null) error code
     */
    public String errorCode() {
        return errorCode;
    }

    /**
     * Return the (potentially null) error description as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>.
     * 
     * @return the (potentially null) error description
     */
    public String errorDescription() {
        return errorDescription;
    }

    /**
     * Return the (potentially null) error URI as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>.
     * 
     * @return the (potentially null) error URI
     */
    public String errorUri() {
        return errorUri;
    }

    /**
     * Set the token. All error-related values are cleared.
     * 
     * @param token
     *            the optional token to set
     */
    public void token(OAuthBearerToken token) {
        this.token = token;
        this.errorCode = null;
        this.errorDescription = null;
        this.errorUri = null;
    }

    /**
     * Set the error values as per
     * <a href="https://tools.ietf.org/html/rfc6749#section-5.2">RFC 6749: The OAuth
     * 2.0 Authorization Framework</a>. Any token is cleared.
     * 
     * @param errorCode
     *            the mandatory error code to set
     * @param errorDescription
     *            the optional error description to set
     * @param errorUri
     *            the optional error URI to set
     */
    public void error(String errorCode, String errorDescription, String errorUri) {
        if (Objects.requireNonNull(errorCode).isEmpty())
            throw new IllegalArgumentException("error code must not be empty");
        this.errorCode = errorCode;
        this.errorDescription = errorDescription;
        this.errorUri = errorUri;
        this.token = null;
    }
}
