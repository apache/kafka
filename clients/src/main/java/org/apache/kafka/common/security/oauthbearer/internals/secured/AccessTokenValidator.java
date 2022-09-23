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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

/**
 * An instance of <code>AccessTokenValidator</code> acts as a function object that, given an access
 * token in base-64 encoded JWT format, can parse the data, perform validation, and construct an
 * {@link OAuthBearerToken} for use by the caller.
 *
 * The primary reason for this abstraction is that client and broker may have different libraries
 * available to them to perform these operations. Additionally, the exact steps for validation may
 * differ between implementations. To put this more concretely: the implementation in the Kafka
 * client does not have bundled a robust library to perform this logic, and it is not the
 * responsibility of the client to perform vigorous validation. However, the Kafka broker ships with
 * a richer set of library dependencies that can perform more substantial validation and is also
 * expected to perform a trust-but-verify test of the access token's signature.
 *
 * See:
 *
 * <ul>
 *     <li><a href="https://datatracker.ietf.org/doc/html/rfc6749#section-1.4">RFC 6749, Section 1.4</a></li>
 *     <li><a href="https://datatracker.ietf.org/doc/html/rfc6750#section-2.1">RFC 6750, Section 2.1</a></li>
 *     <li><a href="https://datatracker.ietf.org/doc/html/draft-ietf-oauth-access-token-jwt">RFC 6750, Section 2.1</a></li>
 * </ul>
 *
 * @see LoginAccessTokenValidator A basic AccessTokenValidator used by client-side login
 *                                authentication
 * @see ValidatorAccessTokenValidator A more robust AccessTokenValidator that is used on the broker
 *                                    to validate the token's contents and verify the signature
 */

public interface AccessTokenValidator {

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param accessToken Non-<code>null</code> JWT access token
     *
     * @return {@link OAuthBearerToken}
     *
     * @throws ValidateException Thrown on errors performing validation of given token
     */

    OAuthBearerToken validate(String accessToken) throws ValidateException;

}
