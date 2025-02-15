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

import org.apache.kafka.common.security.oauthbearer.internals.secured.BrokerAccessTokenValidator;
import org.apache.kafka.common.utils.Time;

/**
 * Implementation of {@link AccessTokenValidator} that is used
 * by the broker to perform more extensive validation of the JWT access token that is received
 * from the client, but ultimately from posting the client credentials to the OAuth/OIDC provider's
 * token endpoint.
 *
 * The validation steps performed (primary by the jose4j library) are:
 *
 * <ol>
 *     <li>
 *         Basic structural validation of the <code>b64token</code> value as defined in
 *         <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
 *     </li>
 *     <li>Basic conversion of the token into an in-memory data structure</li>
 *     <li>
 *         Presence of scope, <code>exp</code>, subject, <code>iss</code>, and
 *         <code>iat</code> claims
 *     </li>
 *     <li>
 *         Signature matching validation against the <code>kid</code> and those provided by
 *         the OAuth/OIDC provider's JWKS
 *     </li>
 * </ol>
 */

public class DefaultAccessTokenValidator extends BrokerAccessTokenValidator {

    public DefaultAccessTokenValidator() {
        super(Time.SYSTEM);
    }

    public DefaultAccessTokenValidator(Time time) {
        super(time);
    }
}
