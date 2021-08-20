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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoginAccessTokenValidator implements AccessTokenValidator {

    private static final Logger log = LoggerFactory.getLogger(LoginAccessTokenValidator.class);

    private final String scopeClaimName;

    private final String subClaimName;

    public LoginAccessTokenValidator(String scopeClaimName, String subClaimName) {
        this.scopeClaimName = OAuthBearerValidationUtils.validateClaimName(scopeClaimName, "scope");
        this.subClaimName = OAuthBearerValidationUtils.validateClaimName(subClaimName, "sub");
    }

    public OAuthBearerToken validate(String accessToken) throws ValidateException {
        log.debug("validate - accessToken: {}", accessToken);
        accessToken = OAuthBearerValidationUtils.validateAccessToken(accessToken);
        String[] splits = OAuthBearerValidationUtils.validateAccessTokenSplits(accessToken);
        Map<String, Object> payload;

        try {
            payload = OAuthBearerUnsecuredJws.toMap(splits[1]);
        } catch (OAuthBearerIllegalTokenException e) {
            throw new ValidateException(e);
        }

        @SuppressWarnings("unchecked")
        Collection<String> scopeCollection = (Collection<String>) getClaim(payload, scopeClaimName);
        Number expiration = (Number) getClaim(payload, "exp");
        String sub = (String) getClaim(payload, subClaimName);
        Number issuedAt = (Number) getClaim(payload, "iat");

        Set<String> scopes = OAuthBearerValidationUtils.validateScopes(scopeCollection);
        long lifetimeMs = OAuthBearerValidationUtils.validateLifetimeMs(expiration != null ? expiration.longValue() * 1000L : null);
        String principalName = OAuthBearerValidationUtils.validatePrincipalName(sub);
        Long startTimeMs = OAuthBearerValidationUtils.validateStartTimeMs(issuedAt != null ? issuedAt.longValue() * 1000L : null);

        OAuthBearerToken token = new BasicOAuthBearerToken(accessToken,
            scopes,
            lifetimeMs,
            principalName,
            startTimeMs);

        log.debug("validate - token: {}", token);

        return token;
    }

    private Object getClaim(Map<String, Object> payload, String claimName) {
        Object value = payload.get(claimName);
        log.debug("getClaim - {}: {}", claimName, value);
        return value;
    }

}
