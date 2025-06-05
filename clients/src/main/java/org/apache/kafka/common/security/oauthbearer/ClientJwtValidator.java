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

import org.apache.kafka.common.security.oauthbearer.internals.secured.BasicOAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ClaimValidationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SerializedJwt;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;

/**
 * {@code ClientJwtValidator} is an implementation of {@link JwtValidator} that is used
 * by the client to perform some rudimentary validation of the JWT access token that is received
 * as part of the response from posting the client credentials to the OAuth/OIDC provider's
 * token endpoint.
 *
 * The validation steps performed are:
 *
 * <ol>
 *     <li>
 *         Basic structural validation of the <code>b64token</code> value as defined in
 *         <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
 *     </li>
 *     <li>Basic conversion of the token into an in-memory map</li>
 *     <li>Presence of <code>scope</code>, <code>exp</code>, <code>subject</code>, and <code>iat</code> claims</li>
 * </ol>
 */

public class ClientJwtValidator implements JwtValidator {

    private static final Logger log = LoggerFactory.getLogger(ClientJwtValidator.class);

    public static final String EXPIRATION_CLAIM_NAME = "exp";

    public static final String ISSUED_AT_CLAIM_NAME = "iat";

    private String scopeClaimName;

    private String subClaimName;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        this.scopeClaimName = ClaimValidationUtils.validateClaimNameOverride(
            DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
            cu.get(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME)
        );
        this.subClaimName = ClaimValidationUtils.validateClaimNameOverride(
            DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME,
            cu.get(SASL_OAUTHBEARER_SUB_CLAIM_NAME)
        );
    }

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param accessToken Non-<code>null</code> JWT access token
     * @return {@link OAuthBearerToken}
     * @throws JwtValidatorException Thrown on errors performing validation of given token
     */

    @SuppressWarnings("unchecked")
    public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
        SerializedJwt serializedJwt = new SerializedJwt(accessToken);
        Map<String, Object> payload;

        try {
            payload = OAuthBearerUnsecuredJws.toMap(serializedJwt.getPayload());
        } catch (OAuthBearerIllegalTokenException e) {
            throw new JwtValidatorException(String.format("Could not validate the access token: %s", e.getMessage()), e);
        }

        Object scopeRaw = getClaim(payload, scopeClaimName);
        Collection<String> scopeRawCollection;

        if (scopeRaw instanceof String)
            scopeRawCollection = Collections.singletonList((String) scopeRaw);
        else if (scopeRaw instanceof Collection)
            scopeRawCollection = (Collection<String>) scopeRaw;
        else
            scopeRawCollection = Collections.emptySet();

        Number expirationRaw = (Number) getClaim(payload, EXPIRATION_CLAIM_NAME);
        String subRaw = (String) getClaim(payload, subClaimName);
        Number issuedAtRaw = (Number) getClaim(payload, ISSUED_AT_CLAIM_NAME);

        Set<String> scopes = ClaimValidationUtils.validateScopes(scopeClaimName, scopeRawCollection);
        long expiration = ClaimValidationUtils.validateExpiration(EXPIRATION_CLAIM_NAME,
            expirationRaw != null ? expirationRaw.longValue() * 1000L : null);
        String subject = ClaimValidationUtils.validateSubject(subClaimName, subRaw);
        Long issuedAt = ClaimValidationUtils.validateIssuedAt(ISSUED_AT_CLAIM_NAME,
            issuedAtRaw != null ? issuedAtRaw.longValue() * 1000L : null);

        return new BasicOAuthBearerToken(accessToken,
            scopes,
            expiration,
            subject,
            issuedAt);
    }

    private Object getClaim(Map<String, Object> payload, String claimName) {
        Object value = payload.get(claimName);
        log.debug("getClaim - {}: {}", claimName, value);
        return value;
    }

}
