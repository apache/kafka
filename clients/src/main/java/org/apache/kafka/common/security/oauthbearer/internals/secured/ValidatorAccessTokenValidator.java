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

import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ValidatorAccessTokenValidator is an implementation of {@link AccessTokenValidator} that is used
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

public class ValidatorAccessTokenValidator implements AccessTokenValidator {

    private static final Logger log = LoggerFactory.getLogger(ValidatorAccessTokenValidator.class);

    private final JwtConsumer jwtConsumer;

    private final String scopeClaimName;

    private final String subClaimName;

    /**
     * Creates a new ValidatorAccessTokenValidator that will be used by the broker for more
     * thorough validation of the JWT.
     *
     * @param clockSkew               The optional value (in seconds) to allow for differences
     *                                between the time of the OAuth/OIDC identity provider and
     *                                the broker. If <code>null</code> is provided, the broker
     *                                and the OAUth/OIDC identity provider are assumed to have
     *                                very close clock settings.
     * @param expectedAudiences       The (optional) set the broker will use to verify that
     *                                the JWT was issued for one of the expected audiences.
     *                                The JWT will be inspected for the standard OAuth
     *                                <code>aud</code> claim and if this value is set, the
     *                                broker will match the value from JWT's <code>aud</code>
     *                                claim to see if there is an <b>exact</b> match. If there is no
     *                                match, the broker will reject the JWT and authentication
     *                                will fail. May be <code>null</code> to not perform any
     *                                check to verify the JWT's <code>aud</code> claim matches any
     *                                fixed set of known/expected audiences.
     * @param expectedIssuer          The (optional) value for the broker to use to verify that
     *                                the JWT was created by the expected issuer. The JWT will
     *                                be inspected for the standard OAuth <code>iss</code> claim
     *                                and if this value is set, the broker will match it
     *                                <b>exactly</b> against what is in the JWT's <code>iss</code>
     *                                claim. If there is no match, the broker will reject the JWT
     *                                and authentication will fail. May be <code>null</code> to not
     *                                perform any check to verify the JWT's <code>iss</code> claim
     *                                matches a specific issuer.
     * @param verificationKeyResolver jose4j-based {@link VerificationKeyResolver} that is used
     *                                to validate the signature matches the contents of the header
     *                                and payload
     * @param scopeClaimName          Name of the scope claim to use; must be non-<code>null</code>
     * @param subClaimName            Name of the subject claim to use; must be
     *                                non-<code>null</code>
     *
     * @see JwtConsumerBuilder
     * @see JwtConsumer
     * @see VerificationKeyResolver
     */

    public ValidatorAccessTokenValidator(Integer clockSkew,
        Set<String> expectedAudiences,
        String expectedIssuer,
        VerificationKeyResolver verificationKeyResolver,
        String scopeClaimName,
        String subClaimName) {
        final JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

        if (clockSkew != null)
            jwtConsumerBuilder.setAllowedClockSkewInSeconds(clockSkew);

        if (expectedAudiences != null && !expectedAudiences.isEmpty())
            jwtConsumerBuilder.setExpectedAudience(expectedAudiences.toArray(new String[0]));

        if (expectedIssuer != null)
            jwtConsumerBuilder.setExpectedIssuer(expectedIssuer);

        this.jwtConsumer = jwtConsumerBuilder
            .setJwsAlgorithmConstraints(DISALLOW_NONE)
            .setRequireExpirationTime()
            .setRequireIssuedAt()
            .setVerificationKeyResolver(verificationKeyResolver)
            .build();
        this.scopeClaimName = scopeClaimName;
        this.subClaimName = subClaimName;
    }

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param accessToken Non-<code>null</code> JWT access token
     * @return {@link OAuthBearerToken}
     * @throws ValidateException Thrown on errors performing validation of given token
     */

    @SuppressWarnings("unchecked")
    public OAuthBearerToken validate(String accessToken) throws ValidateException {
        SerializedJwt serializedJwt = new SerializedJwt(accessToken);

        JwtContext jwt;

        try {
            jwt = jwtConsumer.process(serializedJwt.getToken());
        } catch (InvalidJwtException e) {
            throw new ValidateException(String.format("Could not validate the access token: %s", e.getMessage()), e);
        }

        JwtClaims claims = jwt.getJwtClaims();

        Object scopeRaw = getClaim(() -> claims.getClaimValue(scopeClaimName), scopeClaimName);
        Collection<String> scopeRawCollection;

        if (scopeRaw instanceof String)
            scopeRawCollection = Collections.singletonList((String) scopeRaw);
        else if (scopeRaw instanceof Collection)
            scopeRawCollection = (Collection<String>) scopeRaw;
        else
            scopeRawCollection = Collections.emptySet();

        NumericDate expirationRaw = getClaim(claims::getExpirationTime, ReservedClaimNames.EXPIRATION_TIME);
        String subRaw = getClaim(() -> claims.getStringClaimValue(subClaimName), subClaimName);
        NumericDate issuedAtRaw = getClaim(claims::getIssuedAt, ReservedClaimNames.ISSUED_AT);

        Set<String> scopes = ClaimValidationUtils.validateScopes(scopeClaimName, scopeRawCollection);
        long expiration = ClaimValidationUtils.validateExpiration(ReservedClaimNames.EXPIRATION_TIME,
            expirationRaw != null ? expirationRaw.getValueInMillis() : null);
        String sub = ClaimValidationUtils.validateSubject(subClaimName, subRaw);
        Long issuedAt = ClaimValidationUtils.validateIssuedAt(ReservedClaimNames.ISSUED_AT,
            issuedAtRaw != null ? issuedAtRaw.getValueInMillis() : null);

        OAuthBearerToken token = new BasicOAuthBearerToken(accessToken,
            scopes,
            expiration,
            sub,
            issuedAt);

        return token;
    }

    private <T> T getClaim(ClaimSupplier<T> supplier, String claimName) throws ValidateException {
        try {
            T value = supplier.get();
            log.debug("getClaim - {}: {}", claimName, value);
            return value;
        } catch (MalformedClaimException e) {
            throw new ValidateException(String.format("Could not extract the '%s' claim from the access token", claimName), e);
        }
    }

    public interface ClaimSupplier<T> {

        T get() throws MalformedClaimException;

    }

}
