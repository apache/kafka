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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.jose4j.jwa.AlgorithmConstraints.ConstraintType;
import org.jose4j.jws.AlgorithmIdentifiers;
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

public class ValidatorAccessTokenValidator implements AccessTokenValidator {

    private static final Logger log = LoggerFactory.getLogger(ValidatorAccessTokenValidator.class);

    private final JwtConsumer jwtConsumer;

    private final String scopeClaimName;

    private final String subClaimName;

    public ValidatorAccessTokenValidator(Integer clockSkew,
        Set<String> expectedAudiences,
        String expectedIssuer,
        VerificationKeyResolver verificationKeyResolver,
        String scopeClaimName,
        String subClaimName) {
        final JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

        if (clockSkew != null)
            jwtConsumerBuilder.setAllowedClockSkewInSeconds(clockSkew);

        if (expectedAudiences != null && expectedAudiences.isEmpty())
            jwtConsumerBuilder.setExpectedAudience(expectedAudiences.toArray(new String[0]));

        if (expectedIssuer != null)
            jwtConsumerBuilder.setExpectedIssuer(expectedIssuer);

        this.jwtConsumer = jwtConsumerBuilder
            .setJwsAlgorithmConstraints(ConstraintType.PERMIT, AlgorithmIdentifiers.RSA_USING_SHA256)
            .setRequireExpirationTime()
            .setRequireIssuedAt()
            .setRequireSubject()
            .setVerificationKeyResolver(verificationKeyResolver)
            .build();
        this.scopeClaimName = scopeClaimName;
        this.subClaimName = subClaimName;
    }

    public OAuthBearerToken validate(String accessToken) throws ValidateException {
        log.debug("validate - accessToken: {}", accessToken);

        JwtContext jwt;

        try {
            jwt = jwtConsumer.process(accessToken);
        } catch (InvalidJwtException e) {
            throw new ValidateException("Could not process the access token", e);
        }

        JwtClaims claims = jwt.getJwtClaims();
        List<String> scopes = getClaim(() -> claims.getStringListClaimValue(scopeClaimName), scopeClaimName);
        NumericDate expiration = getClaim(claims::getExpirationTime, ReservedClaimNames.EXPIRATION_TIME);
        String sub = getClaim(() -> claims.getStringClaimValue(subClaimName), subClaimName);
        NumericDate issuedAt = getClaim(claims::getIssuedAt, ReservedClaimNames.ISSUED_AT);

        OAuthBearerToken token = new BasicOAuthBearerToken(accessToken,
            scopes != null && !scopes.isEmpty() ? new HashSet<>(scopes) : Collections.emptySet(),
            expiration != null ? expiration.getValueInMillis() : null,
            sub,
            issuedAt != null ? issuedAt.getValueInMillis() : null);

        log.debug("validate - token: {}", token);

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
