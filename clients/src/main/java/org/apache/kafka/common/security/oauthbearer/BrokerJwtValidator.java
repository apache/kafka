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
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ConfigurationUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SerializedJwt;
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory;

import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

/**
 * {@code BrokerJwtValidator} is an implementation of {@link JwtValidator} that is used
 * by the broker to perform more extensive validation of the JWT access token that is received
 * from the client, but ultimately from posting the client credentials to the OAuth/OIDC provider's
 * token endpoint.
 *
 * The validation steps performed (primarily by the jose4j library) are:
 *
 * <ol>
 *     <li>
 *         Basic structural validation of the <code>b64token</code> value as defined in
 *         <a href="https://tools.ietf.org/html/rfc6750#section-2.1">RFC 6750 Section 2.1</a>
 *     </li>
 *     <li>
 *         Basic conversion of the token into an in-memory data structure
 *     </li>
 *     <li>
 *         Presence of <code>scope</code>, <code>exp</code>, <code>subject</code>, <code>iss</code>, and
 *         <code>iat</code> claims
 *     </li>
 *     <li>
 *         Signature matching validation against the <code>kid</code> and those provided by
 *         the OAuth/OIDC provider's JWKS
 *     </li>
 * </ol>
 */
public class BrokerJwtValidator implements JwtValidator {

    private static final Logger log = LoggerFactory.getLogger(BrokerJwtValidator.class);

    private final Optional<CloseableVerificationKeyResolver> verificationKeyResolverOpt;

    private JwtConsumer jwtConsumer;

    private String scopeClaimName;

    private String subClaimName;

    /**
     * A public, no-args constructor is necessary for instantiation via configuration.
     */
    public BrokerJwtValidator() {
        this.verificationKeyResolverOpt = Optional.empty();
    }

    /*
     * Package-visible for testing.
     */
    BrokerJwtValidator(CloseableVerificationKeyResolver verificationKeyResolver) {
        this.verificationKeyResolverOpt = Optional.of(verificationKeyResolver);
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        Set<String> expectedAudiences = Set.copyOf(cu.get(SASL_OAUTHBEARER_EXPECTED_AUDIENCE));
        Integer clockSkew = cu.validateInteger(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, false);
        String expectedIssuer = cu.validateString(SASL_OAUTHBEARER_EXPECTED_ISSUER, false);
        String scopeClaimName = cu.validateString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        String subClaimName = cu.validateString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);

        CloseableVerificationKeyResolver verificationKeyResolver = verificationKeyResolverOpt.orElseGet(
            () -> VerificationKeyResolverFactory.get(configs, saslMechanism, jaasConfigEntries)
        );

        final JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

        if (clockSkew != null)
            jwtConsumerBuilder.setAllowedClockSkewInSeconds(clockSkew);

        if (!expectedAudiences.isEmpty())
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
     * @throws JwtValidatorException Thrown on errors performing validation of given token
     */

    @SuppressWarnings("unchecked")
    public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
        SerializedJwt serializedJwt = new SerializedJwt(accessToken);

        JwtContext jwt;

        try {
            jwt = jwtConsumer.process(serializedJwt.getToken());
        } catch (InvalidJwtException e) {
            throw new JwtValidatorException(String.format("Could not validate the access token: %s", e.getMessage()), e);
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

        return new BasicOAuthBearerToken(accessToken,
            scopes,
            expiration,
            sub,
            issuedAt);
    }

    private <T> T getClaim(ClaimSupplier<T> supplier, String claimName) throws JwtValidatorException {
        try {
            T value = supplier.get();
            log.debug("getClaim - {}: {}", claimName, value);
            return value;
        } catch (MalformedClaimException e) {
            throw new JwtValidatorException(String.format("Could not extract the '%s' claim from the access token", claimName), e);
        }
    }

    public interface ClaimSupplier<T> {

        T get() throws MalformedClaimException;

    }
}
