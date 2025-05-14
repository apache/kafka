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
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.DefaultVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerConfig;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerJwtClaims;
import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SerializedJwt;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateClaimScopes;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

/**
 * {@code BrokerJwtValidator} is an implementation of {@link JwtValidator} that is used
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
 *     <li>
 *         Basic conversion of the token into an in-memory data structure
 *     </li>
 *     <li>
 *         Presence of <code>scope</code>, <code>exp</code>, <code>subject</code>, <code>iss</code>,
 *         and <code>iat</code> claims
 *     </li>
 *     <li>
 *         Signature matching validation against the <code>kid</code> and those provided by
 *         the OAuth/OIDC provider's JWKS
 *     </li>
 * </ol>
 */
public class BrokerJwtValidator implements JwtValidator {

    private final Supplier<CloseableVerificationKeyResolver> verificationKeyResolverSupplier;

    private JwtConsumer jwtConsumer;

    private String scopeClaimName;

    private String subClaimName;

    private CloseableVerificationKeyResolver verificationKeyResolver;

    public BrokerJwtValidator() {
        this.verificationKeyResolverSupplier = DefaultVerificationKeyResolver::new;
    }

    /*
     * Package-visible for testing.
     */
    BrokerJwtValidator(CloseableVerificationKeyResolver verificationKeyResolverSupplier) {
        this.verificationKeyResolverSupplier = () -> verificationKeyResolverSupplier;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig config = new OAuthBearerConfig(configs, saslMechanism);

        scopeClaimName = config.getString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        subClaimName = config.getString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);

        verificationKeyResolver = verificationKeyResolverSupplier.get();
        verificationKeyResolver.configure(configs, saslMechanism, jaasConfigEntries);

        JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();
        config.maybeGetInt(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS).ifPresent(jwtConsumerBuilder::setAllowedClockSkewInSeconds);
        config.maybeGetString(SASL_OAUTHBEARER_EXPECTED_ISSUER).ifPresent(jwtConsumerBuilder::setExpectedIssuer);
        maybeGetExpectedAudiences(config).ifPresent(jwtConsumerBuilder::setExpectedAudience);

        jwtConsumer = jwtConsumerBuilder
            .setJwsAlgorithmConstraints(DISALLOW_NONE)
            .setRequireExpirationTime()
            .setRequireIssuedAt()
            .setVerificationKeyResolver(verificationKeyResolver)
            .build();
    }

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param accessToken Non-<code>null</code> JWT access token
     * @return {@link OAuthBearerToken}
     * @throws JwtValidatorException Thrown on errors performing validation of given token
     */
    public OAuthBearerToken validate(String accessToken) throws JwtValidatorException {
        OAuthBearerUtils.requireConfigured(jwtConsumer, () -> "JWT consumer", getClass());

        SerializedJwt serializedJwt = new SerializedJwt(accessToken);

        OAuthBearerJwtClaims claims;

        try {
            JwtContext jwt = jwtConsumer.process(serializedJwt.getToken());
            claims = new OAuthBearerJwtClaims(jwt.getJwtClaims().getClaimsMap());
        } catch (InvalidJwtException e) {
            throw new JwtValidatorException(String.format("Could not validate the access token: %s", e.getMessage()), e);
        }

        String subject = claims.maybeGetString(subClaimName).orElse(null);
        Long issuedAt = claims.maybeGetNumber(ReservedClaimNames.ISSUED_AT).map(n -> n.longValue() * 1000L).orElse(null);
        Set<String> scopes = validateClaimScopes(claims, scopeClaimName);
        long expiration = claims.getNumber(ReservedClaimNames.EXPIRATION_TIME).longValue() * 1000L;

        return new BasicOAuthBearerToken(
            accessToken,
            scopes,
            expiration,
            subject,
            issuedAt
        );
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(verificationKeyResolver, "Verification key resolver");
    }

    private Optional<String[]> maybeGetExpectedAudiences(OAuthBearerConfig config) {
        if (!config.containsKey(SASL_OAUTHBEARER_EXPECTED_AUDIENCE))
            return Optional.empty();

        List<String> expectedAudiencesList = config.getList(SASL_OAUTHBEARER_EXPECTED_AUDIENCE);
        Set<String> expectedAudiences = Set.copyOf(expectedAudiencesList);

        if (!expectedAudiences.isEmpty())
            return Optional.of(expectedAudiences.toArray(new String[0]));
        else
            return Optional.empty();
    }
}
