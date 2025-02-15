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

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import java.security.Key;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

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

public class BrokerAccessTokenValidator implements AccessTokenValidator {

    private static final Logger log = LoggerFactory.getLogger(BrokerAccessTokenValidator.class);

    /**
     * Because a {@link CloseableVerificationKeyResolver} instance can spawn threads and issue
     * HTTP(S) calls ({@link RefreshingHttpsJwksVerificationKeyResolver}), we only want to create
     * a new instance for each particular set of configuration. Because each set of configuration
     * may have multiple instances, we want to reuse the single instance.
     */

    private static final Map<VerificationKeyResolverKey, CloseableVerificationKeyResolver> VERIFICATION_KEY_RESOLVER_CACHE = new HashMap<>();

    private final Time time;

    protected CloseableVerificationKeyResolver verificationKeyResolver;

    protected JwtConsumer jwtConsumer;

    protected String scopeClaimName;

    protected String subClaimName;

    public BrokerAccessTokenValidator() {
        this(Time.SYSTEM);
    }

    public BrokerAccessTokenValidator(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Map<String, Object> moduleOptions = JaasOptionsUtils.getOptions(saslMechanism, jaasConfigEntries);

        // Here's the logic which keeps our VerificationKeyResolvers down to a single instance.
        synchronized (VERIFICATION_KEY_RESOLVER_CACHE) {
            VerificationKeyResolverKey key = new VerificationKeyResolverKey(configs, moduleOptions);
            verificationKeyResolver = VERIFICATION_KEY_RESOLVER_CACHE.computeIfAbsent(
                key,
                k -> new RefCountingVerificationKeyResolver(new DelegatingVerificationKeyResolver(time))
            );
        }

        configure(verificationKeyResolver, configs, saslMechanism, jaasConfigEntries);
    }

    public void configure(CloseableVerificationKeyResolver verificationKeyResolver,
                   Map<String, ?> configs,
                   String saslMechanism,
                   List<AppConfigurationEntry> jaasConfigEntries) {
        verificationKeyResolver.configure(configs, saslMechanism, jaasConfigEntries);

        ConfigurationUtils cu = new ConfigurationUtils(saslMechanism, configs);
        Set<String> expectedAudiences = null;
        List<String> l = cu.get(SASL_OAUTHBEARER_EXPECTED_AUDIENCE);

        if (l != null)
            expectedAudiences = Set.copyOf(l);

        Integer clockSkew = cu.validateInteger(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS, false);
        String expectedIssuer = cu.validateString(SASL_OAUTHBEARER_EXPECTED_ISSUER, false);
        String scopeClaimName = cu.validateString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        String subClaimName = cu.validateString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);

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

    @Override
    public void close() {
        Utils.closeQuietly(verificationKeyResolver, "verificationKeyResolver");
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

        return new BasicOAuthBearerToken(accessToken,
            scopes,
            expiration,
            sub,
            issuedAt);
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
    /**
     * <code>VkrKey</code> is a simple structure which encapsulates the criteria for different
     * sets of configuration. This will allow us to use this object as a key in a {@link Map}
     * to keep a single instance per key.
     */

    private static class VerificationKeyResolverKey {

        private final Map<String, ?> configs;

        private final Map<String, Object> moduleOptions;

        public VerificationKeyResolverKey(Map<String, ?> configs, Map<String, Object> moduleOptions) {
            this.configs = configs;
            this.moduleOptions = moduleOptions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            VerificationKeyResolverKey that = (VerificationKeyResolverKey) o;
            return configs.equals(that.configs) && moduleOptions.equals(that.moduleOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(configs, moduleOptions);
        }

    }

    /**
     * <code>RefCountingVerificationKeyResolver</code> allows us to share a single
     * {@link CloseableVerificationKeyResolver} instance between multiple
     * {@link AuthenticateCallbackHandler} instances and perform the lifecycle methods the
     * appropriate number of times.
     */

    private static class RefCountingVerificationKeyResolver implements CloseableVerificationKeyResolver {

        private final CloseableVerificationKeyResolver delegate;

        private final AtomicInteger count = new AtomicInteger(0);

        public RefCountingVerificationKeyResolver(CloseableVerificationKeyResolver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
            return delegate.resolveKey(jws, nestingContext);
        }

        @Override
        public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
            if (count.incrementAndGet() == 1)
                delegate.configure(configs, saslMechanism, jaasConfigEntries);
        }

        @Override
        public void close() {
            if (count.decrementAndGet() == 0)
                delegate.close();
        }
    }
}
