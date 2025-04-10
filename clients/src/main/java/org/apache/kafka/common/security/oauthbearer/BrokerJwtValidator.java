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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.BasicOAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JwksFileVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefCountingMap;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwks;
import org.apache.kafka.common.security.oauthbearer.internals.secured.RefreshingHttpsJwksVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SerializedJwt;
import org.apache.kafka.common.security.oauthbearer.internals.secured.SslResource;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;
import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.protocolMatches;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerUtils.validateUrl;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

/**
 * Implementation of {@link JwtValidator} that is used
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
 *         Presence of scope, <code>exp</code>, <code>sub</code>, <code>iss</code>, and
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

    /**
     * Because a {@link CloseableVerificationKeyResolver} instance can spawn threads and issue
     * HTTP(S) calls ({@link RefreshingHttpsJwksVerificationKeyResolver}), we only want to create
     * a new instance for each particular set of configuration. Because each set of configuration
     * may have multiple instances, we want to reuse the single instance.
     */
    private static final RefCountingMap<VerificationKeyResolverKey, CloseableVerificationKeyResolver> VERIFICATION_KEY_RESOLVER_CACHE = new RefCountingMap<>();

    private final Time time;

    private CloseableVerificationKeyResolver verificationKeyResolver;

    private Optional<SslResource> sslResource = Optional.empty();

    private JwtConsumer jwtConsumer;

    private String scopeClaimName;

    private String subClaimName;

    public BrokerJwtValidator() {
        this(Time.SYSTEM);
    }

    public BrokerJwtValidator(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        Function<VerificationKeyResolverKey, CloseableVerificationKeyResolver> wrapResolverFn = k -> {
            OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, saslMechanism);
            URL jwksEndpoint = validateUrl(oauthConfig, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);

            if (protocolMatches(jwksEndpoint, "file")) {
                return new JwksFileVerificationKeyResolver(configs, saslMechanism);
            } else {
                HttpsJwks httpsJwks = new HttpsJwks(jwksEndpoint.toString());
                sslResource = OAuthBearerUtils.maybeCreateSslResource(
                    jwksEndpoint,
                    new OAuthBearerJaasConfig(saslMechanism, jaasConfigEntries)
                );
                Optional<SSLContext> sslContext = sslResource.map(SslResource::sslContext);
                long refreshMs = oauthConfig.getLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS);

                if (refreshMs < 0)
                    throw new ConfigException(String.format("The OAuth configuration option %s value must be non-negative", SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS));

                long refreshRetryBackoffMs = oauthConfig.getLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS);
                long refreshRetryBackoffMaxMs = oauthConfig.getLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS);

                RefreshingHttpsJwks refreshingHttpsJwks = new RefreshingHttpsJwks(
                    time,
                    httpsJwks,
                    sslContext,
                    Executors.newSingleThreadScheduledExecutor(),
                    refreshMs,
                    refreshRetryBackoffMs,
                    refreshRetryBackoffMaxMs
                );
                return new RefreshingHttpsJwksVerificationKeyResolver(refreshingHttpsJwks);
            }
        };

        // Here's the logic which keeps our VerificationKeyResolvers down to a single instance.
        VerificationKeyResolverKey key = new VerificationKeyResolverKey(configs, jaasConfigEntries);
        CloseableVerificationKeyResolver resolver = VERIFICATION_KEY_RESOLVER_CACHE.get(
            key,
            wrapResolverFn
        );

        configure(resolver, configs, saslMechanism);
    }

    public void configure(CloseableVerificationKeyResolver verificationKeyResolver,
                          Map<String, ?> configs,
                          String saslMechanism) {
        this.verificationKeyResolver = verificationKeyResolver;

        OAuthBearerConfig oauthConfig = new OAuthBearerConfig(configs, saslMechanism);
        final JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

        if (oauthConfig.containsKey(SASL_OAUTHBEARER_EXPECTED_AUDIENCE)) {
            // It's a bit convoluted turning the optional list of expected audiences into an array for the jose4j API.
            List<String> list = oauthConfig.get(SASL_OAUTHBEARER_EXPECTED_AUDIENCE);

            if (!list.isEmpty()) {
                Set<String> set = Set.copyOf(list);
                String[] array = set.toArray(new String[0]);
                jwtConsumerBuilder.setExpectedAudience(array);
            }
        }

        oauthConfig.maybeGetInt(SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS).ifPresent(jwtConsumerBuilder::setAllowedClockSkewInSeconds);
        oauthConfig.maybeGetString(SASL_OAUTHBEARER_EXPECTED_ISSUER).ifPresent(jwtConsumerBuilder::setExpectedIssuer);

        this.jwtConsumer = jwtConsumerBuilder
            .setJwsAlgorithmConstraints(DISALLOW_NONE)
            .setRequireExpirationTime()
            .setRequireIssuedAt()
            .setVerificationKeyResolver(verificationKeyResolver)
            .build();
        this.scopeClaimName = oauthConfig.getString(SASL_OAUTHBEARER_SCOPE_CLAIM_NAME);
        this.subClaimName = oauthConfig.getString(SASL_OAUTHBEARER_SUB_CLAIM_NAME);
    }

    @Override
    public void close() {
        Utils.closeQuietly(verificationKeyResolver, "verificationKeyResolver");
        sslResource.ifPresent(r -> Utils.closeQuietly(r, "sslResource"));
    }

    /**
     * Accepts an OAuth JWT access token in base-64 encoded format, validates, and returns an
     * OAuthBearerToken.
     *
     * @param jwt Non-<code>null</code> JWT
     * @return {@link OAuthBearerToken}
     * @throws JwtValidatorException Thrown on errors performing validation of given token
     */
    @Override
    @SuppressWarnings("unchecked")
    public OAuthBearerToken validate(String jwt) throws JwtValidatorException {
        SerializedJwt serializedJwt = new SerializedJwt(jwt);

        JwtContext jwtContext;

        try {
            jwtContext = jwtConsumer.process(serializedJwt.getToken());
        } catch (org.jose4j.jwt.consumer.InvalidJwtException e) {
            throw new JwtValidatorException(String.format("Could not validate the access token: %s", e.getMessage()), e);
        }

        JwtClaims claims = jwtContext.getJwtClaims();

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

        Set<String> scopes = OAuthBearerUtils.validateClaimScopes(scopeClaimName, scopeRawCollection);
        long expiration = OAuthBearerUtils.validateClaimExpiration(
            ReservedClaimNames.EXPIRATION_TIME,
            expirationRaw != null ? expirationRaw.getValueInMillis() : null
        );
        String sub = OAuthBearerUtils.validateClaimSubject(subClaimName, subRaw);
        Long issuedAt = OAuthBearerUtils.validateClaimIssuedAt(
            ReservedClaimNames.ISSUED_AT,
            issuedAtRaw != null ? issuedAtRaw.getValueInMillis() : null
        );

        return new BasicOAuthBearerToken(
            jwt,
            scopes,
            expiration,
            sub,
            issuedAt
        );
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
    /**
     * <code>VerificationKeyResolverKey</code> is a simple structure which encapsulates the criteria
     * for different sets of configuration. This will allow us to use this object as a key in a
     * {@link Map} to keep a single instance per key.
     */
    private static class VerificationKeyResolverKey {

        private final Map<String, ?> configs;

        // The equality of two lists cannot be determined with AppConfigurationEntry directly since
        // that class does not implement hashCode() or equals(). So the JAAS options from the
        // AppConfigurationEntry entries are extracted for comparison purposes.
        private final List<Map<String, ?>> jaasOptions;

        public VerificationKeyResolverKey(Map<String, ?> configs, List<AppConfigurationEntry> jaasConfigEntries) {
            this.configs = configs;
            this.jaasOptions = jaasConfigEntries.stream()
                .map(AppConfigurationEntry::getOptions)
                .collect(Collectors.toList());
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
            return configs.equals(that.configs) && jaasOptions.equals(that.jaasOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(configs, jaasOptions);
        }
    }
}
