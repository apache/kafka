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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.jose4j.http.Get;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.VerificationJwkSelector;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.security.auth.login.AppConfigurationEntry;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_JWKS_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.requireConfigured;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.validateUrl;

/**
 * <code>RefreshingHttpsJwksVerificationKeyResolver</code> is a
 * {@link VerificationKeyResolver} implementation that will periodically refresh the
 * JWKS using its {@link HttpsJwks} instance.
 *
 * A <a href="https://datatracker.ietf.org/doc/html/rfc7517#section-5">JWKS (JSON Web Key Set)</a>
 * is a JSON document provided by the OAuth/OIDC provider that lists the keys used to sign the JWTs
 * it issues.
 *
 * Here is a sample JWKS JSON document:
 *
 * <pre>
 * {
 *   "keys": [
 *     {
 *       "kty": "RSA",
 *       "alg": "RS256",
 *       "kid": "abc123",
 *       "use": "sig",
 *       "e": "AQAB",
 *       "n": "..."
 *     },
 *     {
 *       "kty": "RSA",
 *       "alg": "RS256",
 *       "kid": "def456",
 *       "use": "sig",
 *       "e": "AQAB",
 *       "n": "..."
 *     }
 *   ]
 * }
 * </pre>
 *
 * Without going into too much detail, the array of keys enumerates the key data that the provider
 * is using to sign the JWT. The key ID (<code>kid</code>) is referenced by the JWT's header in
 * order to match up the JWT's signing key with the key in the JWKS. During the validation step of
 * the broker, the jose4j OAuth library will use the contents of the appropriate key in the JWKS
 * to validate the signature.
 *
 * Given that the JWKS is referenced by the JWT, the JWKS must be made available by the
 * OAuth/OIDC provider so that a JWT can be validated.
 *
 * @see CloseableVerificationKeyResolver
 * @see VerificationKeyResolver
 * @see RefreshingHttpsJwks
 * @see HttpsJwks
 */

public class RefreshingHttpsJwksVerificationKeyResolver implements CloseableVerificationKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwksVerificationKeyResolver.class);

    private final Time time;
    private final VerificationJwkSelector verificationJwkSelector;
    private Optional<SslResource> sslResource;

    private RefreshingHttpsJwks refreshingHttpsJwks;
    public RefreshingHttpsJwksVerificationKeyResolver() {
        this(Time.SYSTEM);
    }

    public RefreshingHttpsJwksVerificationKeyResolver(Time time) {
        this.time = time;
        this.verificationJwkSelector = new VerificationJwkSelector();
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        OAuthBearerConfig config = new OAuthBearerConfig(configs, saslMechanism);
        long refreshIntervalMs = config.getLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS);
        OAuthBearerJaasConfig jaasConfig = new OAuthBearerJaasConfig(saslMechanism, jaasConfigEntries);

        URL jwksEndpointUrl = validateUrl(config, SASL_OAUTHBEARER_JWKS_ENDPOINT_URL);

        sslResource = OAuthBearerUtils.maybeCreateSslResource(jwksEndpointUrl, jaasConfig);

        HttpsJwks httpsJwks = new HttpsJwks(jwksEndpointUrl.toString());
        httpsJwks.setDefaultCacheDuration(refreshIntervalMs);

        if (sslResource.isPresent()) {
            Get get = new Get();
            get.setSslSocketFactory(sslResource.get().sslContext().getSocketFactory());
            httpsJwks.setSimpleHttpGet(get);
        }

        refreshingHttpsJwks = new RefreshingHttpsJwks(
            time,
            httpsJwks,
            refreshIntervalMs,
            config.getLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS),
            config.getLong(SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS)
        );
        refreshingHttpsJwks.configure(configs, saslMechanism, jaasConfigEntries);
    }

    @Override
    public void close() {
        Utils.closeQuietly(refreshingHttpsJwks, "HTTPS JWKS");
        sslResource.ifPresent(r -> Utils.closeQuietly(r, "SSL resource"));
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        requireConfigured(refreshingHttpsJwks, () -> "HTTPS JWKS", getClass());

        try {
            List<JsonWebKey> jwks = refreshingHttpsJwks.getJsonWebKeys();
            JsonWebKey jwk = verificationJwkSelector.select(jws, jwks);

            if (jwk != null)
                return jwk.getKey();

            String keyId = jws.getKeyIdHeaderValue();

            if (refreshingHttpsJwks.maybeExpediteRefresh(keyId))
                log.debug("Refreshing JWKs from {} as no suitable verification key for JWS w/ header {} was found in {}", refreshingHttpsJwks.getLocation(), jws.getHeaders().getFullHeaderAsJsonString(), jwks);

            String sb = "Unable to find a suitable verification key for JWS w/ header " + jws.getHeaders().getFullHeaderAsJsonString() +
                    " from JWKs " + jwks + " obtained from " +
                    refreshingHttpsJwks.getLocation();
            throw new UnresolvableKeyException(sb);
        } catch (JoseException | IOException e) {
            String sb = "Unable to find a suitable verification key for JWS w/ header " + jws.getHeaders().getFullHeaderAsJsonString() +
                    " due to an unexpected exception (" + e + ") while obtaining or using keys from JWKS endpoint at " +
                    refreshingHttpsJwks.getLocation();
            throw new UnresolvableKeyException(sb, e);
        }
    }
}
