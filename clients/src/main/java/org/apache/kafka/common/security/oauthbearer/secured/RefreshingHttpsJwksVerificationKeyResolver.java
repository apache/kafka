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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.IOException;
import java.security.Key;
import java.util.List;
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

    private final RefreshingHttpsJwks refreshingHttpsJwks;

    private final VerificationJwkSelector verificationJwkSelector;

    private boolean isInitialized;

    public RefreshingHttpsJwksVerificationKeyResolver(RefreshingHttpsJwks refreshingHttpsJwks) {
        this.refreshingHttpsJwks = refreshingHttpsJwks;
        this.verificationJwkSelector = new VerificationJwkSelector();
    }

    @Override
    public void init() throws IOException {
        try {
            log.debug("init started");

            refreshingHttpsJwks.init();
        } finally {
            isInitialized = true;

            log.debug("init completed");
        }
    }

    @Override
    public void close() {
        try {
            log.debug("close started");

            refreshingHttpsJwks.close();
        } finally {
            log.debug("close completed");
        }
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        if (!isInitialized)
            throw new IllegalStateException("Please call init() first");

        try {
            List<JsonWebKey> jwks = refreshingHttpsJwks.getJsonWebKeys();
            JsonWebKey jwk = verificationJwkSelector.select(jws, jwks);

            if (jwk != null)
                return jwk.getKey();

            String keyId = jws.getKeyIdHeaderValue();

            if (refreshingHttpsJwks.maybeExpediteRefresh(keyId))
                log.debug("Refreshing JWKs from {} as no suitable verification key for JWS w/ header {} was found in {}", refreshingHttpsJwks.getLocation(), jws.getHeaders().getFullHeaderAsJsonString(), jwks);

            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
            sb.append(" from JWKs ").append(jwks).append(" obtained from ").append(
                refreshingHttpsJwks.getLocation());
            throw new UnresolvableKeyException(sb.toString());
        } catch (JoseException | IOException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
            sb.append(" due to an unexpected exception (").append(e).append(") while obtaining or using keys from JWKS endpoint at ").append(
                refreshingHttpsJwks.getLocation());
            throw new UnresolvableKeyException(sb.toString(), e);
        }
    }

}
