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
import javax.net.ssl.SSLSocketFactory;
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

/**
 * <code>RefreshingHttpsJwksVerificationKeyResolver</code> is a
 * {@link VerificationKeyResolver} implementation that will periodically refresh the
 * JWKS using its {@link HttpsJwks} instance.
 *
 * @see CloseableVerificationKeyResolver
 * @see VerificationKeyResolver
 * @see RefreshingHttpsJwks
 * @see HttpsJwks
 */

public class RefreshingHttpsJwksVerificationKeyResolver implements CloseableVerificationKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(RefreshingHttpsJwksVerificationKeyResolver.class);

    private final RefreshingHttpsJwks httpsJwks;

    private final VerificationJwkSelector verificationJwkSelector;

    private boolean isInitialized;

    public RefreshingHttpsJwksVerificationKeyResolver(String location, long refreshMs, SSLSocketFactory sslSocketFactory) {
        this.httpsJwks = new RefreshingHttpsJwks(location, refreshMs);

        if (sslSocketFactory != null) {
            Get get = new Get();
            get.setSslSocketFactory(sslSocketFactory);
            this.httpsJwks.setSimpleHttpGet(get);
        }

        this.verificationJwkSelector = new VerificationJwkSelector();
    }

    @Override
    public void init() throws IOException {
        try {
            log.debug("init started");

            httpsJwks.init();
        } finally {
            isInitialized = true;

            log.debug("init completed");
        }
    }

    @Override
    public void close() {
        try {
            log.debug("close started");

            httpsJwks.close();
        } finally {
            log.debug("close completed");
        }
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        if (!isInitialized)
            throw new IllegalStateException("Please call init() first");

        try {
            List<JsonWebKey> jwks = httpsJwks.getJsonWebKeys();
            JsonWebKey jwk = verificationJwkSelector.select(jws, jwks);

            if (jwk != null)
                return jwk.getKey();

            String keyId = jws.getKeyIdHeaderValue();

            if (httpsJwks.maybeScheduleRefreshForMissingKeyId(keyId))
                log.debug("Refreshing JWKs from {} as no suitable verification key for JWS w/ header {} was found in {}", httpsJwks.getLocation(), jws.getHeaders().getFullHeaderAsJsonString(), jwks);

            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
            sb.append(" from JWKs ").append(jwks).append(" obtained from ").append(httpsJwks.getLocation());
            throw new UnresolvableKeyException(sb.toString());
        } catch (JoseException | IOException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find a suitable verification key for JWS w/ header ").append(jws.getHeaders().getFullHeaderAsJsonString());
            sb.append(" due to an unexpected exception (").append(e).append(") while obtaining or using keys from JWKS endpoint at ").append(httpsJwks.getLocation());
            throw new UnresolvableKeyException(sb.toString(), e);
        }
    }

}
