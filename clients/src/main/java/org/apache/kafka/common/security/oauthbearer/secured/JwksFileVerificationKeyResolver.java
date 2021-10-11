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
import java.nio.file.Path;
import java.security.Key;
import java.util.List;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>JwksFileVerificationKeyResolver</code> is a {@link VerificationKeyResolver} implementation
 * that will load the JWKS from the given file system directory.
 *
 * @see org.apache.kafka.common.config.SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URI
 * @see VerificationKeyResolver
 */

public class JwksFileVerificationKeyResolver implements CloseableVerificationKeyResolver {

    private static final Logger log = LoggerFactory.getLogger(JwksFileVerificationKeyResolver.class);

    private final Path jwksFile;

    private VerificationKeyResolver delegate;

    public JwksFileVerificationKeyResolver(Path jwksFile) {
        this.jwksFile = jwksFile;
    }

    @Override
    public void init() throws IOException {
        log.debug("Starting creation of new VerificationKeyResolver from {}", jwksFile);
        String json = Utils.readFileAsString(jwksFile.toFile().getPath());

        JsonWebKeySet jwks;

        try {
            jwks = new JsonWebKeySet(json);
        } catch (JoseException e) {
            throw new IOException(e);
        }

        delegate = new JwksVerificationKeyResolver(jwks.getJsonWebKeys());
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        if (delegate == null)
            throw new UnresolvableKeyException("VerificationKeyResolver delegate is null; please call init() first");

        return delegate.resolveKey(jws, nestingContext);
    }

}
