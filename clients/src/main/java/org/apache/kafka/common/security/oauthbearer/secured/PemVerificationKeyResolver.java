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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.Key;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.utils.Utils;
import org.jose4j.jwk.EllipticCurveJsonWebKey;
import org.jose4j.jwk.HttpsJwks;
import org.jose4j.jwk.JsonWebKey;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.keys.EcKeyUtil;
import org.jose4j.keys.RsaKeyUtil;
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver;
import org.jose4j.keys.resolvers.VerificationKeyResolver;
import org.jose4j.lang.JoseException;
import org.jose4j.lang.UnresolvableKeyException;

/**
 * <code>PemVerificationKeyResolver</code> is a
 * {@link VerificationKeyResolver} implementation that will periodically refresh the
 * JWKS from the given file system directory.
 *
 * This class is a wrapper around the <code>RefreshingHttpsJwks</code> to expose the
 * {@link #init()} and {@link #close()} lifecycle methods.
 *
 * @see ValidatorCallbackHandlerConfiguration#JWKS_FILE_CONFIG
 * @see VerificationKeyResolver
 * @see RefreshingHttpsJwks
 * @see HttpsJwks
 */

public class PemVerificationKeyResolver extends DelegatedFileUpdate<VerificationKeyResolver> implements CloseableVerificationKeyResolver {

    private static final String PEM_SUFFIX = ".pem";

    public PemVerificationKeyResolver(Path path) {
        super(path);
    }

    public static String toKid(File f) {
        return f.getName().replace(PEM_SUFFIX, "");
    }

    @Override
    public Key resolveKey(JsonWebSignature jws, List<JsonWebStructure> nestingContext) throws UnresolvableKeyException {
        VerificationKeyResolver localDelegate = retrieveDelegate();

        if (localDelegate == null)
            throw new UnresolvableKeyException("VerificationKeyResolver delegate is null");

        return localDelegate.resolveKey(jws, nestingContext);
    }

    protected VerificationKeyResolver createDelegate() throws IOException {
        log.debug("Starting creation of new VerificationKeyResolver from *{} files in {}", PEM_SUFFIX, path);
        File[] files = path.toFile().listFiles((dir, name) -> name.endsWith(PEM_SUFFIX));
        List<JsonWebKey> jsonWebKeys = new ArrayList<>();

        if (files != null) {
            RsaKeyUtil rsaKeyUtil = new RsaKeyUtil();
            EcKeyUtil ecKeyUtil = new EcKeyUtil();

            for (File f : files) {
                JsonWebKey jwk;

                try {
                    log.debug("Reading PEM file from {}", f.getAbsolutePath());
                    String pemEncoded = Utils.readFileAsString(f.getAbsolutePath());

                    try {
                        PublicKey publicKey = rsaKeyUtil.fromPemEncoded(pemEncoded);
                        jwk = new RsaJsonWebKey((RSAPublicKey) publicKey);
                    } catch (InvalidKeySpecException e) {
                        PublicKey publicKey = ecKeyUtil.fromPemEncoded(pemEncoded);
                        jwk = new EllipticCurveJsonWebKey((ECPublicKey) publicKey);
                    }
                } catch (StringIndexOutOfBoundsException e) {
                    throw new IOException(String.format("Error creating public key from file %s due to malformed contents", f), e);
                } catch (JoseException | InvalidKeySpecException e) {
                    throw new IOException(String.format("Error creating public key from file %s", f), e);
                }

                jwk.setKeyId(toKid(f));
                jsonWebKeys.add(jwk);
            }
        }

        if (jsonWebKeys.isEmpty())
            log.warn("No PEM files found in {}", path);

        return new JwksVerificationKeyResolver(jsonWebKeys);
    }

}
