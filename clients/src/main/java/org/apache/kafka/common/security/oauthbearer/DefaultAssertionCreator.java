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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class DefaultAssertionCreator implements AssertionCreator {

    static final String TOKEN_SIGNING_ALGORITHM_RS256 = "RS256";
    static final String TOKEN_SIGNING_ALGORITHM_ES256 = "ES256";

    private final Time time;
    private final String algorithm;
    private final File privateKeyFile;

    public DefaultAssertionCreator(Time time, String algorithm, File privateKeyFile) {
        this.time = time;
        this.algorithm = algorithm;
        this.privateKeyFile = privateKeyFile;
    }

    @Override
    public String create(AssertionJwtTemplate template) throws GeneralSecurityException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String header = encodeHeader(template, mapper, encoder);
        String payload = encodePayload(template, mapper, encoder);
        String content = header + "." + payload;
        PrivateKey privateKey = getPrivateKey();
        String signedContent = sign(privateKey, content);
        return content + "." + signedContent;
    }

    PrivateKey getPrivateKey() throws GeneralSecurityException, IOException {
        String privateKeySecret = Utils.readFileAsString(privateKeyFile.getPath());
        byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(privateKeySecret);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    String encodeHeader(AssertionJwtTemplate template,
                        ObjectMapper mapper,
                        Base64.Encoder encoder) throws IOException {
        Map<String, Object> values = new HashMap<>(template.header());
        values.put("alg", algorithm);
        values.put("typ", "JWT");

        String json = mapper.writeValueAsString(values);
        return encoder.encodeToString(Utils.utf8(json));
    }

    String encodePayload(AssertionJwtTemplate template,
                         ObjectMapper mapper,
                         Base64.Encoder encoder) throws IOException {
        long currentTimeSecs = time.milliseconds() / 1000L;
        long expirationSecs = currentTimeSecs + Duration.ofMinutes(60).toSeconds();

        Map<String, Object> values = new HashMap<>(template.payload());
        values.put("iat", currentTimeSecs);
        values.put("exp", expirationSecs);

        String json = mapper.writeValueAsString(values);
        return encoder.encodeToString(Utils.utf8(json));
    }

    Signature getSignature() throws GeneralSecurityException {
        if (algorithm.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_RS256)) {
            return Signature.getInstance("SHA256withRSA");
        } else if (algorithm.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_ES256)) {
            return Signature.getInstance("SHA256withECDSA");
        } else {
            throw new NoSuchAlgorithmException(String.format("Unsupported signing algorithm: %s", algorithm));
        }
    }

    String sign(PrivateKey privateKey, String contentToSign) throws GeneralSecurityException {
        Signature signature = getSignature();
        signature.initSign(privateKey);
        signature.update(contentToSign.getBytes(StandardCharsets.UTF_8));
        byte[] signedContent = signature.sign();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signedContent);
    }
}
