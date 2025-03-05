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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.oauthbearer.HttpRequestFormatter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JwtBearerRequestFormatter implements HttpRequestFormatter {

    public static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    static final String TOKEN_SIGNING_ALGORITHM_RS256 = "RS256";
    static final String TOKEN_SIGNING_ALGORITHM_ES256 = "ES256";

    private final Time time;
    private final String privateKeyId;
    private final String privateKeySecret;
    private final String tokenSigningAlgo;
    private final String tokenSubject;
    private final String tokenIssuer;
    private final String tokenAudience;
    private final String tokenTargetAudience;

    public JwtBearerRequestFormatter(Time time,
                                     String privateKeyId,
                                     String privateKeySecret,
                                     String tokenSigningAlgo,
                                     String tokenSubject,
                                     String tokenIssuer,
                                     String tokenAudience,
                                     String tokenTargetAudience) {
        this.time = time;
        this.privateKeyId = privateKeyId;
        this.privateKeySecret = privateKeySecret;
        this.tokenSigningAlgo = tokenSigningAlgo;
        this.tokenSubject = tokenSubject;
        this.tokenIssuer = tokenIssuer;
        this.tokenAudience = tokenAudience;
        this.tokenTargetAudience = tokenTargetAudience;
    }

    @Override
    public String formatBody() {
        String assertion;

        try {
            assertion = createAssertion();
        } catch (Exception e) {
            throw new KafkaException("Error signing assertion with private key", e);
        }

        String encodedGrantType = URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8);
        String encodedAssertion = URLEncoder.encode(assertion, StandardCharsets.UTF_8);
        return String.format("grant_type=%s&assertion=%s", encodedGrantType, encodedAssertion);
    }

    @Override
    public Map<String, String> formatHeaders() {
        return Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded");
    }

    String createAssertion() throws IOException, GeneralSecurityException {
        ObjectMapper mapper = new ObjectMapper();
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String header = encodeHeader(mapper, encoder);
        String payload = encodePayload(mapper, encoder);
        String content = header + "." + payload;
        PrivateKey privateKey = getPrivateKey();
        String signedContent = sign(privateKey, content);
        return content + "." + signedContent;
    }

    PrivateKey getPrivateKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
        byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(privateKeySecret);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    String encodeHeader(ObjectMapper mapper, Base64.Encoder encoder) throws IOException {
        Map<String, Object> values = new HashMap<>();
        values.put("alg", tokenSigningAlgo);
        values.put("typ", "JWT");
        values.put("kid", privateKeyId);

        String json = mapper.writeValueAsString(values);
        return encoder.encodeToString(Utils.utf8(json));
    }

    String encodePayload(ObjectMapper mapper, Base64.Encoder encoder) throws IOException {
        long currentTimeSecs = time.milliseconds() / 1000L;
        long expirationSecs = currentTimeSecs + Duration.ofMinutes(60).toSeconds();

        Map<String, Object> values = new HashMap<>();
        values.put("iss", tokenIssuer);
        values.put("sub", tokenSubject);
        values.put("aud", tokenAudience);
        values.put("iat", currentTimeSecs);
        values.put("exp", expirationSecs);
        values.put("target_audience", tokenTargetAudience);

        String json = mapper.writeValueAsString(values);
        return encoder.encodeToString(Utils.utf8(json));
    }

    Signature getSignature() throws NoSuchAlgorithmException {
        if (tokenSigningAlgo.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_RS256)) {
            return Signature.getInstance("SHA256withRSA");
        } else if (tokenSigningAlgo.equalsIgnoreCase(TOKEN_SIGNING_ALGORITHM_ES256)) {
            return Signature.getInstance("SHA256withECDSA");
        } else {
            throw new NoSuchAlgorithmException(String.format("Unsupported signing algorithm: %s", tokenSigningAlgo));
        }
    }

    String sign(PrivateKey privateKey, String contentToSign) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException {
        Signature signature = getSignature();
        signature.initSign(privateKey);
        signature.update(contentToSign.getBytes(StandardCharsets.UTF_8));
        byte[] signedContent = signature.sign();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signedContent);
    }
}
