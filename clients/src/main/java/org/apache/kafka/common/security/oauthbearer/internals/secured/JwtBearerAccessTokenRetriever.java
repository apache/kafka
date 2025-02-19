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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;

import javax.security.auth.login.AppConfigurationEntry;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JwtBearerAccessTokenRetriever extends HttpAccessTokenRetriever {

    public final static String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    private final Time time;

    private String assertion;

    public JwtBearerAccessTokenRetriever() {
        this(Time.SYSTEM);
    }

    public JwtBearerAccessTokenRetriever(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        super.configure(configs, saslMechanism, jaasConfigEntries);

        JaasOptionsUtils jou = new JaasOptionsUtils(saslMechanism, jaasConfigEntries);
        String privateKeyId = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_ID);
        String privateKeySecret = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_PRIVATE_KEY_SECRET);
        String tokenSigningAlgo = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_SIGNING_ALGO);
        String tokenSubject = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_SUBJECT);
        String tokenIssuer = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_ISSUER);
        String tokenAudience = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_AUDIENCE);
        String tokenTargetAudience = jou.validateString(OAuthBearerConfigs.SASL_OAUTHBEARER_TOKEN_TARGET_AUDIENCE, false);

        try {
            byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(privateKeySecret);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey privateKey = keyFactory.generatePrivate(keySpec);

            AssertionCreator assertionCreator = new AssertionCreator(
                time,
                privateKeyId,
                privateKey,
                tokenSigningAlgo,
                tokenSubject,
                tokenIssuer,
                tokenAudience,
                tokenTargetAudience
            );

            assertion = assertionCreator.create();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException exception) {
            throw new ConfigException(String.format("Error getting private key from secret: %s", exception.getMessage()));
        }
    }

    @Override
    protected byte[] formatRequestBody() {
        String encodedGrantType = URLEncoder.encode(GRANT_TYPE, StandardCharsets.UTF_8);
        String encodedAssertion = URLEncoder.encode(assertion, StandardCharsets.UTF_8);
        String body = String.format("grant_type=%s&assertion=%s", encodedGrantType, encodedAssertion);
        return body.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected Map<String, String> formatRequestHeaders(int contentLength) {
        Map<String, String> headers = new HashMap<>(super.formatRequestHeaders(contentLength));
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        return headers;
    }

    static class AssertionCreator {

        private final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        private final Time time;
        private final String privateKeyId;
        private final PrivateKey privateKey;
        private final String tokenSigningAlgo;
        private final String tokenSubject;
        private final String tokenIssuer;
        private final String tokenAudience;
        private final String tokenTargetAudience;

        public AssertionCreator(Time time,
                                String privateKeyId,
                                PrivateKey privateKey,
                                String tokenSigningAlgo,
                                String tokenSubject,
                                String tokenIssuer,
                                String tokenAudience,
                                String tokenTargetAudience) {
            this.time = time;
            this.privateKeyId = privateKeyId;
            this.privateKey = privateKey;
            this.tokenSigningAlgo = tokenSigningAlgo;
            this.tokenSubject = tokenSubject;
            this.tokenIssuer = tokenIssuer;
            this.tokenAudience = tokenAudience;
            this.tokenTargetAudience = tokenTargetAudience;
        }

        String create() {
            try {
                long currentTimeMs = time.milliseconds() / 1000L;
                ObjectMapper mapper = new ObjectMapper();
                JwtTokenHeader tokenHeader = new JwtTokenHeader(tokenSigningAlgo, "JWT", privateKeyId);
                JwtTokenPayload tokenPayload = new JwtTokenPayload(
                    tokenIssuer,
                    tokenSubject,
                    tokenAudience,
                    currentTimeMs,
                    currentTimeMs + Duration.ofMinutes(60).toSeconds(),
                    tokenTargetAudience
                );
                String tokenHeaderString = mapper.writeValueAsString(tokenHeader);
                String tokenPayloadString = mapper.writeValueAsString(tokenPayload);
                String base64TokenHeader = encoder.encodeToString(tokenHeaderString.getBytes(StandardCharsets.UTF_8));
                String base64TokenPayload = encoder.encodeToString(tokenPayloadString.getBytes(StandardCharsets.UTF_8));

                String contentToSign = base64TokenHeader + "." + base64TokenPayload;
                String signedContent = sign(contentToSign);
                return contentToSign + "." + signedContent;
            } catch (Throwable t) {
                throw new KafkaException("Error signing assertion with private key", t);
            }
        }

        String sign(String contentToSign) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException {
            Signature signatureAlgo = getSignature(tokenSigningAlgo);
            signatureAlgo.initSign(privateKey);
            signatureAlgo.update(contentToSign.getBytes(StandardCharsets.UTF_8));
            byte[] signedContent = signatureAlgo.sign();
            return encoder.encodeToString(signedContent);
        }

        static Signature getSignature(String algorithm) throws NoSuchAlgorithmException {
            if (algorithm.equalsIgnoreCase("RS256")) {
                return Signature.getInstance("SHA256withRSA");
            } else if (algorithm.equalsIgnoreCase("ES256")) {
                return Signature.getInstance("SHA256withECDSA");
            } else {
                throw new NoSuchAlgorithmException(String.format("Unsupported signing algorithm: %s", algorithm));
            }
        }
    }

    public static class JwtTokenHeader {

        private String alg;
        private String typ;
        private String kid;

        public JwtTokenHeader(String alg, String typ, String kid) {
            this.alg = alg;
            this.typ = typ;
            this.kid = kid;
        }

        public String getAlg() {
            return alg;
        }

        public String getTyp() {
            return typ;
        }

        public String getKid() {
            return kid;
        }

        public void setAlg(String alg) {
            this.alg = alg;
        }

        public void setTyp(String typ) {
            this.typ = typ;
        }

        public void setKid(String kid) {
            this.kid = kid;
        }
    }

    public static class JwtTokenPayload {

        private String iss;
        private String sub;
        private String aud;
        private long iat;
        private long exp;
        @JsonProperty("target_audience")
        private String targetAudience;

        public JwtTokenPayload(String iss, String sub, String aud, long iat, long exp, String targetAudience) {
            this.iss = iss;
            this.sub = sub;
            this.aud = aud;
            this.iat = iat;
            this.exp = exp;
            this.targetAudience = targetAudience;
        }

        public String getIss() {
            return iss;
        }

        public String getSub() {
            return sub;
        }

        public String getAud() {
            return aud;
        }

        public long getIat() {
            return iat;
        }

        public long getExp() {
            return exp;
        }

        public String getTargetAudience() {
            return targetAudience;
        }

        public void setIss(String iss) {
            this.iss = iss;
        }

        public void setSub(String sub) {
            this.sub = sub;
        }

        public void setAud(String aud) {
            this.aud = aud;
        }

        public void setIat(long iat) {
            this.iat = iat;
        }

        public void setExp(long exp) {
            this.exp = exp;
        }

        public void setTargetAudience(String targetAudience) {
            this.targetAudience = targetAudience;
        }
    }
}
