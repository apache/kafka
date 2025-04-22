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
import org.apache.kafka.common.security.oauthbearer.AssertionCreator;
import org.apache.kafka.common.security.oauthbearer.AssertionJwtTemplate;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.function.BiFunction;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.staticCacheRefreshPolicy;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerUtils.sign;

public class DefaultAssertionCreator implements AssertionCreator {

    private static final BiFunction<File, String, PrivateKey> PRIVATE_KEY_TRANSFORMER = (file, privateKeySecret) -> {
        try {
            byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(privateKeySecret);
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(keySpec);
        } catch (GeneralSecurityException e) {
            throw new KafkaException("An error occurred generating the OAuth assertion private key from " + file.getPath(), e);
        }
    };

    private final String algorithm;
    private final CachedFile<PrivateKey> privateKeyFile;

    public DefaultAssertionCreator(String algorithm, File privateKeyFile) {
        this.algorithm = algorithm;
        this.privateKeyFile = new CachedFile<>(
            privateKeyFile,
            PRIVATE_KEY_TRANSFORMER,
            staticCacheRefreshPolicy()
        );
    }

    @Override
    public String create(AssertionJwtTemplate template) throws GeneralSecurityException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String header = encoder.encodeToString(Utils.utf8(mapper.writeValueAsString(template.header())));
        String payload = encoder.encodeToString(Utils.utf8(mapper.writeValueAsString(template.payload())));
        String content = header + "." + payload;
        PrivateKey privateKey = privateKeyFile.transformed();
        String signedContent = sign(algorithm, privateKey, content);
        return content + "." + signedContent;
    }
}
