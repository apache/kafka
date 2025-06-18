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
package org.apache.kafka.common.security.oauthbearer.internals.secured.assertion;

import org.apache.kafka.common.security.oauthbearer.JwtRetrieverException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Optional;

import static org.apache.kafka.common.security.oauthbearer.internals.secured.CachedFile.RefreshPolicy.lastModifiedPolicy;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.privateKey;
import static org.apache.kafka.common.security.oauthbearer.internals.secured.assertion.AssertionUtils.sign;

/**
 * This is the "default" {@link AssertionCreator} in that it is the common case of using a configured signing
 * algorithm, private key file, and optional passphrase to sign a JWT to dynamically create an assertion.
 *
 * <p/>
 *
 * The provided private key file will be cached in memory but will be refreshed when the file changes.
 * <em>Note</em>: there is not yet a facility to reload the configured passphrase. If using a private key
 * passphrase, either use the same passphrase for each private key or else restart the client/application
 * so that the new private key and passphrase will be used.
 */
public class DefaultAssertionCreator implements AssertionCreator {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private final String algorithm;
    private final CachedFile<PrivateKey> privateKeyFile;

    public DefaultAssertionCreator(String algorithm, File privateKeyFile, Optional<String> passphrase) {
        this.algorithm = algorithm;

        this.privateKeyFile = new CachedFile<>(
            privateKeyFile,
            new PrivateKeyTransformer(passphrase),
            lastModifiedPolicy()
        );
    }

    @Override
    public String create(AssertionJwtTemplate template) throws GeneralSecurityException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        String header = BASE64_ENCODER.encodeToString(Utils.utf8(mapper.writeValueAsString(template.header())));
        String payload = BASE64_ENCODER.encodeToString(Utils.utf8(mapper.writeValueAsString(template.payload())));
        String content = header + "." + payload;
        PrivateKey privateKey = privateKeyFile.transformed();
        String signedContent = sign(algorithm, privateKey, content);
        return content + "." + signedContent;
    }

    private static class PrivateKeyTransformer implements CachedFile.Transformer<PrivateKey> {

        private final Optional<String> passphrase;

        public PrivateKeyTransformer(Optional<String> passphrase) {
            this.passphrase = passphrase;
        }

        @Override
        public PrivateKey transform(File file, String contents) {
            try {
                contents = contents.replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replace("\n", "");

                return privateKey(contents.getBytes(StandardCharsets.UTF_8), passphrase);
            } catch (GeneralSecurityException | IOException e) {
                throw new JwtRetrieverException("An error occurred generating the OAuth assertion private key from " + file.getPath(), e);
            }
        }
    }
}
