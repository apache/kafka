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

import org.apache.kafka.common.security.oauthbearer.internals.secured.OAuthBearerTest;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

class FileJwtRetrieverTest extends OAuthBearerTest {

    @AfterEach
    void tearDown() {
        System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
    }

    @Test
    public void testRetrieveCalledBeforeConfigure() throws IOException {
        try (FileJwtRetriever retriever = new FileJwtRetriever()) {

            Assertions.assertThrows(
                    IllegalStateException.class,
                    retriever::retrieve
            );
        }
    }

    @Test
    public void testRetrieveReturnsTokenFromFile() throws Exception {
        String jwtFileContent = createJwt("test");
        String jwtFileURI = TestUtils.tempFile(jwtFileContent).toURI().toString();
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, jwtFileURI);

        try (FileJwtRetriever retriever = new FileJwtRetriever()) {
            retriever.configure(
                    Map.of(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, jwtFileURI),
                    OAUTHBEARER_MECHANISM,
                    Collections.emptyList()
            );

            Assertions.assertEquals(jwtFileContent, retriever.retrieve());
        }
    }

    @Test
    public void testRetrieveThrowsIfFileIsMissing() throws Exception {
        String jwtFileContent = createJwt("test");
        File jwtFile = TestUtils.tempFile(jwtFileContent);
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, jwtFile.toURI().toString());

        try (FileJwtRetriever retriever = new FileJwtRetriever()) {
            retriever.configure(
                    Map.of(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, jwtFile.toURI().toString()),
                    OAUTHBEARER_MECHANISM,
                    Collections.emptyList()
            );
            Files.delete(jwtFile.toPath());

            Assertions.assertThrows(
                    Exception.class,
                    retriever::retrieve
            );
        }
    }
}