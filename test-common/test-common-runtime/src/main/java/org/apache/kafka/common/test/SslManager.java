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
package org.apache.kafka.common.test;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;

public class SslManager {

    private static final Logger log = LoggerFactory.getLogger(SslManager.class);
    private File keyStoreFile;
    private final File trustStoreFile;

    public SslManager() {
        try {
            trustStoreFile = org.apache.kafka.test.TestUtils.tempFile("kafka.server.truststore", ".jks");
        } catch (IOException e) {
            throw new RuntimeException("Failed to create truststore file", e);
        }
    }

    public Map<String, Object> createSslConfig() {
        try {
            Map<String, Object> config = new TestSslUtils.SslConfigsBuilder(ConnectionMode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
            keyStoreFile = new File((String) config.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
            log.info("Created unified SSL config - KeyStore: {}, TrustStore: {}", keyStoreFile.getPath(), trustStoreFile.getPath());
            return config;
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Failed to create SSL config", e);
        }
    }

    public Map<String, Object> createClientSslConfig() {
        try {
            return new TestSslUtils.SslConfigsBuilder(ConnectionMode.CLIENT)
                .useExistingTrustStore(trustStoreFile)
                .build();
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Failed to create client SSL config", e);
        }
    }

    public void close() throws IOException {
        if (keyStoreFile != null) {
            Utils.delete(keyStoreFile);
        }
        if (trustStoreFile != null) {
            Utils.delete(trustStoreFile);
        }
    }
}
