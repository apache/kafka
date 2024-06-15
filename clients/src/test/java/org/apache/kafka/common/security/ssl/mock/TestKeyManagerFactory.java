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
package org.apache.kafka.common.security.ssl.mock;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestSslUtils.CertificateBuilder;
import org.apache.kafka.test.TestUtils;

public class TestKeyManagerFactory extends KeyManagerFactorySpi {
    public static final String ALGORITHM = "TestAlgorithm";

    @Override
    protected void engineInit(KeyStore keyStore, char[] chars) {

    }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) {

    }

    @Override
    protected KeyManager[] engineGetKeyManagers() {
        return new KeyManager[] {new TestKeyManager()};
    }

    public static class TestKeyManager extends X509ExtendedKeyManager {

        public static String mockTrustStoreFile;
        public static final String ALIAS = "TestAlias";
        private static final String CN = "localhost";
        private static final String SIGNATURE_ALGORITHM = "RSA";
        private final KeyPair keyPair;
        private final X509Certificate certificate;

        protected TestKeyManager() {
            try {
                this.keyPair = TestSslUtils.generateKeyPair(SIGNATURE_ALGORITHM);
                CertificateBuilder certBuilder = new CertificateBuilder();
                this.certificate = certBuilder.generate("CN=" + CN + ", O=A server", this.keyPair);
                Map<String, X509Certificate> certificates = new HashMap<>();
                certificates.put(ALIAS, certificate);
                File trustStoreFile = TestUtils.tempFile("testTrustStore", ".jks");
                mockTrustStoreFile = trustStoreFile.getPath();
                TestSslUtils.createTrustStore(mockTrustStoreFile, new Password(TestSslUtils.TRUST_STORE_PASSWORD), certificates);
            } catch (IOException | GeneralSecurityException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String[] getClientAliases(String s, Principal[] principals) {
            return new String[] {ALIAS};
        }

        @Override
        public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
            return ALIAS;
        }

        @Override
        public String[] getServerAliases(String s, Principal[] principals) {
            return new String[] {ALIAS};
        }

        @Override
        public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
            return ALIAS;
        }

        @Override
        public X509Certificate[] getCertificateChain(String s) {
            return new X509Certificate[] {this.certificate};
        }

        @Override
        public PrivateKey getPrivateKey(String s) {
            return this.keyPair.getPrivate();
        }
    }

}

