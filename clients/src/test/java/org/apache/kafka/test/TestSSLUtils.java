/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.test;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.network.SSLFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.InvalidKeyException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.x509.X509V1CertificateGenerator;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;


public class TestSSLUtils {

    /**
     * Create a self-signed X.509 Certificate.
     * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
     *
     * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
     * @param pair the KeyPair
     * @param days how many days from now the Certificate is valid for
     * @param algorithm the signing algorithm, eg "SHA1withRSA"
     * @return the self-signed certificate
     * @throws IOException thrown if an IO error ocurred.
     * @throws GeneralSecurityException thrown if an Security error ocurred.
     */
    public static X509Certificate generateCertificate(String dn, KeyPair pair,
                                                      int days, String algorithm)
        throws CertificateEncodingException, InvalidKeyException, IllegalStateException,
               NoSuchProviderException, NoSuchAlgorithmException, SignatureException {
        Date from = new Date();
        Date to = new Date(from.getTime() + days * 86400000L);
        BigInteger sn = new BigInteger(64, new SecureRandom());
        KeyPair keyPair = pair;
        X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
        X500Principal  dnName = new X500Principal(dn);

        certGen.setSerialNumber(sn);
        certGen.setIssuerDN(dnName);
        certGen.setNotBefore(from);
        certGen.setNotAfter(to);
        certGen.setSubjectDN(dnName);
        certGen.setPublicKey(keyPair.getPublic());
        certGen.setSignatureAlgorithm(algorithm);
        X509Certificate cert = certGen.generate(pair.getPrivate());
        return cert;
    }

    public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
        keyGen.initialize(1024);
        return keyGen.genKeyPair();
    }

    private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null); // initialize
        return ks;
    }

    private static void saveKeyStore(KeyStore ks, String filename,
                                     String password) throws GeneralSecurityException, IOException {
        FileOutputStream out = new FileOutputStream(filename);
        try {
            ks.store(out, password.toCharArray());
        } finally {
            out.close();
        }
    }

    public static void createKeyStore(String filename,
                                      String password, String alias,
                                      Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        ks.setKeyEntry(alias, privateKey, password.toCharArray(),
                new Certificate[]{cert});
        saveKeyStore(ks, filename, password);
    }

    /**
     * Creates a keystore with a single key and saves it to a file.
     *
     * @param filename String file to save
     * @param password String store password to set on keystore
     * @param keyPassword String key password to set on key
     * @param alias String alias to use for the key
     * @param privateKey Key to save in keystore
     * @param cert Certificate to use as certificate chain associated to key
     * @throws GeneralSecurityException for any error with the security APIs
     * @throws IOException if there is an I/O error saving the file
     */
    public static void createKeyStore(String filename,
                                      String password, String keyPassword, String alias,
                                      Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(),
                new Certificate[]{cert});
        saveKeyStore(ks, filename, password);
    }

    public static void createTrustStore(String filename,
                                        String password, String alias,
                                        Certificate cert) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        ks.setCertificateEntry(alias, cert);
        saveKeyStore(ks, filename, password);
    }

    public static <T extends Certificate> void createTrustStore(
            String filename, String password, Map<String, T> certs) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        for (Map.Entry<String, T> cert : certs.entrySet()) {
            ks.setCertificateEntry(cert.getKey(), cert.getValue());
        }
        saveKeyStore(ks, filename, password);
    }

    public static Map<String, X509Certificate> createX509Certificates(KeyPair keyPair)
        throws GeneralSecurityException {
        Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
        X509Certificate cert = generateCertificate("CN=localhost, O=localhost", keyPair, 30, "SHA1withRSA");
        certs.put("localhost", cert);
        return certs;
    }

    public static Map<String, Object> createSSLConfig(SSLFactory.Mode mode, File keyStoreFile, String password, String keyPassword,
                                                      File trustStoreFile, String trustStorePassword, boolean useClientCert) {
        Map<String, Object> sslConfigs = new HashMap<String, Object>();
        sslConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL"); // kafka security protocol
        sslConfigs.put(CommonClientConfigs.SSL_PROTOCOL_CONFIG, "TLS"); // protocol to create SSLContext

        if (mode == SSLFactory.Mode.SERVER || (mode == SSLFactory.Mode.CLIENT && keyStoreFile != null)) {
            sslConfigs.put(CommonClientConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreFile.getPath());
            sslConfigs.put(CommonClientConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
            sslConfigs.put(CommonClientConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, "SunX509");
            sslConfigs.put(CommonClientConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
            sslConfigs.put(CommonClientConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }

        sslConfigs.put(CommonClientConfigs.SSL_CLIENT_REQUIRE_CERT_CONFIG, useClientCert);
        sslConfigs.put(CommonClientConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFile.getPath());
        sslConfigs.put(CommonClientConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
        sslConfigs.put(CommonClientConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        sslConfigs.put(CommonClientConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "SunX509");

        List<String> enabledProtocols  = new ArrayList<String>();
        enabledProtocols.add("TLSv1.2");
        sslConfigs.put(CommonClientConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);

        return sslConfigs;
    }

    public static Map<SSLFactory.Mode, Map<String, ?>> createSSLConfigs(boolean useClientCert, boolean trustStore)
        throws IOException, GeneralSecurityException {
        Map<SSLFactory.Mode, Map<String, ?>> sslConfigs = new HashMap<SSLFactory.Mode, Map<String, ?>>();
        Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        File clientKeyStoreFile = null;
        File serverKeyStoreFile = File.createTempFile("serverKS", ".jks");
        String clientPassword = "ClientPassword";
        String serverPassword = "ServerPassword";
        String trustStorePassword = "TrustStorePassword";

        if (useClientCert) {
            clientKeyStoreFile = File.createTempFile("clientKS", ".jks");
            KeyPair cKP = generateKeyPair("RSA");
            X509Certificate cCert = generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA");
            createKeyStore(clientKeyStoreFile.getPath(), clientPassword, "client", cKP.getPrivate(), cCert);
            certs.put("client", cCert);
        }

        KeyPair sKP = generateKeyPair("RSA");
        X509Certificate sCert = generateCertificate("CN=localhost, O=server", sKP, 30,
                                                    "SHA1withRSA");
        createKeyStore(serverKeyStoreFile.getPath(), serverPassword, serverPassword, "server", sKP.getPrivate(), sCert);
        certs.put("server", sCert);

        if (trustStore) {
            createTrustStore(trustStoreFile.getPath(), trustStorePassword, certs);
        }

        Map<String, Object> clientSSLConfig = createSSLConfig(SSLFactory.Mode.CLIENT, clientKeyStoreFile, clientPassword,
                                                              clientPassword, trustStoreFile, trustStorePassword, useClientCert);
        Map<String, Object> serverSSLConfig = createSSLConfig(SSLFactory.Mode.SERVER, serverKeyStoreFile, serverPassword,
                                                              serverPassword, trustStoreFile, trustStorePassword, useClientCert);
        sslConfigs.put(SSLFactory.Mode.CLIENT, clientSSLConfig);
        sslConfigs.put(SSLFactory.Mode.SERVER, serverSSLConfig);
        return sslConfigs;
    }

}
