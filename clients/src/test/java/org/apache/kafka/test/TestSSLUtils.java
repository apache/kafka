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

import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.network.SSLFactory;
import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateIssuerName;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateSubjectName;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

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
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


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
                                                      int days, String algorithm) throws GeneralSecurityException, IOException {
        PrivateKey privkey = pair.getPrivate();
        X509CertInfo info = new X509CertInfo();
        Date from = new Date();
        Date to = new Date(from.getTime() + days * 86400000L);
        CertificateValidity interval = new CertificateValidity(from, to);
        BigInteger sn = new BigInteger(64, new SecureRandom());
        X500Name owner = new X500Name(dn);

        info.set(X509CertInfo.VALIDITY, interval);
        info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(sn));
        info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner));
        info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner));
        info.set(X509CertInfo.KEY, new CertificateX509Key(pair.getPublic()));
        info
                .set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
        AlgorithmId algo = new AlgorithmId(AlgorithmId.md5WithRSAEncryption_oid);
        info.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algo));

        // Sign the cert to identify the algorithm that's used.
        X509CertImpl cert = new X509CertImpl(info);
        cert.sign(privkey, algorithm);

        // Update the algorith, and resign.
        algo = (AlgorithmId) cert.get(X509CertImpl.SIG_ALG);
        info
                .set(CertificateAlgorithmId.NAME + "." + CertificateAlgorithmId.ALGORITHM,
                        algo);
        cert = new X509CertImpl(info);
        cert.sign(privkey, algorithm);
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

    public static SecurityConfig createSSLConfigFile(SSLFactory.Mode mode, String trustStoreFileClient) throws IOException, GeneralSecurityException {
        Properties securityConfigProps = new Properties();
        Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
        KeyPair keyPair = generateKeyPair("RSA");
        X509Certificate cert = generateCertificate("CN=localhost, O=localhost", keyPair, 30, "SHA1withRSA");
        String password = "test";

        if (mode == SSLFactory.Mode.SERVER) {
            File keyStoreFile = File.createTempFile("keystore", ".jks");
            createKeyStore(keyStoreFile.getPath(), password, password, "localhost", keyPair.getPrivate(), cert);
            certs.put("localhost", cert);
            securityConfigProps.put(SecurityConfig.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreFile.getPath());
            securityConfigProps.put(SecurityConfig.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
            securityConfigProps.put(SecurityConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG, "SunX509");
            securityConfigProps.put(SecurityConfig.SSL_KEYSTORE_PASSWORD_CONFIG, password);
            securityConfigProps.put(SecurityConfig.SSL_KEY_PASSWORD_CONFIG, password);

            File trustStoreFile = File.createTempFile("truststore", ".jks");
            createTrustStore(trustStoreFile.getPath(), password, certs);

            securityConfigProps.put(SecurityConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
            securityConfigProps.put(SecurityConfig.SSL_CLIENT_REQUIRE_CERT_CONFIG, "false");
            securityConfigProps.put(SecurityConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFile.getPath());
            securityConfigProps.put(SecurityConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
            securityConfigProps.put(SecurityConfig.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        } else {
            securityConfigProps.put(SecurityConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
            securityConfigProps.put(SecurityConfig.SSL_CLIENT_REQUIRE_CERT_CONFIG, "false");
            securityConfigProps.put(SecurityConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFileClient);
            securityConfigProps.put(SecurityConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, password);
            securityConfigProps.put(SecurityConfig.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        }

        securityConfigProps.put(SecurityConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, "SunX509");
        return new SecurityConfig(securityConfigProps);
    }

}
