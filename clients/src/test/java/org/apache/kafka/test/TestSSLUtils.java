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

import org.apache.kafka.common.config.SSLConfigs;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.clients.CommonClientConfigs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.math.BigInteger;
import javax.net.ssl.TrustManagerFactory;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

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
     * @throws CertificateException thrown if a security error or an IO error ocurred.
     */
    public static X509Certificate generateCertificate(String dn, KeyPair pair,
                                                      int days, String algorithm)
        throws  CertificateException {

        try {
            Security.addProvider(new BouncyCastleProvider());
            AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
            AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
            AsymmetricKeyParameter privateKeyAsymKeyParam = PrivateKeyFactory.createKey(pair.getPrivate().getEncoded());
            SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
            ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId).build(privateKeyAsymKeyParam);
            X500Name name = new X500Name(dn);
            Date from = new Date();
            Date to = new Date(from.getTime() + days * 86400000L);
            BigInteger sn = new BigInteger(64, new SecureRandom());

            X509v1CertificateBuilder v1CertGen = new X509v1CertificateBuilder(name, sn, from, to, name, subPubKeyInfo);
            X509CertificateHolder certificateHolder = v1CertGen.build(sigGen);
            return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder);
        } catch (CertificateException ce) {
            throw ce;
        } catch (Exception e) {
            throw new CertificateException(e);
        }
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
        KeyStore ks = KeyStore.getInstance("JKS");
        try {
            FileInputStream in = new FileInputStream(filename);
            ks.load(in, password.toCharArray());
            in.close();
        } catch (EOFException e) {
            ks = createEmptyKeyStore();
        }
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

    public static Map<String, Object> createSSLConfig(Mode mode, File keyStoreFile, String password, String keyPassword,
                                                      File trustStoreFile, String trustStorePassword) {
        Map<String, Object> sslConfigs = new HashMap<String, Object>();
        sslConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL"); // kafka security protocol
        sslConfigs.put(SSLConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2"); // protocol to create SSLContext

        if (mode == Mode.SERVER || (mode == Mode.CLIENT && keyStoreFile != null)) {
            sslConfigs.put(SSLConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreFile.getPath());
            sslConfigs.put(SSLConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
            sslConfigs.put(SSLConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, TrustManagerFactory.getDefaultAlgorithm());
            sslConfigs.put(SSLConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, password);
            sslConfigs.put(SSLConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        }

        sslConfigs.put(SSLConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFile.getPath());
        sslConfigs.put(SSLConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
        sslConfigs.put(SSLConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        sslConfigs.put(SSLConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, TrustManagerFactory.getDefaultAlgorithm());

        List<String> enabledProtocols  = new ArrayList<String>();
        enabledProtocols.add("TLSv1.2");
        sslConfigs.put(SSLConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);

        return sslConfigs;
    }

    public static  Map<String, Object> createSSLConfig(boolean useClientCert, boolean trustStore, Mode mode, File trustStoreFile, String certAlias)
        throws IOException, GeneralSecurityException {
        Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();
        File keyStoreFile;
        String password;

        if (mode == Mode.SERVER)
            password = "ServerPassword";
        else
            password = "ClientPassword";

        String trustStorePassword = "TrustStorePassword";

        if (useClientCert) {
            keyStoreFile = File.createTempFile("clientKS", ".jks");
            KeyPair cKP = generateKeyPair("RSA");
            X509Certificate cCert = generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA");
            createKeyStore(keyStoreFile.getPath(), password, "client", cKP.getPrivate(), cCert);
            certs.put(certAlias, cCert);
        } else {
            keyStoreFile = File.createTempFile("serverKS", ".jks");
            KeyPair sKP = generateKeyPair("RSA");
            X509Certificate sCert = generateCertificate("CN=localhost, O=server", sKP, 30,
                                                        "SHA1withRSA");
            createKeyStore(keyStoreFile.getPath(), password, password, "server", sKP.getPrivate(), sCert);
            certs.put(certAlias, sCert);
        }

        if (trustStore) {
            createTrustStore(trustStoreFile.getPath(), trustStorePassword, certs);
        }

        Map<String, Object> sslConfig = createSSLConfig(mode, keyStoreFile, password,
                                                        password, trustStoreFile, trustStorePassword);
        return sslConfig;
    }

}
