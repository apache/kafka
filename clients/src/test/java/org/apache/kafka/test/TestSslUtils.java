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
package org.apache.kafka.test;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.network.Mode;

import java.io.File;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TestSslUtils {

    public static final String TRUST_STORE_PASSWORD = "TrustStorePassword";
    public static final String DEFAULT_TLS_PROTOCOL_FOR_TESTS = SslConfigs.DEFAULT_SSL_PROTOCOL;

    /**
     * Create a self-signed X.509 Certificate.
     * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
     *
     * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
     * @param pair the KeyPair
     * @param days how many days from now the Certificate is valid for
     * @param algorithm the signing algorithm, eg "SHA1withRSA"
     * @return the self-signed certificate
     * @throws CertificateException thrown if a security error or an IO error occurred.
     */
    public static X509Certificate generateCertificate(String dn, KeyPair pair,
                                                      int days, String algorithm)
        throws  CertificateException {
        return new CertificateBuilder(days, algorithm).generate(dn, pair);
    }

    public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
        keyGen.initialize(2048);
        return keyGen.genKeyPair();
    }

    private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null); // initialize
        return ks;
    }

    private static void saveKeyStore(KeyStore ks, String filename,
                                     Password password) throws GeneralSecurityException, IOException {
        try (OutputStream out = Files.newOutputStream(Paths.get(filename))) {
            ks.store(out, password.value().toCharArray());
        }
    }

    public static void createKeyStore(String filename,
                                      Password password, String alias,
                                      Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        ks.setKeyEntry(alias, privateKey, password.value().toCharArray(),
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
                                      Password password, Password keyPassword, String alias,
                                      Key privateKey, Certificate cert) throws GeneralSecurityException, IOException {
        KeyStore ks = createEmptyKeyStore();
        ks.setKeyEntry(alias, privateKey, keyPassword.value().toCharArray(),
                new Certificate[]{cert});
        saveKeyStore(ks, filename, password);
    }

    public static <T extends Certificate> void createTrustStore(
            String filename, Password password, Map<String, T> certs) throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance("JKS");
        try (InputStream in = Files.newInputStream(Paths.get(filename))) {
            ks.load(in, password.value().toCharArray());
        } catch (EOFException e) {
            ks = createEmptyKeyStore();
        }
        for (Map.Entry<String, T> cert : certs.entrySet()) {
            ks.setCertificateEntry(cert.getKey(), cert.getValue());
        }
        saveKeyStore(ks, filename, password);
    }

    public static Map<String, Object> createSslConfig(String keyManagerAlgorithm, String trustManagerAlgorithm, String tlsProtocol) {
        Map<String, Object> sslConfigs = new HashMap<>();
        sslConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsProtocol); // protocol to create SSLContext

        sslConfigs.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, keyManagerAlgorithm);
        sslConfigs.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, trustManagerAlgorithm);

        List<String> enabledProtocols  = new ArrayList<>();
        enabledProtocols.add(tlsProtocol);
        sslConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);

        return sslConfigs;
    }

    public static  Map<String, Object> createSslConfig(boolean useClientCert, boolean trustStore, Mode mode, File trustStoreFile, String certAlias)
        throws IOException, GeneralSecurityException {
        return createSslConfig(useClientCert, trustStore, mode, trustStoreFile, certAlias, "localhost");
    }

    public static  Map<String, Object> createSslConfig(boolean useClientCert, boolean trustStore,
            Mode mode, File trustStoreFile, String certAlias, String cn)
        throws IOException, GeneralSecurityException {
        return createSslConfig(useClientCert, trustStore, mode, trustStoreFile, certAlias, cn, new CertificateBuilder());
    }

    public static  Map<String, Object> createSslConfig(boolean useClientCert, boolean createTrustStore,
            Mode mode, File trustStoreFile, String certAlias, String cn, CertificateBuilder certBuilder)
            throws IOException, GeneralSecurityException {
        SslConfigsBuilder builder = new SslConfigsBuilder(mode)
                .useClientCert(useClientCert)
                .certAlias(certAlias)
                .cn(cn)
                .certBuilder(certBuilder);
        if (createTrustStore)
            builder = builder.createNewTrustStore(trustStoreFile);
        else
            builder = builder.useExistingTrustStore(trustStoreFile);
        return builder.build();
    }

    public static class CertificateBuilder {
        private final int days;
        private final String algorithm;
        private byte[] subjectAltName;

        public CertificateBuilder() {
            this(30, "SHA1withRSA");
        }

        public CertificateBuilder(int days, String algorithm) {
            this.days = days;
            this.algorithm = algorithm;
        }

        public CertificateBuilder sanDnsName(String hostName) throws IOException {
            subjectAltName = new GeneralNames(new GeneralName(GeneralName.dNSName, hostName)).getEncoded();
            return this;
        }

        public CertificateBuilder sanIpAddress(InetAddress hostAddress) throws IOException {
            subjectAltName = new GeneralNames(new GeneralName(GeneralName.iPAddress, new DEROctetString(hostAddress.getAddress()))).getEncoded();
            return this;
        }

        public X509Certificate generate(String dn, KeyPair keyPair) throws CertificateException {
            try {
                Security.addProvider(new BouncyCastleProvider());
                AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
                AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
                AsymmetricKeyParameter privateKeyAsymKeyParam = PrivateKeyFactory.createKey(keyPair.getPrivate().getEncoded());
                SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
                ContentSigner sigGen = new BcRSAContentSignerBuilder(sigAlgId, digAlgId).build(privateKeyAsymKeyParam);
                X500Name name = new X500Name(dn);
                Date from = new Date();
                Date to = new Date(from.getTime() + days * 86400000L);
                BigInteger sn = new BigInteger(64, new SecureRandom());
                X509v3CertificateBuilder v3CertGen = new X509v3CertificateBuilder(name, sn, from, to, name, subPubKeyInfo);

                if (subjectAltName != null)
                    v3CertGen.addExtension(Extension.subjectAlternativeName, false, subjectAltName);
                X509CertificateHolder certificateHolder = v3CertGen.build(sigGen);
                return new JcaX509CertificateConverter().setProvider("BC").getCertificate(certificateHolder);
            } catch (CertificateException ce) {
                throw ce;
            } catch (Exception e) {
                throw new CertificateException(e);
            }
        }
    }

    public static class SslConfigsBuilder {
        final Mode mode;
        String tlsProtocol;
        boolean useClientCert;
        boolean createTrustStore;
        File trustStoreFile;
        Password trustStorePassword;
        File keyStoreFile;
        Password keyStorePassword;
        Password keyPassword;
        String certAlias;
        String cn;
        CertificateBuilder certBuilder;

        public SslConfigsBuilder(Mode mode) {
            this.mode = mode;
            this.tlsProtocol = DEFAULT_TLS_PROTOCOL_FOR_TESTS;
            trustStorePassword = new Password(TRUST_STORE_PASSWORD);
            keyStorePassword = mode == Mode.SERVER ? new Password("ServerPassword") : new Password("ClientPassword");
            keyPassword = keyStorePassword;
            this.certBuilder = new CertificateBuilder();
            this.cn = "localhost";
            this.certAlias = mode.name().toLowerCase(Locale.ROOT);
        }

        public SslConfigsBuilder tlsProtocol(String tlsProtocol) {
            this.tlsProtocol = tlsProtocol;
            return this;
        }

        public SslConfigsBuilder createNewTrustStore(File trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            this.createTrustStore = true;
            return this;
        }

        public SslConfigsBuilder useExistingTrustStore(File trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            this.createTrustStore = false;
            return this;
        }

        public SslConfigsBuilder createNewKeyStore(File keyStoreFile) {
            this.keyStoreFile = keyStoreFile;
            return this;
        }

        public SslConfigsBuilder useClientCert(boolean useClientCert) {
            this.useClientCert = useClientCert;
            return this;
        }

        public SslConfigsBuilder certAlias(String certAlias) {
            this.certAlias = certAlias;
            return this;
        }

        public SslConfigsBuilder cn(String cn) {
            this.cn = cn;
            return this;
        }

        public SslConfigsBuilder certBuilder(CertificateBuilder certBuilder) {
            this.certBuilder = certBuilder;
            return this;
        }

        public  Map<String, Object> build() throws IOException, GeneralSecurityException {
            Map<String, X509Certificate> certs = new HashMap<>();
            File keyStoreFile = null;

            if (mode == Mode.CLIENT && useClientCert) {
                keyStoreFile = File.createTempFile("clientKS", ".jks");
                KeyPair cKP = generateKeyPair("RSA");
                X509Certificate cCert = certBuilder.generate("CN=" + cn + ", O=A client", cKP);
                createKeyStore(keyStoreFile.getPath(), keyStorePassword, "client", cKP.getPrivate(), cCert);
                certs.put(certAlias, cCert);
                keyStoreFile.deleteOnExit();
            } else if (mode == Mode.SERVER) {
                keyStoreFile = File.createTempFile("serverKS", ".jks");
                KeyPair sKP = generateKeyPair("RSA");
                X509Certificate sCert = certBuilder.generate("CN=" + cn + ", O=A server", sKP);
                createKeyStore(keyStoreFile.getPath(), keyStorePassword, keyStorePassword, "server", sKP.getPrivate(), sCert);
                certs.put(certAlias, sCert);
                keyStoreFile.deleteOnExit();
            }

            if (createTrustStore) {
                createTrustStore(trustStoreFile.getPath(), trustStorePassword, certs);
                trustStoreFile.deleteOnExit();
            }

            Map<String, Object> sslConfigs = new HashMap<>();

            sslConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsProtocol); // protocol to create SSLContext

            if (mode == Mode.SERVER || (mode == Mode.CLIENT && keyStoreFile != null)) {
                sslConfigs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreFile.getPath());
                sslConfigs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
                sslConfigs.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, TrustManagerFactory.getDefaultAlgorithm());
                sslConfigs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                sslConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
            }

            sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreFile.getPath());
            sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
            sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
            sslConfigs.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, TrustManagerFactory.getDefaultAlgorithm());

            List<String> enabledProtocols  = new ArrayList<>();
            enabledProtocols.add(tlsProtocol);
            sslConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);

            return sslConfigs;
        }
    }

    public static final class TestSslEngineFactory implements SslEngineFactory {

        DefaultSslEngineFactory defaultSslEngineFactory = new DefaultSslEngineFactory();

        @Override
        public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
            return defaultSslEngineFactory.createClientSslEngine(peerHost, peerPort, endpointIdentification);
        }

        @Override
        public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
            return defaultSslEngineFactory.createServerSslEngine(peerHost, peerPort);
        }

        @Override
        public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
            return defaultSslEngineFactory.shouldBeRebuilt(nextConfigs);
        }

        @Override
        public Set<String> reconfigurableConfigs() {
            return defaultSslEngineFactory.reconfigurableConfigs();
        }

        @Override
        public KeyStore keystore() {
            return defaultSslEngineFactory.keystore();
        }

        @Override
        public KeyStore truststore() {
            return defaultSslEngineFactory.truststore();
        }

        @Override
        public void close() throws IOException {
            defaultSslEngineFactory.close();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            defaultSslEngineFactory.configure(configs);
        }
    }
}
