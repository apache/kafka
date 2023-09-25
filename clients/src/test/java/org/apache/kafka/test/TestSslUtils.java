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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.bouncycastle.asn1.ASN1EncodableVector;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.DERT61String;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x500.AttributeTypeAndValue;
import org.bouncycastle.asn1.x500.RDN;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PKCS8Generator;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcContentSignerBuilder;
import org.bouncycastle.operator.bc.BcDSAContentSignerBuilder;
import org.bouncycastle.operator.bc.BcECContentSignerBuilder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemWriter;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import static org.apache.kafka.common.security.ssl.DefaultSslEngineFactory.PEM_TYPE;

public class TestSslUtils {

    public static final String TRUST_STORE_PASSWORD = "TrustStorePassword";
    public static final String DEFAULT_TLS_PROTOCOL_FOR_TESTS = SslConfigs.DEFAULT_SSL_PROTOCOL;

    /**
     * Create a self-signed X.509 Certificate.
     * From http://bfo.com/blog/2011/03/08/odds_and_ends_creating_a_new_x_509_certificate.html.
     *
     * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
     * @param pair the KeyPair
     * @param days how many days from now the Certificate is valid for, or - for negative values - how many days before now
     * @param algorithm the signing algorithm, eg "SHA1withRSA"
     * @return the self-signed certificate
     * @throws CertificateException thrown if a security error or an IO error occurred.
     */
    public static X509Certificate generateCertificate(String dn, KeyPair pair,
                                                      int days, String algorithm)
        throws  CertificateException {
        return new CertificateBuilder(days, algorithm).generate(dn, pair);
    }

    /**
     * Generate a signed certificate. Self-signed, if no issuer and parentKeyPair are supplied
     * 
     * @param dn The distinguished name of this certificate
     * @param keyPair A key pair
     * @param daysBeforeNow how many days before now the Certificate is valid for
     * @param daysAfterNow how many days from now the Certificate is valid for
     * @param issuer The issuer who signs the certificate. Leave null if you want to generate a root
     *        CA.
     * @param parentKeyPair The key pair of the issuer. Leave null if you want to generate a root
     *        CA.
     * @param algorithm the signing algorithm, eg "SHA1withRSA"
     * @return the signed certificate
     * @throws CertificateException
     */
    public static X509Certificate generateSignedCertificate(String dn, KeyPair keyPair,
            int daysBeforeNow, int daysAfterNow, String issuer, KeyPair parentKeyPair,
            String algorithm, boolean isCA, boolean isServerCert, boolean isClientCert) throws CertificateException {
        return new CertificateBuilder(0, algorithm).generateSignedCertificate(dn, keyPair,
                daysBeforeNow, daysAfterNow, issuer, parentKeyPair, isCA, isServerCert, isClientCert);
    }

    public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
        keyGen.initialize(algorithm.equals("EC") ? 256 : 2048);
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

    public static void convertToPem(Map<String, Object> sslProps, boolean writeToFile, boolean encryptPrivateKey) throws Exception {
        String tsPath = (String) sslProps.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        String tsType = (String) sslProps.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        Password tsPassword = (Password) sslProps.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        Password trustCerts = (Password) sslProps.remove(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG);
        if (trustCerts == null && tsPath != null) {
            trustCerts = exportCertificates(tsPath, tsPassword, tsType);
        }
        if (trustCerts != null) {
            if (tsPath == null) {
                tsPath = TestUtils.tempFile("truststore", ".pem").getPath();
                sslProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsPath);
            }
            sslProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PEM_TYPE);
            if (writeToFile)
                writeToFile(tsPath, trustCerts);
            else {
                sslProps.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, trustCerts);
                sslProps.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            }
        }

        String ksPath = (String) sslProps.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        Password certChain = (Password) sslProps.remove(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG);
        Password key = (Password) sslProps.remove(SslConfigs.SSL_KEYSTORE_KEY_CONFIG);
        if (certChain == null && ksPath != null) {
            String ksType = (String) sslProps.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
            Password ksPassword = (Password) sslProps.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
            Password keyPassword = (Password) sslProps.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
            certChain = exportCertificates(ksPath, ksPassword, ksType);
            Password pemKeyPassword = encryptPrivateKey ? keyPassword : null;
            key = exportPrivateKey(ksPath, ksPassword, keyPassword, ksType, pemKeyPassword);
            if (!encryptPrivateKey)
                sslProps.remove(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        }

        if (certChain != null) {
            if (ksPath == null) {
                ksPath = TestUtils.tempFile("keystore", ".pem").getPath();
                sslProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksPath);
            }
            sslProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PEM_TYPE);
            if (writeToFile)
                writeToFile(ksPath, key, certChain);
            else {
                sslProps.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, key);
                sslProps.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certChain);
                sslProps.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
            }
        }
    }

    private static void writeToFile(String path, Password... entries) throws IOException {
        try (FileOutputStream out = new FileOutputStream(path)) {
            for (Password entry: entries) {
                out.write(entry.value().getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    public static void convertToPemWithoutFiles(Properties sslProps) throws Exception {
        String tsPath = sslProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        if (tsPath != null) {
            Password trustCerts = exportCertificates(tsPath,
                    (Password) sslProps.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
                    sslProps.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
            sslProps.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
            sslProps.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
            sslProps.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PEM_TYPE);
            sslProps.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, trustCerts);
        }
        String ksPath = sslProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        if (ksPath != null) {
            String ksType = sslProps.getProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
            Password ksPassword = (Password) sslProps.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
            Password keyPassword = (Password) sslProps.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
            Password certChain = exportCertificates(ksPath, ksPassword, ksType);
            Password key = exportPrivateKey(ksPath, ksPassword, keyPassword, ksType, keyPassword);
            sslProps.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
            sslProps.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
            sslProps.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PEM_TYPE);
            sslProps.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certChain);
            sslProps.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, key);
        }
    }

    public static Password exportCertificates(String storePath, Password storePassword, String storeType) throws Exception {
        StringBuilder builder = new StringBuilder();
        try (FileInputStream in = new FileInputStream(storePath)) {
            KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(in, storePassword.value().toCharArray());
            Enumeration<String> aliases = ks.aliases();
            if (!aliases.hasMoreElements())
                throw new IllegalArgumentException("No certificates found in file " + storePath);
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                Certificate[] certs = ks.getCertificateChain(alias);
                if (certs != null) {
                    for (Certificate cert : certs) {
                        builder.append(pem(cert));
                    }
                } else {
                    builder.append(pem(ks.getCertificate(alias)));
                }
            }
        }
        return new Password(builder.toString());
    }

    public static Password exportPrivateKey(String storePath,
                                            Password storePassword,
                                            Password keyPassword,
                                            String storeType,
                                            Password pemKeyPassword) throws Exception {
        try (FileInputStream in = new FileInputStream(storePath)) {
            KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(in, storePassword.value().toCharArray());
            String alias = ks.aliases().nextElement();
            return new Password(pem((PrivateKey) ks.getKey(alias, keyPassword.value().toCharArray()), pemKeyPassword));
        }
    }

    static String pem(Certificate cert) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(cert));
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    static String pem(PrivateKey privateKey, Password password) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            if (password == null) {
                pemWriter.writeObject(new JcaPKCS8Generator(privateKey, null));
            } else {
                JceOpenSSLPKCS8EncryptorBuilder encryptorBuilder = new JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.PBE_SHA1_3DES);
                encryptorBuilder.setPassword(password.value().toCharArray());
                try {
                    pemWriter.writeObject(new JcaPKCS8Generator(privateKey, encryptorBuilder.build()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
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

        public CertificateBuilder sanDnsNames(String... hostNames) throws IOException {
            if (hostNames.length > 0) {
                GeneralName[] altNames = new GeneralName[hostNames.length];
                for (int i = 0; i < hostNames.length; i++)
                    altNames[i] = new GeneralName(GeneralName.dNSName, hostNames[i]);
                subjectAltName = GeneralNames.getInstance(new DERSequence(altNames)).getEncoded();
            } else {
                subjectAltName = null;
            }
            return this;
        }

        public CertificateBuilder sanIpAddress(InetAddress hostAddress) throws IOException {
            subjectAltName = new GeneralNames(new GeneralName(GeneralName.iPAddress, new DEROctetString(hostAddress.getAddress()))).getEncoded();
            return this;
        }

        public X509Certificate generate(String dn, KeyPair keyPair) throws CertificateException {
            return generate(new X500Name(dn), keyPair);
        }

        public X509Certificate generate(String commonName, String org, boolean utf8, KeyPair keyPair) throws CertificateException {
            RDN[] rdns = new RDN[2];
            rdns[0] = new RDN(new AttributeTypeAndValue(BCStyle.CN, utf8 ? new DERUTF8String(commonName) : new DERT61String(commonName)));
            rdns[1] = new RDN(new AttributeTypeAndValue(BCStyle.O, utf8 ? new DERUTF8String(org) : new DERT61String(org)));
            return generate(new X500Name(rdns), keyPair);
        }

        public X509Certificate generate(X500Name dn, KeyPair keyPair) throws CertificateException {
            try {
                Security.addProvider(new BouncyCastleProvider());
                AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
                AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
                AsymmetricKeyParameter privateKeyAsymKeyParam = PrivateKeyFactory.createKey(keyPair.getPrivate().getEncoded());
                SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
                BcContentSignerBuilder signerBuilder;
                String keyAlgorithm = keyPair.getPublic().getAlgorithm();
                if (keyAlgorithm.equals("RSA"))
                    signerBuilder = new BcRSAContentSignerBuilder(sigAlgId, digAlgId);
                else if (keyAlgorithm.equals("DSA"))
                    signerBuilder = new BcDSAContentSignerBuilder(sigAlgId, digAlgId);
                else if (keyAlgorithm.equals("EC"))
                    signerBuilder = new BcECContentSignerBuilder(sigAlgId, digAlgId);
                else
                    throw new IllegalArgumentException("Unsupported algorithm " + keyAlgorithm);
                ContentSigner sigGen = signerBuilder.build(privateKeyAsymKeyParam);
                // Negative numbers for "days" can be used to generate expired certificates
                Date now = new Date();
                Date from = (days >= 0) ? now : new Date(now.getTime() + days * 86400000L);
                Date to = (days >= 0) ? new Date(now.getTime() + days * 86400000L) : now;
                BigInteger sn = new BigInteger(64, new SecureRandom());
                X509v3CertificateBuilder v3CertGen = new X509v3CertificateBuilder(dn, sn, from, to, dn, subPubKeyInfo);

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
        
        /**
         * @param dn The distinguished name to use
         * @param keyPair A key pair to use
         * @param daysBeforeNow how many days before now the Certificate is valid for
         * @param daysAfterNow how many days from now the Certificate is valid for
         * @param issuer The issuer name. if null, "dn" is used
         * @param parentKeyPair The parent key pair used to sign this certificate. If null, create
         *        self-signed certificate authority (CA)
         * @return A (self-) signed certificate
         * @throws CertificateException
         */
        public X509Certificate generateSignedCertificate(String dn, KeyPair keyPair,
                int daysBeforeNow, int daysAfterNow, String issuer, KeyPair parentKeyPair, boolean isCA, boolean isServerCert, boolean isClientCert)
                throws CertificateException {
            X500Name issuerOrDn = (issuer != null) ? new X500Name(issuer) : new X500Name(dn);
            return generateSignedCertificate(new X500Name(dn), keyPair, daysBeforeNow, daysAfterNow,
                    issuerOrDn, parentKeyPair, isCA, isServerCert, isClientCert);
        }

        /**
         * 
         * @param dn The distinguished name to use
         * @param keyPair A key pair to use
         * @param daysBeforeNow how many days before now the Certificate is valid for
         * @param daysAfterNow how many days from now the Certificate is valid for
         * @param issuer The issuer name. if null, "dn" is used
         * @param parentKeyPair The parent key pair used to sign this certificate. If null, create
         *        self-signed certificate authority (CA)
         * @return A (self-) signed certificate
         * @throws CertificateException
         */
        public X509Certificate generateSignedCertificate(X500Name dn, KeyPair keyPair,
                int daysBeforeNow, int daysAfterNow, X500Name issuer, KeyPair parentKeyPair, boolean isCA, boolean isServerCert, boolean isClientCert)
                throws CertificateException {
            try {
                Security.addProvider(new BouncyCastleProvider());
                AlgorithmIdentifier sigAlgId =
                        new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
                AlgorithmIdentifier digAlgId =
                        new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
                // Create self-signed certificate if no parentKeyPair has been specified, otherwise
                // sign with private key of parentKeyPair
                KeyPair signingKeyPair = (parentKeyPair != null) ? parentKeyPair : keyPair;
                AsymmetricKeyParameter privateKeyAsymKeyParam =
                        PrivateKeyFactory.createKey(signingKeyPair.getPrivate().getEncoded());
                SubjectPublicKeyInfo subPubKeyInfo =
                        SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
                BcContentSignerBuilder signerBuilder;
                String keyAlgorithm = keyPair.getPublic().getAlgorithm();
                if (keyAlgorithm.equals("RSA"))
                    signerBuilder = new BcRSAContentSignerBuilder(sigAlgId, digAlgId);
                else if (keyAlgorithm.equals("DSA"))
                    signerBuilder = new BcDSAContentSignerBuilder(sigAlgId, digAlgId);
                else if (keyAlgorithm.equals("EC"))
                    signerBuilder = new BcECContentSignerBuilder(sigAlgId, digAlgId);
                else
                    throw new IllegalArgumentException("Unsupported algorithm " + keyAlgorithm);
                ContentSigner sigGen = signerBuilder.build(privateKeyAsymKeyParam);
                // Negative numbers for "days" can be used to generate expired certificates
                Date now = new Date();
                Date from = new Date(now.getTime() - daysBeforeNow * 86400000L);
                Date to = new Date(now.getTime() + daysAfterNow * 86400000L);
                BigInteger sn = new BigInteger(64, new SecureRandom());
                X500Name issuerOrDn = (issuer != null) ? issuer : dn;
                X509v3CertificateBuilder v3CertGen =
                        new X509v3CertificateBuilder(issuerOrDn, sn, from, to, dn, subPubKeyInfo);
                if (isCA) {
                    v3CertGen.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));
                }
                if (isServerCert || isClientCert) {
                    ASN1EncodableVector purposes = new ASN1EncodableVector();
                    if (isServerCert) {
                        purposes.add(KeyPurposeId.id_kp_serverAuth);
                    }
                    if (isClientCert) {
                        purposes.add(KeyPurposeId.id_kp_clientAuth);
                    }
                    v3CertGen.addExtension(Extension.extendedKeyUsage, false, new DERSequence(purposes));
                }
                if (subjectAltName != null) {
                    v3CertGen.addExtension(Extension.subjectAlternativeName, false, subjectAltName);
                }
                X509CertificateHolder certificateHolder = v3CertGen.build(sigGen);
                return new JcaX509CertificateConverter().setProvider("BC")
                        .getCertificate(certificateHolder);
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
        Password keyStorePassword;
        Password keyPassword;
        String certAlias;
        String cn;
        String algorithm;
        CertificateBuilder certBuilder;
        boolean usePem;

        public SslConfigsBuilder(Mode mode) {
            this.mode = mode;
            this.tlsProtocol = DEFAULT_TLS_PROTOCOL_FOR_TESTS;
            trustStorePassword = new Password(TRUST_STORE_PASSWORD);
            keyStorePassword = mode == Mode.SERVER ? new Password("ServerPassword") : new Password("ClientPassword");
            keyPassword = keyStorePassword;
            this.certBuilder = new CertificateBuilder();
            this.cn = "localhost";
            this.certAlias = mode.name().toLowerCase(Locale.ROOT);
            this.algorithm = "RSA";
            this.createTrustStore = true;
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

        public SslConfigsBuilder algorithm(String algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        public SslConfigsBuilder certBuilder(CertificateBuilder certBuilder) {
            this.certBuilder = certBuilder;
            return this;
        }

        public SslConfigsBuilder usePem(boolean usePem) {
            this.usePem = usePem;
            return this;
        }

        public  Map<String, Object> build() throws IOException, GeneralSecurityException {
            if (usePem) {
                return buildPem();
            } else
                return buildJks();
        }

        private Map<String, Object> buildJks() throws IOException, GeneralSecurityException {
            Map<String, X509Certificate> certs = new HashMap<>();
            File keyStoreFile = null;

            if (mode == Mode.CLIENT && useClientCert) {
                keyStoreFile = TestUtils.tempFile("clientKS", ".jks");
                KeyPair cKP = generateKeyPair(algorithm);
                X509Certificate cCert = certBuilder.generate("CN=" + cn + ", O=A client", cKP);
                createKeyStore(keyStoreFile.getPath(), keyStorePassword, keyPassword, "client", cKP.getPrivate(), cCert);
                certs.put(certAlias, cCert);
            } else if (mode == Mode.SERVER) {
                keyStoreFile = TestUtils.tempFile("serverKS", ".jks");
                KeyPair sKP = generateKeyPair(algorithm);
                X509Certificate sCert = certBuilder.generate("CN=" + cn + ", O=A server", sKP);
                createKeyStore(keyStoreFile.getPath(), keyStorePassword, keyPassword, "server", sKP.getPrivate(), sCert);
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

        private Map<String, Object> buildPem() throws IOException, GeneralSecurityException {
            if (!createTrustStore) {
                throw new IllegalArgumentException("PEM configs cannot be created with existing trust stores");
            }

            Map<String, Object> sslConfigs = new HashMap<>();
            sslConfigs.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsProtocol);
            sslConfigs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Collections.singletonList(tlsProtocol));

            if (mode != Mode.CLIENT || useClientCert) {
                KeyPair keyPair = generateKeyPair(algorithm);
                X509Certificate cert = certBuilder.generate("CN=" + cn + ", O=A " + mode.name().toLowerCase(Locale.ROOT), keyPair);

                Password privateKeyPem = new Password(pem(keyPair.getPrivate(), keyPassword));
                Password certPem = new Password(pem(cert));
                sslConfigs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, PEM_TYPE);
                sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, PEM_TYPE);
                sslConfigs.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, privateKeyPem);
                sslConfigs.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certPem);
                sslConfigs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
                sslConfigs.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, certPem);
            }
            return sslConfigs;
        }
    }

    public static final class TestSslEngineFactory implements SslEngineFactory {

        public boolean closed = false;

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
            closed = true;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            defaultSslEngineFactory.configure(configs);
        }
    }
}
