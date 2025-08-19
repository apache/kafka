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

import org.apache.kafka.common.config.types.Password;

import org.bouncycastle.asn1.ASN1EncodableVector;
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
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcContentSignerBuilder;
import org.bouncycastle.operator.bc.BcDSAContentSignerBuilder;
import org.bouncycastle.operator.bc.BcECContentSignerBuilder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
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
import java.util.Date;
import java.util.Map;

public class TestSslUtils {

    public static X509Certificate generateSignedCertificate(
        String dn,
        KeyPair keyPair,
        int daysBeforeNow,
        int daysAfterNow,
        String issuer,
        KeyPair parentKeyPair,
        String algorithm,
        boolean isCA,
        boolean isServerCert,
        boolean isClientCert,
        String[] hostNames
    ) throws CertificateException, IOException {
        return new CertificateBuilder(0, algorithm)
                .sanDnsNames(hostNames)
                .generateSignedCertificate(dn, keyPair, daysBeforeNow, daysAfterNow, issuer, parentKeyPair, isCA, isServerCert, isClientCert);
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

    public static class CertificateBuilder {
        private final int days;
        private final String algorithm;
        private byte[] subjectAltName;

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
                signerBuilder = switch (keyAlgorithm) {
                    case "RSA" -> new BcRSAContentSignerBuilder(sigAlgId, digAlgId);
                    case "DSA" -> new BcDSAContentSignerBuilder(sigAlgId, digAlgId);
                    case "EC" -> new BcECContentSignerBuilder(sigAlgId, digAlgId);
                    default -> throw new IllegalArgumentException("Unsupported algorithm " + keyAlgorithm);
                };
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
                signerBuilder = switch (keyAlgorithm) {
                    case "RSA" -> new BcRSAContentSignerBuilder(sigAlgId, digAlgId);
                    case "DSA" -> new BcDSAContentSignerBuilder(sigAlgId, digAlgId);
                    case "EC" -> new BcECContentSignerBuilder(sigAlgId, digAlgId);
                    default -> throw new IllegalArgumentException("Unsupported algorithm " + keyAlgorithm);
                };
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
}
