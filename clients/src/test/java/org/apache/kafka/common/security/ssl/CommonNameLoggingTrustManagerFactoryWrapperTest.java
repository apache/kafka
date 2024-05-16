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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.security.ssl.CommonNameLoggingTrustManagerFactoryWrapper.CommonNameLoggingTrustManager;
import org.apache.kafka.common.security.ssl.CommonNameLoggingTrustManagerFactoryWrapper.NeverExpiringX509Certificate;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.test.TestSslUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(Lifecycle.PER_CLASS)
public class CommonNameLoggingTrustManagerFactoryWrapperTest {

    private X509Certificate[] chainWithValidEndCertificate;
    private X509Certificate[] chainWithExpiredEndCertificate;
    private X509Certificate[] chainWithInvalidEndCertificate;
    private X509Certificate[] chainWithMultipleEndCertificates;
    private X509Certificate[] chainWithValidAndInvalidEndCertificates;

    @BeforeAll
    public void setUpOnce() throws CertificateException, NoSuchAlgorithmException {
        chainWithValidEndCertificate = generateKeyChainIncludingCA(false, false, true, false);
        chainWithExpiredEndCertificate = generateKeyChainIncludingCA(true, false, true, false);
        chainWithInvalidEndCertificate = generateKeyChainIncludingCA(false, false, false, false);
        chainWithMultipleEndCertificates = generateKeyChainIncludingCA(false, true, false, true);
        chainWithValidAndInvalidEndCertificates = generateKeyChainIncludingCA(false, true, true, false);
    }

    @Test
    void testNeverExpiringX509Certificate() throws Exception {
        final KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        final String dn = "CN=Test, L=London, C=GB";
        // Create and initialize data structures
        int nrOfCerts = 5;
        X509Certificate[] testCerts = new X509Certificate[nrOfCerts];
        PublicKey[] signedWith = new PublicKey[nrOfCerts];
        boolean[] expectValidEndCert = new boolean[nrOfCerts];
        final int days = 1;
        // Generate valid certificate
        testCerts[0] = TestSslUtils.generateCertificate(dn, keyPair, days, "SHA512withRSA");
        // Self-signed
        signedWith[0] = testCerts[0].getPublicKey();
        expectValidEndCert[0] = true;
        // Generate expired, but valid certificate
        testCerts[1] = TestSslUtils.generateCertificate(dn, keyPair, -days, "SHA512withRSA");
        // Self-signed
        signedWith[1] = testCerts[1].getPublicKey();
        expectValidEndCert[1] = true;
        // Use existing real certificate chain, where the end certificate (the first on
        // in the
        // chain) is valid
        testCerts[2] = chainWithValidEndCertificate[0];
        // The end certificate must be signed by the intermediate CA public key
        signedWith[2] = chainWithValidEndCertificate[1].getPublicKey();
        expectValidEndCert[2] = true;
        // Use existing real certificate chain, where the end certificate (the first on
        // in the
        // chain) is expired
        testCerts[3] = chainWithExpiredEndCertificate[0];
        // The end certificate must be signed by the intermediate CA public key
        signedWith[3] = chainWithExpiredEndCertificate[1].getPublicKey();
        expectValidEndCert[3] = true;
        // Test with invalid certificate
        testCerts[4] = chainWithInvalidEndCertificate[0];
        // Check whether this certificate is signed by the intermediate certificate in
        // our chain (it is not)
        signedWith[4] = chainWithInvalidEndCertificate[1].getPublicKey();
        expectValidEndCert[4] = false;

        for (int i = 0; i < testCerts.length; i++) {
            X509Certificate cert = testCerts[i];
            final NeverExpiringX509Certificate wrappedCert = new NeverExpiringX509Certificate(
                    cert);
            // All results must be identically for original as well as wrapped certificate
            // class
            assertEquals(cert.getCriticalExtensionOIDs(), wrappedCert.getCriticalExtensionOIDs());
            final String testOid = "2.5.29.14"; // Should not be in test certificate
            assertEquals(cert.getExtensionValue(testOid), wrappedCert.getExtensionValue(testOid));
            assertEquals(cert.getNonCriticalExtensionOIDs(),
                    wrappedCert.getNonCriticalExtensionOIDs());
            assertEquals(cert.hasUnsupportedCriticalExtension(),
                    wrappedCert.hasUnsupportedCriticalExtension());
            // We have just generated a valid test certificate, it should still be valid now
            assertEquals(cert.getBasicConstraints(), wrappedCert.getBasicConstraints());
            assertEquals(cert.getIssuerDN(), wrappedCert.getIssuerDN());
            assertEquals(cert.getIssuerUniqueID(), wrappedCert.getIssuerUniqueID());
            assertEquals(cert.getKeyUsage(), wrappedCert.getKeyUsage());
            assertEquals(cert.getNotAfter(), wrappedCert.getNotAfter());
            assertEquals(cert.getNotBefore(), wrappedCert.getNotBefore());
            assertEquals(cert.getSerialNumber(), wrappedCert.getSerialNumber());
            assertEquals(cert.getSigAlgName(), wrappedCert.getSigAlgName());
            assertEquals(cert.getSigAlgOID(), wrappedCert.getSigAlgOID());
            assertArrayEquals(cert.getSigAlgParams(), wrappedCert.getSigAlgParams());
            assertArrayEquals(cert.getSignature(), wrappedCert.getSignature());
            assertEquals(cert.getSubjectDN(), wrappedCert.getSubjectDN());
            assertEquals(cert.getSubjectUniqueID(), wrappedCert.getSubjectUniqueID());
            assertArrayEquals(cert.getTBSCertificate(), wrappedCert.getTBSCertificate());
            assertEquals(cert.getVersion(), wrappedCert.getVersion());
            assertArrayEquals(cert.getEncoded(), wrappedCert.getEncoded());
            assertEquals(cert.getPublicKey(), wrappedCert.getPublicKey());
            assertEquals(cert.toString(), wrappedCert.toString());
            final PublicKey signingKey = signedWith[i];
            if (expectValidEndCert[i]) {
                assertDoesNotThrow(() -> cert.verify(signingKey));
                assertDoesNotThrow(() -> wrappedCert.verify(signingKey));
            } else {
                Exception origException = assertThrows(SignatureException.class, () -> cert.verify(signingKey));
                Exception testException = assertThrows(SignatureException.class, () -> wrappedCert.verify(signingKey));
                assertEquals(origException.getMessage(), testException.getMessage());
            }
            // Test timing now, starting with "now"
            Date dateNow = new Date();
            if (cert.getNotBefore().before(dateNow) && cert.getNotAfter().after(dateNow)) {
                assertDoesNotThrow(() -> cert.checkValidity());
            } else {
                assertThrows(CertificateException.class, () -> cert.checkValidity());
            }
            // The wrappedCert must never throw due to being expired
            assertDoesNotThrow(() -> wrappedCert.checkValidity());
            if (cert.getNotBefore().before(dateNow) && cert.getNotAfter().after(dateNow)) {
                assertDoesNotThrow(() -> cert.checkValidity(dateNow));
            } else {
                assertThrows(CertificateException.class, () -> cert.checkValidity(dateNow));
            }
            // wrapped cert must not throw even if it is expired
            assertDoesNotThrow(() -> wrappedCert.checkValidity(dateNow));
            // Test with (days/2) before now.
            Date dateRecentPast = new Date(System.currentTimeMillis() - days * 12 * 60 * 60 * 1000);
            if (cert.getNotBefore().before(dateRecentPast)
                    && cert.getNotAfter().after(dateRecentPast)) {
                assertDoesNotThrow(() -> cert.checkValidity(dateRecentPast));
                assertDoesNotThrow(() -> wrappedCert.checkValidity(dateRecentPast));
            } else {
                // Cert not valid yet
                assertThrows(CertificateException.class,
                        () -> cert.checkValidity(dateRecentPast));
                // The wrappend certificate class does not check dates at all
                assertDoesNotThrow(() -> wrappedCert.checkValidity(dateRecentPast));
            }
            // Test with (days+1) before now. Both certificates were not yet valid, thus
            // both checks
            // must throw
            Date datePast = new Date(System.currentTimeMillis() - (days + 2) * 24 * 60 * 60 * 1000);
            assertThrows(CertificateException.class, () -> cert.checkValidity(datePast));
            // The wrappend certificate class does not check dates at all
            assertDoesNotThrow(() -> wrappedCert.checkValidity(datePast));
            // Test with "days+2" after now.
            // Cert is not valid anymore. The original class must throw
            Date dateFuture = new Date(System.currentTimeMillis() + (days + 2) * 24 * 60 * 60 * 1000);
            assertThrows(CertificateException.class, () -> cert.checkValidity(dateFuture));
            // This checks the only deviation in behavior of the
            // NeverExpiringX509Certificate
            // compared to the standard Certificate:
            // The NeverExpiringX509Certificate will report any expired certificate as still
            // valid
            assertDoesNotThrow(() -> wrappedCert.checkValidity(dateFuture));
        }
    }

    private static X509TrustManager getX509TrustManager(TrustManagerFactory tmf) throws Exception {
        for (TrustManager trustManager : tmf.getTrustManagers()) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        throw new Exception("Unable to find X509TrustManager");
    }

    @Test
    public void testCommonNameLoggingTrustManagerFactoryWrapper() throws Exception {
        // We need to construct a trust store for testing
        X509Certificate caCert = chainWithValidEndCertificate[2];
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String kmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory origTmFactory = TrustManagerFactory.getInstance(kmfAlgorithm);
        origTmFactory.init(trustStore);
        TrustManager[] origTrustManagers = origTmFactory.getTrustManagers();
        // Create wrapped trust manager factory
        CommonNameLoggingTrustManagerFactoryWrapper testTmFactory = CommonNameLoggingTrustManagerFactoryWrapper.getInstance(kmfAlgorithm);
        testTmFactory.init(trustStore);
        TrustManager[] wrappendTrustManagers = testTmFactory.getTrustManagers();
        // Number of trust managers must be equal (usually "1")
        assertEquals(origTrustManagers.length, wrappendTrustManagers.length);
        // Algorithms must be equal
        assertEquals(origTmFactory.getAlgorithm(), testTmFactory.getAlgorithm());
        // Compare trust managers. Only for X509 there must be a difference
        for (int i = 0; i < origTrustManagers.length; i++) {
            TrustManager origTrustManager = origTrustManagers[i];
            TrustManager testTrustManager = wrappendTrustManagers[i];
            if (origTrustManager instanceof X509TrustManager) {
                assertInstanceOf(CommonNameLoggingTrustManager.class, testTrustManager);
                CommonNameLoggingTrustManager commonNameLoggingTrustManager = (CommonNameLoggingTrustManager) testTrustManager;
                // Two different instances of X509TrustManager wouldn't be considered equal. Thus we at least check that their classes are equal
                assertEquals(origTrustManager.getClass(), commonNameLoggingTrustManager.getOriginalTrustManager().getClass());
            } else {
                // Two different instances of X509TrustManager wouldn't be considered equal. Thus we at least check that their classes are equal
                assertEquals(origTrustManager.getClass(), testTrustManager.getClass());
            }
        }
    }

    @Test
    public void testCommonNameLoggingTrustManagerValidChain() throws Exception {

        X509Certificate endCert = chainWithValidEndCertificate[0];
        X509Certificate intermediateCert = chainWithValidEndCertificate[1];
        X509Certificate caCert = chainWithValidEndCertificate[2];
        X509Certificate[] chainWithoutCa = new X509Certificate[] {endCert, intermediateCert};
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(trustStore);
        final X509TrustManager origTrustManager = getX509TrustManager(tmf);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CommonNameLoggingSslEngineFactory.class)) {
            int nrOfInitialMessagges = appender.getMessages().size();
            CommonNameLoggingTrustManager testTrustManager = new CommonNameLoggingTrustManager(origTrustManager, 2);
            // Check client certificate first
            assertEquals(testTrustManager.getOriginalTrustManager(), origTrustManager);
            assertDoesNotThrow(() -> origTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertDoesNotThrow(() -> testTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            // Check the same client certificate again. Expect the exact same behavior as before
            assertEquals(testTrustManager.getOriginalTrustManager(), origTrustManager);
            assertDoesNotThrow(() -> origTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertDoesNotThrow(() -> testTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            // Check server certificate (no changes here)
            assertDoesNotThrow(() -> origTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            assertDoesNotThrow(() -> testTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            assertArrayEquals(origTrustManager.getAcceptedIssuers(), testTrustManager.getAcceptedIssuers());
        }
    }

    @Test
    public void testCommonNameLoggingTrustManagerValidChainWithCA() throws Exception {

        X509Certificate endCert = chainWithValidEndCertificate[0];
        X509Certificate intermediateCert = chainWithValidEndCertificate[1];
        X509Certificate caCert = chainWithValidEndCertificate[2];
        X509Certificate[] chainWitCa = new X509Certificate[] {endCert, intermediateCert, caCert};
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(trustStore);
        final X509TrustManager origTrustManager = getX509TrustManager(tmf);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CommonNameLoggingSslEngineFactory.class)) {
            int nrOfInitialMessagges = appender.getMessages().size();
            CommonNameLoggingTrustManager testTrustManager = new CommonNameLoggingTrustManager(origTrustManager, 2);
            assertEquals(testTrustManager.getOriginalTrustManager(), origTrustManager);
            assertDoesNotThrow(() -> origTrustManager.checkClientTrusted(chainWitCa, "RSA"));
            assertDoesNotThrow(() -> testTrustManager.checkClientTrusted(chainWitCa, "RSA"));
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            assertDoesNotThrow(() -> origTrustManager.checkServerTrusted(chainWitCa, "RSA"));
            assertDoesNotThrow(() -> testTrustManager.checkServerTrusted(chainWitCa, "RSA"));
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            assertArrayEquals(origTrustManager.getAcceptedIssuers(), testTrustManager.getAcceptedIssuers());
        }
    }


    @Test
    public void testCommonNameLoggingTrustManagerWithInvalidEndCert() throws Exception {
        X509Certificate endCert = chainWithInvalidEndCertificate[0];
        X509Certificate intermediateCert = chainWithInvalidEndCertificate[1];
        X509Certificate caCert = chainWithInvalidEndCertificate[2];
        X509Certificate[] chainWithoutCa = new X509Certificate[] {endCert, intermediateCert};
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(trustStore);
        final X509TrustManager origTrustManager = getX509TrustManager(tmf);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CommonNameLoggingSslEngineFactory.class)) {
            int nrOfInitialMessagges = appender.getMessages().size();
            CommonNameLoggingTrustManager testTrustManager = new CommonNameLoggingTrustManager(origTrustManager, 2);
            // Check client certificate
            assertEquals(testTrustManager.getOriginalTrustManager(), origTrustManager);
            Exception origException = assertThrows(CertificateException.class,
                    () -> origTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            Exception testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            // Check the client certificate again, expecting the exact same result
            assertEquals(testTrustManager.getOriginalTrustManager(), origTrustManager);
            origException = assertThrows(CertificateException.class,
                    () -> origTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());

            // Check server certificate
            origException = assertThrows(CertificateException.class,
                    () -> origTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            assertEquals(nrOfInitialMessagges, appender.getMessages().size());
            assertArrayEquals(origTrustManager.getAcceptedIssuers(), testTrustManager.getAcceptedIssuers());
        }
    }

    @Test
    public void testCommonNameLoggingTrustManagerWithExpiredEndCert() throws Exception {
        X509Certificate endCert = chainWithExpiredEndCertificate[0];
        X509Certificate intermediateCert = chainWithExpiredEndCertificate[1];
        X509Certificate caCert = chainWithExpiredEndCertificate[2];
        X509Certificate[] chainWithoutCa = new X509Certificate[] {endCert, intermediateCert};
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(trustStore);
        final X509TrustManager origTrustManager = getX509TrustManager(tmf);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CommonNameLoggingTrustManagerFactoryWrapper.class)) {
            int nrOfInitialMessagges = appender.getMessages().size();

            CommonNameLoggingTrustManager testTrustManager = new CommonNameLoggingTrustManager(origTrustManager, 2);
            assertEquals(origTrustManager, testTrustManager.getOriginalTrustManager());
            // Call original method, then method of wrapped trust manager and compare result
            Exception origException = assertThrows(CertificateException.class,
                    () -> origTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            Exception testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            // Check that there is exactly one new message
            List<String> logMessages = appender.getMessages();
            assertEquals(nrOfInitialMessagges + 1, logMessages.size());
            assertEquals("Certificate with common name \"" + endCert.getSubjectX500Principal() +
                "\" expired on " + endCert.getNotAfter(), logMessages.get(logMessages.size() - 1));
            // Call original method, then method of wrapped trust manager and compare result
            origException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            // Check that there are no new messages
            assertEquals(nrOfInitialMessagges + 1, appender.getMessages().size());
            assertArrayEquals(origTrustManager.getAcceptedIssuers(), testTrustManager.getAcceptedIssuers());
        }
    }

    @Test
    public void testCommonNameLoggingTrustManagerWithExpiredEndCertWithCA() throws Exception {
        X509Certificate endCert = chainWithExpiredEndCertificate[0];
        X509Certificate intermediateCert = chainWithExpiredEndCertificate[1];
        X509Certificate caCert = chainWithExpiredEndCertificate[2];
        X509Certificate[] chainWithoutCa = new X509Certificate[] {endCert, intermediateCert, caCert};
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(trustStore);
        final X509TrustManager origTrustManager = getX509TrustManager(tmf);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CommonNameLoggingTrustManagerFactoryWrapper.class)) {
            int nrOfInitialMessagges = appender.getMessages().size();

            CommonNameLoggingTrustManager testTrustManager = new CommonNameLoggingTrustManager(origTrustManager, 2);
            assertEquals(origTrustManager, testTrustManager.getOriginalTrustManager());
            // Call original method, then method of wrapped trust manager and compare result
            Exception origException = assertThrows(CertificateException.class,
                    () -> origTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            Exception testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkClientTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            // Check that there is exactly one new message
            List<String> logMessages = appender.getMessages();
            assertEquals(nrOfInitialMessagges + 1, logMessages.size());
            assertEquals("Certificate with common name \"" + endCert.getSubjectX500Principal() +
                "\" expired on " + endCert.getNotAfter(), logMessages.get(logMessages.size() - 1));
            // Note: As there are multiple SSLContext created within Kafka, the message may be logged multiple times

            // Check validation of server certificates, then method of wrapped trust manager and compare result
            origException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            testException = assertThrows(CertificateException.class,
                    () -> testTrustManager.checkServerTrusted(chainWithoutCa, "RSA"));
            assertEquals(origException.getMessage(), testException.getMessage());
            // Check that there are no new messages
            assertEquals(nrOfInitialMessagges + 1, appender.getMessages().size());
            assertArrayEquals(origTrustManager.getAcceptedIssuers(), testTrustManager.getAcceptedIssuers());
        }
    }

    @Test
    public void testCommonNameLoggingTrustManagerMixValidAndInvalidCertificates() throws Exception {
        // Setup certificate chain with expired end certificate
        X509Certificate endCertValid = chainWithValidAndInvalidEndCertificates[0];
        X509Certificate endCertInvalid = chainWithValidAndInvalidEndCertificates[1];
        X509Certificate intermediateCert = chainWithValidAndInvalidEndCertificates[2];
        X509Certificate caCert = chainWithValidAndInvalidEndCertificates[3];
        X509Certificate[] validChainWithoutCa = new X509Certificate[] {endCertValid, intermediateCert};
        X509Certificate[] invalidChainWithoutCa = new X509Certificate[] {endCertInvalid, intermediateCert};
        // Setup certificate chain with valid end certificate

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("CA", caCert);

        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(trustStore);
        final X509TrustManager origTrustManager = getX509TrustManager(tmf);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(CommonNameLoggingSslEngineFactory.class)) {

            CommonNameLoggingTrustManager testTrustManager = new CommonNameLoggingTrustManager(origTrustManager, 2);

            // Call with valid certificate
            //assertDoesNotThrow(() -> testTrustManager.checkClientTrusted(validChainWithoutCa, "RSA"));
            // Call with invalid certificate
            assertThrows(CertificateException.class,
                    () -> testTrustManager.checkClientTrusted(invalidChainWithoutCa, "RSA"));
            // Call with valid certificate again
            //assertDoesNotThrow(() -> testTrustManager.checkClientTrusted(validChainWithoutCa, "RSA"));
            // Call with invalid certificate
            assertThrows(CertificateException.class,
                    () -> testTrustManager.checkClientTrusted(invalidChainWithoutCa, "RSA"));
            // Call with valid certificate again
            //assertDoesNotThrow(() -> testTrustManager.checkClientTrusted(validChainWithoutCa, "RSA"));
        }
    }

    @Test
    public void testSortChainAnWrapEndCertificate() {
        // Calling method with null or empty chain is expected to throw
        assertThrows(CertificateException.class,
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(null));
        assertThrows(CertificateException.class,
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(new X509Certificate[0]));

        X509Certificate endCert = chainWithExpiredEndCertificate[0];
        X509Certificate intermediateCert = chainWithExpiredEndCertificate[1];
        X509Certificate caCert = chainWithExpiredEndCertificate[2];

        // Check that a chain with just one certificate works
        X509Certificate[] chainWithEndCert = new X509Certificate[] {endCert};
        X509Certificate[] sortedChain = assertDoesNotThrow(
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(chainWithEndCert));
        assertEquals(endCert.getSubjectX500Principal(), sortedChain[0].getSubjectX500Principal());
        // Check that the order is unchanged for an already sorted certificate chain
        // (starting with end certificate)
        X509Certificate[] chainWithoutCaInOrder = new X509Certificate[] {endCert, intermediateCert};
        sortedChain = assertDoesNotThrow(
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(chainWithoutCaInOrder));
        assertEquals(endCert.getSubjectX500Principal(), sortedChain[0].getSubjectX500Principal());
        assertEquals(intermediateCert.getSubjectX500Principal(), sortedChain[1].getSubjectX500Principal());
        // Check that the order is changed for an unsorted certificate chain such that
        // it starts with end certificate
        X509Certificate[] chainWithoutCaOutOfOrder = new X509Certificate[] {intermediateCert, endCert};
        sortedChain = assertDoesNotThrow(
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(chainWithoutCaOutOfOrder));
        assertEquals(endCert.getSubjectX500Principal(), sortedChain[0].getSubjectX500Principal());
        assertEquals(intermediateCert.getSubjectX500Principal(), sortedChain[1].getSubjectX500Principal());

        X509Certificate[] chainWithCaOutOfOrder = new X509Certificate[] {caCert, intermediateCert, endCert};
        sortedChain = assertDoesNotThrow(
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(chainWithCaOutOfOrder));
        assertEquals(endCert.getSubjectX500Principal(), sortedChain[0].getSubjectX500Principal());
        assertEquals(intermediateCert.getSubjectX500Principal(), sortedChain[1].getSubjectX500Principal());
        assertEquals(caCert.getSubjectX500Principal(), sortedChain[2].getSubjectX500Principal());
    }

    @Test
    public void testSortChainWithMultipleEndCertificate() {
        assertThrows(CertificateException.class, 
                () -> CommonNameLoggingTrustManager.sortChainAnWrapEndCertificate(chainWithMultipleEndCertificates));
    }

    @Test
    public void testCalcDigestForCertificateChain() {
        ByteBuffer digestForValidChain = 
            assertDoesNotThrow(() -> CommonNameLoggingTrustManager.calcDigestForCertificateChain(chainWithValidEndCertificate));
        ByteBuffer digestForValidChainAgain = 
            assertDoesNotThrow(() -> CommonNameLoggingTrustManager.calcDigestForCertificateChain(chainWithValidEndCertificate));
        assertEquals(digestForValidChain, digestForValidChainAgain);
        ByteBuffer digestForInvalidChain = 
            assertDoesNotThrow(() -> CommonNameLoggingTrustManager.calcDigestForCertificateChain(chainWithInvalidEndCertificate));
        assertNotEquals(digestForValidChain, digestForInvalidChain);
        ByteBuffer digestForExpiredChain = 
            assertDoesNotThrow(() -> CommonNameLoggingTrustManager.calcDigestForCertificateChain(chainWithExpiredEndCertificate));
        assertNotEquals(digestForValidChain, digestForExpiredChain);
        assertNotEquals(digestForInvalidChain, digestForExpiredChain);
    }

    /**
     * This helper method generates a valid key chain with one end entity
     * (client/server cert), one intermediate certificate authority and one 
     * root certificate authority (self-signed)
     * 
     * @return
     * @throws CertificateException
     * @throws NoSuchAlgorithmException
     */
    private X509Certificate[] generateKeyChainIncludingCA(boolean expired, boolean multipleEndCert, boolean endCert0Valid, boolean endCert1Valid)
            throws CertificateException, NoSuchAlgorithmException {
        // For testing, we might create another end certificate
        int nrOfCerts = multipleEndCert ? 4 : 3;
        KeyPair[] keyPairs = new KeyPair[nrOfCerts];
        for (int i = 0; i < nrOfCerts; i++) {
            keyPairs[i] = TestSslUtils.generateKeyPair("RSA");
        }
        X509Certificate[] certs = new X509Certificate[nrOfCerts];
        int endCertDaysValidBeforeNow = 1;
        // If using 0 or a negative value, the generated certificate will be expired
        int endCertDaysValidAfterNow = expired ? 0 : 1;
        // Generate root CA
        int caIndex = nrOfCerts - 1;
        certs[caIndex] = TestSslUtils.generateSignedCertificate("CN=CA", keyPairs[caIndex], 365,
                365, null, null, "SHA512withRSA", true, false, false);
        int intermediateCertIndex = caIndex - 1;
        certs[intermediateCertIndex] = TestSslUtils.generateSignedCertificate("CN=Intermediate CA",
                keyPairs[intermediateCertIndex], 365, 365, certs[caIndex].getSubjectX500Principal().getName(), keyPairs[caIndex],
                "SHA512withRSA", true, false, false);
        for (int currIndex = intermediateCertIndex - 1; currIndex >= 0; currIndex--) {
            // When generating multiple end certificates, 
            boolean endCertValid = (currIndex == 0) ? endCert0Valid : endCert1Valid;
            if (endCertValid) {
                // Generate a valid end certificate, i.e. one that is signed by our intermediate
                // CA
                certs[currIndex] = TestSslUtils.generateSignedCertificate("CN=kafka", keyPairs[currIndex],
                        endCertDaysValidBeforeNow, endCertDaysValidAfterNow,
                        certs[intermediateCertIndex].getSubjectX500Principal().getName(), keyPairs[intermediateCertIndex], "SHA512withRSA", false, true, true);
            } else {
                // Generate an invalid end certificate, by creating a self-signed one.
                certs[currIndex] = TestSslUtils.generateSignedCertificate("C=GB, L=London, CN=kafka", keyPairs[currIndex],
                        endCertDaysValidBeforeNow, endCertDaysValidAfterNow,
                        null, null, "SHA512withRSA", false, true, true);
            }
        }
        return certs;
    }
}

