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

import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
/**
 * A wrapper around the original trust manager factory for creating common name logging trust managers.
 * These trust managers log the common name of an expired but otherwise valid (client) certificate before rejecting the connection attempt.
 * This allows to identify misconfigured clients in complex network environments, where the IP address is not sufficient.
 */
class CommonNameLoggingTrustManagerFactoryWrapper {

    private static final Logger log = LoggerFactory.getLogger(CommonNameLoggingTrustManagerFactoryWrapper.class);

    private TrustManagerFactory origTmf;

    /**
     * Create a wrapped trust manager factory
     * @param kmfAlgorithm the algorithm
     * @return A wrapped trust manager factory
     * @throws NoSuchAlgorithmException
     */
    protected CommonNameLoggingTrustManagerFactoryWrapper(String kmfAlgorithm) throws NoSuchAlgorithmException {
        this.origTmf = TrustManagerFactory.getInstance(kmfAlgorithm);
    }
    /**
     * Factory for creating a wrapped trust manager factory
     * @param kmfAlgorithm the algorithm
     * @return A wrapped trust manager factory
     * @throws NoSuchAlgorithmException
     */
    public static CommonNameLoggingTrustManagerFactoryWrapper getInstance(String kmfAlgorithm) throws NoSuchAlgorithmException {
        return new CommonNameLoggingTrustManagerFactoryWrapper(kmfAlgorithm);
    }

    public TrustManagerFactory getOriginalTrustManagerFactory() {
        return this.origTmf;
    }

    public String getAlgorithm() {
        return this.origTmf.getAlgorithm();
    }

    public void init(KeyStore ts) throws KeyStoreException {
        this.origTmf.init(ts);
    }

    public TrustManager[] getTrustManagers() {
        TrustManager[] origTrustManagers = this.origTmf.getTrustManagers();
        TrustManager[] wrappedTrustManagers = new TrustManager[origTrustManagers.length];
        for (int i = 0; i < origTrustManagers.length; i++) {
            TrustManager tm = origTrustManagers[i];
            if (tm instanceof X509TrustManager) {
                // Wrap only X509 trust managers
                wrappedTrustManagers[i] = new CommonNameLoggingTrustManager((X509TrustManager) tm, 2000);
            } else {
                wrappedTrustManagers[i] = tm;
            }
        }
        return wrappedTrustManagers;
    }
    /**
     * A trust manager which logs the common name of an expired but otherwise valid (client) certificate before rejecting the connection attempt.
     * This allows to identify misconfigured clients in complex network environments, where the IP address is not sufficient.
     * this class wraps a standard trust manager and delegates almost all requests to it, except for cases where an invalid certificate is reported by the
     * standard trust manager. In this cases this manager checks whether the provided certificate is invalid only due to being expired and logs the common
     * name if that is the case. This trust manager will always return the results of the wrapped standard trust manager, i.e. return if the certificate is valid
     * or rethrow the original exception if it is not.
     */
    static class CommonNameLoggingTrustManager implements X509TrustManager {

        final private X509TrustManager origTm;
        final int nrOfRememberedBadCerts;
        final private LinkedHashMap<ByteBuffer, String> previouslyRejectedClientCertChains;

        public CommonNameLoggingTrustManager(X509TrustManager originalTrustManager, int nrOfRememberedBadCerts) {
            this.origTm = originalTrustManager;
            this.nrOfRememberedBadCerts = nrOfRememberedBadCerts;
            // Restrict maximal size of the LinkedHashMap to avoid security attacks causing OOM
            this.previouslyRejectedClientCertChains = new LinkedHashMap<ByteBuffer, String>() {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<ByteBuffer, String> eldest) {
                    return size() > nrOfRememberedBadCerts;
                }
            };
        }

        public X509TrustManager getOriginalTrustManager() {
            return this.origTm;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            CertificateException origException = null;
            ByteBuffer chainDigest = calcDigestForCertificateChain(chain);
            if (chainDigest != null) {
                String errorMessage = this.previouslyRejectedClientCertChains.get(chainDigest);
                if (errorMessage != null) {
                    // Reinsert the digest, to remember that this is the most recent digest we've seen
                    addRejectedClientCertChains(chainDigest, errorMessage, true);
                    // Then throw with the original error Message
                    throw new CertificateException(errorMessage);
                }
            }
            try {
                this.origTm.checkClientTrusted(chain, authType);
                // If the last line did not throw, the chain is valid (including that none of the certificates is expired)
            } catch (CertificateException e) {
                origException = e;
                try {
                    X509Certificate[] wrappedChain = sortChainAnWrapEndCertificate(chain);
                    this.origTm.checkClientTrusted(wrappedChain, authType);
                    // No exception occurred this time. The certificate is either not yet valid or already expired
                    Date now = new Date();
                    // Check if the certificate was valid in the past
                    if (wrappedChain[0].getNotBefore().before(now)) {
                        String commonName = wrappedChain[0].getSubjectX500Principal().toString();
                        String notValidAfter = wrappedChain[0].getNotAfter().toString();
                        log.info("Certificate with common name \"" + commonName + "\" expired on " + notValidAfter);
                        // The end certificate is expired and thus will never become valid anymore, as long as the trust store is not changed
                        addRejectedClientCertChains(chainDigest, origException.getMessage(), false);
                    }

                } catch (CertificateException innerException) {
                    // Ignore this exception as we throw the original one below
                    // Even with disabled date check, this cert chain is invalid: Remember the chain and fail faster next time we see it
                    addRejectedClientCertChains(chainDigest, origException.getMessage(), false);
                }
            }
            if (origException != null) {
                throw origException;
            }
        }

        public static ByteBuffer calcDigestForCertificateChain(X509Certificate[] chain) throws CertificateEncodingException {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                return null;
            }
            for (X509Certificate cert: chain) {
                md.update(cert.getEncoded());
            }
            return ByteBuffer.wrap(md.digest());
        }

        private void addRejectedClientCertChains(ByteBuffer chainDigest, String errorMessage, boolean removeIfExisting) {
            if (removeIfExisting) {
                this.previouslyRejectedClientCertChains.remove(chainDigest);
            }
            this.previouslyRejectedClientCertChains.put(chainDigest, errorMessage);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            this.origTm.checkServerTrusted(chain, authType);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return this.origTm.getAcceptedIssuers();
        }
        
       /**
         * This method sorts the certificate chain from end to root certificate and wraps the end certificate to make it "never-expireing"
         * @param origChain The original (unsorted) certificate chain
         * @return The sorted and wrapped certificate chain
         * @throws CertificateException
         * @throws NoSuchAlgorithmException
         */
        public static X509Certificate[] sortChainAnWrapEndCertificate(X509Certificate[] origChain) throws CertificateException {
            if (origChain == null || origChain.length < 1) {
                throw new CertificateException("Certificate chain is null or empty");
            }
            // Find the end certificate by looking at all issuers and find the one not referred to by any other certificate
            // There might be multiple end certificates if the chain is invalid!
            // Create a map from principal to certificate
            HashMap<X500Principal, X509Certificate> principalToCertMap = new HashMap<>();
            // First, create a map from principal of issuer (!) to certificate for easily finding the right certificates
            HashMap<X500Principal, X509Certificate> issuedbyPrincipalToCertificatesMap = new HashMap<>();
            for (X509Certificate cert: origChain) {
                X500Principal principal = cert.getSubjectX500Principal();
                X500Principal issuerPrincipal = cert.getIssuerX500Principal();
                if (issuerPrincipal.equals(principal)) {
                    // self-signed certificate in chain! This should not happen
                    boolean isCA = cert.getBasicConstraints() >= 0;
                    if (!isCA) {
                        throw new CertificateException("Self-signed certificate in chain that is not a CA!");
                    }
                }
                issuedbyPrincipalToCertificatesMap.put(issuerPrincipal, cert);
                principalToCertMap.put(principal, cert);
            }
            // Thus, expect certificate chain to be broken, e.g. containing multiple enbd certificates
            HashSet<X509Certificate> endCertificates = new HashSet<>();
            for (X509Certificate cert: origChain) {
                X500Principal subjectPrincipal = cert.getSubjectX500Principal();
                if (!issuedbyPrincipalToCertificatesMap.containsKey(subjectPrincipal)) {
                    // We found a certificate which is not an issuer of another certificate. We consider it to be an end certificate
                    endCertificates.add(cert);
                }
            }
            // There should be exactly one end certificate, otherwise we don't know which one to wrap
            if (endCertificates.size() != 1) {
                throw new CertificateException("Multiple end certificates in chain");
            }
            X509Certificate endCertificate = endCertificates.iterator().next();
            X509Certificate[] wrappedChain = new X509Certificate[origChain.length];
            // Add the wrapped certificate as first element in the new certificate chain array
            wrappedChain[0] = new NeverExpiringX509Certificate(endCertificate);
            // Add all other (potential) certificates in order of dependencies (result will be sorted from end certificate to last intermediate/root certificate)
            for (int i = 1; i < origChain.length; i++) {
                X500Principal siblingCertificateIssuer = wrappedChain[i - 1].getIssuerX500Principal();
                if (principalToCertMap.containsKey(siblingCertificateIssuer)) {
                    wrappedChain[i] = principalToCertMap.get(siblingCertificateIssuer);
                } else {
                    throw new CertificateException("Certificate chain contains certificates not belonging to the chain");
                }
            }
            return wrappedChain;
        }
    }

    static class NeverExpiringX509Certificate extends X509Certificate {

        private X509Certificate origCertificate;

        public NeverExpiringX509Certificate(X509Certificate origCertificate) {
            this.origCertificate = origCertificate;
            if (this.origCertificate == null) {
                throw new KafkaException("No X509 certificate provided in constructor NeverExpiringX509Certificate");
            }
        }

        @Override
        public Set<String> getCriticalExtensionOIDs() {
            return this.origCertificate.getCriticalExtensionOIDs();
        }

        @Override
        public byte[] getExtensionValue(String oid) {
            return this.origCertificate.getExtensionValue(oid);
        }

        @Override
        public Set<String> getNonCriticalExtensionOIDs() {
            return this.origCertificate.getNonCriticalExtensionOIDs();
        }

        @Override
        public boolean hasUnsupportedCriticalExtension() {
            return this.origCertificate.hasUnsupportedCriticalExtension();
        }

        @Override
        public void checkValidity()
                throws CertificateExpiredException, CertificateNotYetValidException {
            Date now = new Date();
            // Do nothing for certificates which are not valid anymore now
            if (this.origCertificate.getNotAfter().before(now)) {
                return;
            }
            // Check validity as usual
            this.origCertificate.checkValidity();
        }

        @Override
        public void checkValidity(Date date)
                throws CertificateExpiredException, CertificateNotYetValidException {
            // We do not check validity at all. 
            return;
        }

        @Override
        public int getBasicConstraints() {
            return this.origCertificate.getBasicConstraints();
        }

        @Override
        public Principal getIssuerDN() {
            return this.origCertificate.getIssuerDN();
        }

        @Override
        public boolean[] getIssuerUniqueID() {
            return this.origCertificate.getIssuerUniqueID();
        }

        @Override
        public boolean[] getKeyUsage() {
            return this.origCertificate.getKeyUsage();
        }

        @Override
        public Date getNotAfter() {
            return this.origCertificate.getNotAfter();
        }

        @Override
        public Date getNotBefore() {
            return this.origCertificate.getNotBefore();
        }

        @Override
        public BigInteger getSerialNumber() {
            return this.origCertificate.getSerialNumber();
        }

        @Override
        public String getSigAlgName() {
            return this.origCertificate.getSigAlgName();
        }

        @Override
        public String getSigAlgOID() {
            return this.origCertificate.getSigAlgOID();
        }

        @Override
        public byte[] getSigAlgParams() {
            return this.origCertificate.getSigAlgParams();
        }

        @Override
        public byte[] getSignature() {
            return this.origCertificate.getSignature();
        }

        @Override
        public Principal getSubjectDN() {
            return this.origCertificate.getSubjectDN();
        }

        @Override
        public boolean[] getSubjectUniqueID() {
            return this.origCertificate.getSubjectUniqueID();
        }

        @Override
        public byte[] getTBSCertificate() throws CertificateEncodingException {
            return this.origCertificate.getTBSCertificate();
        }

        @Override
        public int getVersion() {
            return this.origCertificate.getVersion();
        }

        @Override
        public byte[] getEncoded() throws CertificateEncodingException {
            return this.origCertificate.getEncoded();
        }

        @Override
        public PublicKey getPublicKey() {
            return this.origCertificate.getPublicKey();
        }

        @Override
        public String toString() {
            return this.origCertificate.toString();
        }

        @Override
        public void verify(PublicKey publicKey) throws CertificateException, NoSuchAlgorithmException,
                InvalidKeyException, NoSuchProviderException, SignatureException {
            this.origCertificate.verify(publicKey);
        }

        @Override
        public void verify(PublicKey publicKey, String sigProvider)
                throws CertificateException, NoSuchAlgorithmException, InvalidKeyException,
                NoSuchProviderException, SignatureException {
            this.origCertificate.verify(publicKey, sigProvider);
        }
    }
}
