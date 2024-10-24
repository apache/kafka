package org.apache.kafka.common.security.ssl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

public class TruststoreUtility {

    public static final String CRT = "CRT";

    public static KeyStore createTrustStore(String locationOfCerts, String trustStorePass) throws GeneralSecurityException, IOException {
        if(!new File(locationOfCerts).exists()){
            locationOfCerts = System.getenv(locationOfCerts);
        }
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, trustStorePass.toCharArray());
        try (FileInputStream fis = new FileInputStream(locationOfCerts)) {
            try (BufferedInputStream bis = new BufferedInputStream(fis)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                Certificate cert = null;

                while (bis.available() > 0) {
                    cert = cf.generateCertificate(bis);
                    ks.setCertificateEntry(String.valueOf(bis.available()), cert);
                }
                ks.setCertificateEntry(String.valueOf(bis.available()), cert);
                return ks;
            }
        }
    }
}