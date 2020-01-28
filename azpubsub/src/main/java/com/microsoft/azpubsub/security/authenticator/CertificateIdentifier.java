package com.microsoft.azpubsub.security.authenticator;

import javax.net.ssl.SSLSession;

import com.microsoft.azpubsub.security.auth.AzPubSubConfig;

/*
 * Interface retrieve the certificate identity
 */
public interface CertificateIdentifier {
    public void configure(AzPubSubConfig config);

    public CertificateIdentity getIdentity(SSLSession sslSession);
}
