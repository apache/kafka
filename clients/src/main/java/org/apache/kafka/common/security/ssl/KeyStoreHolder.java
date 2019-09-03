package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.config.types.Password;

import java.security.KeyStore;

/**
 * Holder object for {@code KeyStore} and related passwords.
 */
public class KeyStoreHolder {

    private KeyStore keyStore;
    /* this will be null for truststore */
    private Password keyPassword;
    private Password keyStorePassword;

    /**
     * Use this if you are initializing this for truststore.
     * @param keyStore
     * @param keyStorePassword
     */
    public KeyStoreHolder(KeyStore keyStore, Password keyStorePassword) {
        this.keyStore = keyStore;
        this.keyStorePassword = keyStorePassword;
    }

    /**
     * Use this if you are initializing keystore.
     * @param keyStore
     * @param keyStorePassword
     * @param keyPassword
     */
    public KeyStoreHolder(KeyStore keyStore, Password keyStorePassword, Password keyPassword) {
        this(keyStore,keyStorePassword);
        this.keyPassword = keyPassword;
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    public Password getKeyPassword() {
        return keyPassword;
    }

    public Password getKeyStorePassword() {
        return keyStorePassword;
    }
}