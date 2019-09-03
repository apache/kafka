package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.network.Mode;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.security.KeyStore;
import java.util.Map;
import java.util.Set;

public interface ISslEngineBuilder {

    /**
     * Builds a new SSLEngine object.
     *
     * @param mode      Whether to use client or server mode.
     * @param peerHost  The peer host to use. This is used in client mode if endpoint validation is enabled.
     * @param peerPort  The peer port to use. This is a hint and not used for validation.
     * @param endpointIdentification Endpoint identification algorithm for client mode.
     * @return          The new SSLEngine.
     */
    SSLEngine build(Mode mode, String peerHost, int peerPort, String endpointIdentification);

    /**
     * Retruns the SSLContext built by this builder.
     * @return
     */
    SSLContext getSSLContext();

    /**
     * Returns the currently used configurations by this builder.
     * @return
     */
    Map<String, Object> currentConfigs();

    /**
     * Returns the reconfigurable configs this engine builder may need to use.
     * @return
     */
    Set<String> reconfigurableConfigs();

    /**
     * Returns true if this SslEngineBuilder needs to be rebuilt.
     *
     * @param nextConfigs       The configuration we want to use.
     * @return                  True only if this builder should be rebuilt.
     */
    boolean shouldBeRebuilt(Map<String, Object> nextConfigs);

    /**
     * Returns the {@code KeyStore} for the keystore used by this builder.
     * @return
     */
    KeyStore keystore();

    /**
     * Returns the {@code KeyStore} for the truststore used by this builder.
     * @return
     */
    KeyStore truststore();
}
