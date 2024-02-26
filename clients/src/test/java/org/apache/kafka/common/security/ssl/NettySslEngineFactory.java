package org.apache.kafka.common.security.ssl;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NettySslEngineFactory extends DefaultSslEngineFactory {
    private static final Logger log = LoggerFactory.getLogger(NettySslEngineFactory.class);
    private SslContext nettyServerSslContext;
    private SslContext nettyClientSslContext;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(configs);
        this.protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);
        SecurityUtils.addConfiguredSecurityProviders(this.configs);

        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty()) {
            this.cipherSuites = cipherSuitesList.toArray(new String[0]);
        } else {
            this.cipherSuites = null;
        }

        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty()) {
            this.enabledProtocols = enabledProtocolsList.toArray(new String[0]);
        } else {
            this.enabledProtocols = null;
        }

        this.secureRandomImplementation = createSecureRandom((String)
                configs.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG));

        this.sslClientAuth = createSslClientAuth((String) configs.get(
                BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));

        this.kmfAlgorithm = (String) configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        this.tmfAlgorithm = (String) configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);

        this.keystore = createKeystore((String) configs.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_KEY_CONFIG),
                (Password) configs.get(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));

        this.truststore = createTruststore((String) configs.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG),
                (String) configs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG),
                (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG));

        nettyServerSslContext = createNettyServerSslContext(keystore, truststore);
        nettyClientSslContext = createNettyClientSslContext(keystore, truststore);
    }

    @Override
    public void close() {
        super.close();
        nettyServerSslContext = null;
        nettyClientSslContext = null;
    }

    @Override
    protected SSLEngine createSslEngine(Mode mode, String peerHost, int peerPort, String endpointIdentification) {
        SSLEngine sslEngine = mode == Mode.SERVER ? nettyServerSslContext.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort) :
                nettyClientSslContext.newEngine(ByteBufAllocator.DEFAULT, peerHost, peerPort);
        if (cipherSuites != null) sslEngine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null) sslEngine.setEnabledProtocols(enabledProtocols);

        if (mode == Mode.SERVER) {
            sslEngine.setUseClientMode(false);
            switch (sslClientAuth) {
                case REQUIRED:
                    sslEngine.setNeedClientAuth(true);
                    break;
                case REQUESTED:
                    sslEngine.setWantClientAuth(true);
                    break;
                case NONE:
                    break;
            }
        } else {
            sslEngine.setUseClientMode(true);
            SSLParameters sslParams = sslEngine.getSSLParameters();
            // SSLParameters#setEndpointIdentificationAlgorithm enables endpoint validation
            // only in client mode. Hence, validation is enabled only for clients.
            sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
            sslEngine.setSSLParameters(sslParams);
        }
        return sslEngine;
    }

    private SslContext createNettySslContext(Mode mode, SecurityStore keystore, SecurityStore truststore) {
        try {
            String kmfAlgorithm = this.kmfAlgorithm != null ?
                    this.kmfAlgorithm : KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
            if (keystore != null) {
                kmf.init(keystore.get(), keystore.keyPassword());
            } else {
                kmf.init(null, null);
            }
            SslContextBuilder sslContextBuilder = mode == Mode.SERVER ? SslContextBuilder.forServer(kmf).protocols(protocol) :
                    SslContextBuilder.forClient().keyManager(kmf).protocols(protocol);

            String tmfAlgorithm = this.tmfAlgorithm != null ? this.tmfAlgorithm : TrustManagerFactory.getDefaultAlgorithm();
            KeyStore ts = truststore == null ? null : truststore.get();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            tmf.init(ts);

            sslContextBuilder.trustManager(tmf);

            if (provider != null) {
                try {
                    sslContextBuilder.sslProvider(SslProvider.valueOf(provider));
                } catch (IllegalArgumentException e) {
                    log.error("Invalid SSL provider {}", provider, e);
                    throw e;
                }
            }

            SslContext sslContext = sslContextBuilder.build();
            log.debug("Created SSL context with keystore {}, truststore {}, provider {}.",
                    keystore, truststore, provider);
            return sslContext;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private SslContext createNettyClientSslContext(SecurityStore keystore, SecurityStore truststore) {
        return createNettySslContext(Mode.CLIENT, keystore, truststore);
    }

    private SslContext createNettyServerSslContext(SecurityStore keystore, SecurityStore truststore) {
        return createNettySslContext(Mode.SERVER, keystore, truststore);
    }
}
