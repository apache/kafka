package org.apache.kafka.common.replica;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetAddress;

/**
 * Holder for all the client metadata required to determine a preferred replica.
 */
public interface ClientMetadata {
    /**
     * Rack ID sent by the client
     */
    String rackId();

    /**
     * Client ID sent by the client
     */
    String clientId();

    /**
     * Incoming address of the client
     */
    InetAddress clientAddress();

    /**
     * Security principal of the client
     */
    KafkaPrincipal principal();

    /**
     * Listener name for the client
     */
    String listenerName();


    class DefaultClientMetadata implements ClientMetadata {
        public static final ClientMetadata NO_METADATA =
                new DefaultClientMetadata("", "", null, null, "");

        private final String rackId;
        private final String clientId;
        private final InetAddress clientAddress;
        private final KafkaPrincipal principal;
        private final String listenerName;

        public DefaultClientMetadata(String rackId, String clientId, InetAddress clientAddress,
                                     KafkaPrincipal principal, String listenerName) {
            this.rackId = rackId;
            this.clientId = clientId;
            this.clientAddress = clientAddress;
            this.principal = principal;
            this.listenerName = listenerName;
        }

        @Override
        public String rackId() {
            return rackId;
        }

        @Override
        public String clientId() {
            return clientId;
        }

        @Override
        public InetAddress clientAddress() {
            return clientAddress;
        }

        @Override
        public KafkaPrincipal principal() {
            return principal;
        }

        @Override
        public String listenerName() {
            return listenerName;
        }
    }
}
