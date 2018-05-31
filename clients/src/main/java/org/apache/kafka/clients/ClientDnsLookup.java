package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;

public enum ClientDnsLookup {

    RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY("resolve.canonical.bootstrap.servers.only"),
    DISABLED("disabled");

    private String clientDnsLookup;

    ClientDnsLookup(String clientDnsLookup) {
        this.clientDnsLookup = clientDnsLookup;
    }

    private static final Map<String, ClientDnsLookup> map =
            new HashMap<String, ClientDnsLookup>();

    static {
        for (ClientDnsLookup type : ClientDnsLookup.values()) {
            map.put(type.toString(), type);
        }
    }

    @Override
    public String toString() {
        return clientDnsLookup;
    }

    public static ClientDnsLookup fromString(String name) {
        if (map.containsKey(name)) {
            return map.get(name);
        }
        throw new IllegalArgumentException();
    }

}
