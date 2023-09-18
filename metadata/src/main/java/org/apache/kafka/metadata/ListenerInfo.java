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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterControllerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;


/**
 * ListenerInfo contains information about the listeners of either a controller or a broker.
 * ListenerInfo objects are immutable; they cannot be modified once created. The intention is
 * that you store either controller listeners or broker listeners here, but not both. On a
 * combined KRaft node, which has both broker and controller roles, you would have two
 * separate ListenerInfo objects to represent the listeners of each role.
 *
 * Listener information is stored in a linked hash map. This maintains ordering while still
 * allowing the traditional O(1) hash map access. By convention, the first listener is special,
 * corresponding to either the inter-controller listener or the inter-broker listener.
 * This is the only listener that other nodes will attempt to use to communicate with this node.
 *
 * You may wonder why nodes support multiple listeners, given that inter-cluster communication only
 * ever uses the first one. Well, one reason is that external clients may wish to use the additional
 * listeners. It is a good practice to separate external and internal traffic. In some cases,
 * external traffic may be encrypted while internal traffic is not. (Although other admins may wish
 * to encrypt everything.) Another reason is that supporting multiple listeners allows us to change
 * the effective inter-cluster listener via a roll. During such a roll, half of the brokers
 * (or controllers) might be using one listener, while the other half use another. This lets us,
 * for example, transition from using a PLAINTEXT inter broker listener to using an SSL one without
 * taking any downtime.
 *
 * The ListenerInfo class is intended to handle translating endpoint information between various
 * different data structures, and also to handle the two big gotchas of Kafka endpoints.
 *
 * The first gotcha is that the hostname will be null or blank if we are listening on 0.0.0.0.
 * The withWildcardHostnamesResolved function creates a ListenerInfo object where all such hostnames
 * are replaced by specific hostnames. (It's not perfect because we have to choose a single hostname
 * out of multiple possibilities. In production scenarios it would be better to set the desired
 * hostname explicitly in the configuration rather than binding to 0.0.0.0.)
 *
 * The second gotcha is that if someone configures an ephemeral port (aka port 0), we need to fill
 * in the port which is chosen at runtime. The withEphemeralPortsCorrected resolves this by filling
 * in the missing information for ephemeral ports.
 */
final public class ListenerInfo {
    private final static Logger log = LoggerFactory.getLogger(ListenerInfo.class);

    /**
     * Create a ListenerInfo from data in a ControllerRegistrationRequest RPC.
     *
     * @param collection    The RPC data.
     *
     * @return              The ListenerInfo object.
     */
    public static ListenerInfo fromControllerRegistrationRequest(
        ControllerRegistrationRequestData.ListenerCollection collection
    ) {
        LinkedHashMap<String, Endpoint> listeners = new LinkedHashMap<>();
        collection.forEach(listener -> {
            SecurityProtocol protocol = SecurityProtocol.forId(listener.securityProtocol());
            if (protocol == null) {
                throw new RuntimeException("Unknown security protocol " +
                    (int) listener.securityProtocol() + " in listener " + listener.name());
            }
            listeners.put(listener.name(), new Endpoint(listener.name(),
                protocol,
                listener.host(),
                listener.port()));
        });
        return new ListenerInfo(listeners);
    }

    /**
     * Create a ListenerInfo from data in a RegisterControllerRecord.
     *
     * @param collection    The record data.
     *
     * @return              The ListenerInfo object.
     */
    public static ListenerInfo fromControllerRegistrationRecord(
        RegisterControllerRecord.ControllerEndpointCollection collection
    ) {
        LinkedHashMap<String, Endpoint> listeners = new LinkedHashMap<>();
        collection.forEach(listener -> {
            SecurityProtocol protocol = SecurityProtocol.forId(listener.securityProtocol());
            if (protocol == null) {
                throw new RuntimeException("Unknown security protocol " +
                    (int) listener.securityProtocol() + " in listener " + listener.name());
            }
            listeners.put(listener.name(), new Endpoint(listener.name(),
                protocol,
                listener.host(),
                listener.port()));
        });
        return new ListenerInfo(listeners);
    }

    /**
     * Create a ListenerInfo from data in a BrokerRegistrationRequest RPC.
     *
     * @param collection    The RPC data.
     *
     * @return              The ListenerInfo object.
     */
    public static ListenerInfo fromBrokerRegistrationRequest(
        BrokerRegistrationRequestData.ListenerCollection collection
    ) {
        LinkedHashMap<String, Endpoint> listeners = new LinkedHashMap<>();
        collection.forEach(listener -> {
            SecurityProtocol protocol = SecurityProtocol.forId(listener.securityProtocol());
            if (protocol == null) {
                throw new RuntimeException("Unknown security protocol " +
                        (int) listener.securityProtocol() + " in listener " + listener.name());
            }
            listeners.put(listener.name(), new Endpoint(listener.name(),
                protocol,
                listener.host(),
                listener.port()));
        });
        return new ListenerInfo(listeners);
    }

    /**
     * Create a ListenerInfo from data in a RegisterBrokerRecord.
     *
     * @param collection    The record data.
     *
     * @return              The ListenerInfo object.
     */
    public static ListenerInfo fromBrokerRegistrationRecord(
        RegisterBrokerRecord.BrokerEndpointCollection collection
    ) {
        LinkedHashMap<String, Endpoint> listeners = new LinkedHashMap<>();
        collection.forEach(listener -> {
            SecurityProtocol protocol = SecurityProtocol.forId(listener.securityProtocol());
            if (protocol == null) {
                throw new RuntimeException("Unknown security protocol " +
                        (int) listener.securityProtocol() + " in listener " + listener.name());
            }
            listeners.put(listener.name(), new Endpoint(listener.name(),
                    protocol,
                    listener.host(),
                    listener.port()));
        });
        return new ListenerInfo(listeners);
    }

    public static ListenerInfo create(
        List<Endpoint> rawListeners
    ) {
        return create(Optional.empty(), rawListeners);
    }

    public static ListenerInfo create(
        Optional<String> firstListenerName,
        List<Endpoint> rawListeners
    ) {
        LinkedHashMap<String, Endpoint> listeners = new LinkedHashMap<>();
        for (Endpoint listener : rawListeners) {
            String name = listener.listenerName().get();
            if (Optional.of(name).equals(firstListenerName)) {
                listeners.put(name, listener);
                break;
            }
        }
        for (Endpoint listener : rawListeners) {
            String name = listener.listenerName().get();
            if (!Optional.of(name).equals(firstListenerName)) {
                listeners.put(name, listener);
            }
        }
        return new ListenerInfo(listeners);
    }

    /**
     * An ordered map containing all of the listeners. The first listener is special, indicating
     * either the inter-broker or inter-controller listener.
     */
    private final Map<String, Endpoint> listeners;

    private ListenerInfo(Map<String, Endpoint> listeners) {
        this.listeners = Collections.unmodifiableMap(listeners);
    }

    public Map<String, Endpoint> listeners() {
        return listeners;
    }

    public Endpoint firstListener() {
        if (listeners.isEmpty()) {
            throw new RuntimeException("No listeners found.");
        }
        return listeners.values().iterator().next();
    }

    /**
     * Create a new ListenerInfo object where null or blank hostnames (signifying that the user
     * asked to bind to 0.0.0.0) are replaced by specific hostnames.
     *
     * @return A new ListenerInfo object.
     */
    public ListenerInfo withWildcardHostnamesResolved() throws UnknownHostException {
        LinkedHashMap<String, Endpoint> newListeners = new LinkedHashMap<>();
        for (Map.Entry<String, Endpoint> entry : listeners.entrySet()) {
            if (entry.getValue().host() == null || entry.getValue().host().trim().isEmpty()) {
                String newHost = InetAddress.getLocalHost().getCanonicalHostName();
                Endpoint prevEndpoint = entry.getValue();
                newListeners.put(entry.getKey(), new Endpoint(prevEndpoint.listenerName().get(),
                        prevEndpoint.securityProtocol(),
                        newHost,
                        prevEndpoint.port()));
                log.info("{}: resolved wildcard host to {}", entry.getValue().listenerName().get(),
                        newHost);
            } else {
                newListeners.put(entry.getKey(), entry.getValue());
            }
        }
        return new ListenerInfo(newListeners);
    }

    /**
     * Create a new ListenerInfo object where ephemeral ports are populated with their true runtime
     * values.
     *
     * In other words, if a port was set to 0, indicating that a random port should be assigned by the
     * operating system, this function will replace it with the value the operating system actually
     * chose.
     *
     * @param getBoundPortCallback  The callback used to correct ephemeral endpoints.
     *
     * @return A new ListenerInfo object.
     */
    public ListenerInfo withEphemeralPortsCorrected(Function<String, Integer> getBoundPortCallback) {
        LinkedHashMap<String, Endpoint> newListeners = new LinkedHashMap<>();
        for (Map.Entry<String, Endpoint> entry : listeners.entrySet()) {
            if (entry.getValue().port() == 0) {
                Endpoint prevEndpoint = entry.getValue();
                int newPort = getBoundPortCallback.apply(entry.getKey());
                checkPortIsSerializable(newPort);
                log.info("{}: resolved ephemeral port to {}", entry.getValue().listenerName().get(),
                        newPort);
                newListeners.put(entry.getKey(), new Endpoint(prevEndpoint.listenerName().get(),
                        prevEndpoint.securityProtocol(),
                        prevEndpoint.host(),
                        newPort));
            } else {
                newListeners.put(entry.getKey(), entry.getValue());
            }
        }
        return new ListenerInfo(newListeners);
    }

    private static void checkPortIsSerializable(int port) {
        if (port == 0) {
            throw new RuntimeException("Cannot serialize ephemeral port 0 in ListenerInfo.");
        } else if (port < 0) {
            throw new RuntimeException("Cannot serialize negative port number " + port +
                " in ListenerInfo.");
        } else if (port > 65535) {
            throw new RuntimeException("Cannot serialize invalid port number " + port +
                " in ListenerInfo.");
        }
    }

    private static void checkHostIsSerializable(String host) {
        if (host == null) {
            throw new RuntimeException("Cannot serialize null host in ListenerInfo.");
        } else if (host.trim().isEmpty()) {
            throw new RuntimeException("Cannot serialize empty host in ListenerInfo.");
        }
    }

    public ControllerRegistrationRequestData.ListenerCollection toControllerRegistrationRequest() {
        ControllerRegistrationRequestData.ListenerCollection collection =
            new ControllerRegistrationRequestData.ListenerCollection();
        listeners.values().forEach(endpoint -> {
            checkPortIsSerializable(endpoint.port());
            checkHostIsSerializable(endpoint.host());
            collection.add(new ControllerRegistrationRequestData.Listener().
                setHost(endpoint.host()).
                setName(endpoint.listenerName().get()).
                setPort(endpoint.port()).
                setSecurityProtocol(endpoint.securityProtocol().id));
        });
        return collection;
    }

    public RegisterControllerRecord.ControllerEndpointCollection toControllerRegistrationRecord() {
        RegisterControllerRecord.ControllerEndpointCollection collection =
                new RegisterControllerRecord.ControllerEndpointCollection();
        listeners.values().forEach(endpoint -> {
            checkPortIsSerializable(endpoint.port());
            checkHostIsSerializable(endpoint.host());
            collection.add(new RegisterControllerRecord.ControllerEndpoint().
                setHost(endpoint.host()).
                setName(endpoint.listenerName().get()).
                setPort(endpoint.port()).
                setSecurityProtocol(endpoint.securityProtocol().id));
        });
        return collection;
    }

    public BrokerRegistrationRequestData.ListenerCollection toBrokerRegistrationRequest() {
        BrokerRegistrationRequestData.ListenerCollection collection =
                new BrokerRegistrationRequestData.ListenerCollection();
        listeners.values().forEach(endpoint -> {
            checkPortIsSerializable(endpoint.port());
            checkHostIsSerializable(endpoint.host());
            collection.add(new BrokerRegistrationRequestData.Listener().
                setHost(endpoint.host()).
                setName(endpoint.listenerName().get()).
                setPort(endpoint.port()).
                setSecurityProtocol(endpoint.securityProtocol().id));
        });
        return collection;
    }

    public RegisterBrokerRecord.BrokerEndpointCollection toBrokerRegistrationRecord() {
        RegisterBrokerRecord.BrokerEndpointCollection collection =
                new RegisterBrokerRecord.BrokerEndpointCollection();
        listeners.values().forEach(endpoint -> {
            checkPortIsSerializable(endpoint.port());
            checkHostIsSerializable(endpoint.host());
            collection.add(new RegisterBrokerRecord.BrokerEndpoint().
                setHost(endpoint.host()).
                setName(endpoint.listenerName().get()).
                setPort(endpoint.port()).
                setSecurityProtocol(endpoint.securityProtocol().id));
        });
        return collection;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!(o.getClass().equals(ListenerInfo.class)))) return false;
        ListenerInfo other = (ListenerInfo) o;
        return listeners.equals(other.listeners) &&
                firstListener().equals(other.firstListener());
    }

    @Override
    public int hashCode() {
        return Objects.hash(listeners);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("ListenerInfo(");
        String prefix = "";
        for (Endpoint endpoint : listeners.values()) {
            bld.append(prefix).append(endpoint);
            prefix = ", ";
        }
        bld.append(")");
        return bld.toString();
    }
}
