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
package org.apache.kafka.network;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.config.ReplicationConfigs;
import java.util.Arrays;
import java.util.stream.Collectors;

public class SocketServerConfigs {
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_CONFIG = "listener.security.protocol.map";
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT = Arrays.stream(SecurityProtocol.values())
            .collect(Collectors.toMap(ListenerName::forSecurityProtocol, sp -> sp))
            .entrySet()
            .stream()
            .map(entry -> entry.getKey().value() + ":" + entry.getValue().name())
            .collect(Collectors.joining(","));
    public static final String LISTENER_SECURITY_PROTOCOL_MAP_DOC = "Map between listener names and security protocols. This must be defined for " +
            "the same security protocol to be usable in more than one port or IP. For example, internal and " +
            "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners " +
            "with names INTERNAL and EXTERNAL and this property as: <code>INTERNAL:SSL,EXTERNAL:SSL</code>. As shown, key and value are " +
            "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. " +
            "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
            "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the " +
            "INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> would be set. " +
            "If the config for the listener name is not set, the config will fallback to the generic config (i.e. <code>ssl.keystore.location</code>). " +
            "Note that in KRaft a default mapping from the listener names defined by <code>controller.listener.names</code> to PLAINTEXT " +
            "is assumed if no explicit mapping is provided and no other security protocol is in use.";

    public static final String LISTENERS_CONFIG = "listeners";
    public static final String LISTENERS_DEFAULT = "PLAINTEXT://:9092";
    public static final String LISTENERS_DOC = "Listener List - Comma-separated list of URIs we will listen on and the listener names." +
            String.format(" If the listener name is not a security protocol, <code>%s</code> must also be set.%n", LISTENER_SECURITY_PROTOCOL_MAP_CONFIG) +
            " Listener names and port numbers must be unique unless %n" +
            " one listener is an IPv4 address and the other listener is %n" +
            " an IPv6 address (for the same port).%n" +
            " Specify hostname as 0.0.0.0 to bind to all interfaces.%n" +
            " Leave hostname empty to bind to default interface.%n" +
            " Examples of legal listener lists:%n" +
            " <code>PLAINTEXT://myhost:9092,SSL://:9091</code>%n" +
            " <code>CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093</code>%n" +
            " <code>PLAINTEXT://127.0.0.1:9092,SSL://[::1]:9092</code>%n";

    public static final String ADVERTISED_LISTENERS_CONFIG = "advertised.listeners";
    public static final String ADVERTISED_LISTENERS_DOC = String.format(
            "Listeners to publish to ZooKeeper for clients to use, if different than the <code>%s</code> config property." +
                    " In IaaS environments, this may need to be different from the interface to which the broker binds." +
                    " If this is not set, the value for <code>%1$1s</code> will be used." +
                    " Unlike <code>%1$1s</code>, it is not valid to advertise the 0.0.0.0 meta-address.%n" +
                    " Also unlike <code>%1$1s</code>, there can be duplicated ports in this property," +
                    " so that one listener can be configured to advertise another listener's address." +
                    " This can be useful in some cases where external load balancers are used.", LISTENERS_CONFIG);


    public static final String CONTROL_PLANE_LISTENER_NAME_CONFIG = "control.plane.listener.name";
    public static final String CONTROL_PLANE_LISTENER_NAME_DOC = String.format(
            "Name of listener used for communication between controller and brokers. " +
                    "A broker will use the <code>%s</code> to locate the endpoint in %s list, to listen for connections from the controller. " +
                    "For example, if a broker's config is:%n" +
                    "<code>listeners = INTERNAL://192.1.1.8:9092, EXTERNAL://10.1.1.5:9093, CONTROLLER://192.1.1.8:9094" +
                    "listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL" +
                    "control.plane.listener.name = CONTROLLER</code>%n" +
                    "On startup, the broker will start listening on \"192.1.1.8:9094\" with security protocol \"SSL\".%n" +
                    "On the controller side, when it discovers a broker's published endpoints through ZooKeeper, it will use the <code>%1$1s</code> " +
                    "to find the endpoint, which it will use to establish connection to the broker.%n" +
                    "For example, if the broker's published endpoints on ZooKeeper are:%n" +
                    " <code>\"endpoints\" : [\"INTERNAL://broker1.example.com:9092\",\"EXTERNAL://broker1.example.com:9093\",\"CONTROLLER://broker1.example.com:9094\"]</code>%n" +
                    " and the controller's config is:%n" +
                    "<code>listener.security.protocol.map = INTERNAL:PLAINTEXT, EXTERNAL:SSL, CONTROLLER:SSL" +
                    "control.plane.listener.name = CONTROLLER</code>%n" +
                    "then the controller will use \"broker1.example.com:9094\" with security protocol \"SSL\" to connect to the broker.%n" +
                    "If not explicitly configured, the default value will be null and there will be no dedicated endpoints for controller connections.%n" +
                    "If explicitly configured, the value cannot be the same as the value of <code>%s</code>.",
            CONTROL_PLANE_LISTENER_NAME_CONFIG, LISTENERS_CONFIG, ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG);

    public static final String SOCKET_SEND_BUFFER_BYTES_CONFIG = "socket.send.buffer.bytes";
    public static final int SOCKET_SEND_BUFFER_BYTES_DEFAULT = 100 * 1024;
    public static final String SOCKET_SEND_BUFFER_BYTES_DOC = "The SO_SNDBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";

    public static final String SOCKET_RECEIVE_BUFFER_BYTES_CONFIG = "socket.receive.buffer.bytes";
    public static final int SOCKET_RECEIVE_BUFFER_BYTES_DEFAULT = 100 * 1024;
    public static final String SOCKET_RECEIVE_BUFFER_BYTES_DOC = "The SO_RCVBUF buffer of the socket server sockets. If the value is -1, the OS default will be used.";

    public static final String SOCKET_REQUEST_MAX_BYTES_CONFIG = "socket.request.max.bytes";
    public static final int SOCKET_REQUEST_MAX_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String SOCKET_REQUEST_MAX_BYTES_DOC = "The maximum number of bytes in a socket request";

    public static final String SOCKET_LISTEN_BACKLOG_SIZE_CONFIG = "socket.listen.backlog.size";
    public static final int SOCKET_LISTEN_BACKLOG_SIZE_DEFAULT = 50;
    public static final String SOCKET_LISTEN_BACKLOG_SIZE_DOC = "The maximum number of pending connections on the socket. " +
            "In Linux, you may also need to configure <code>somaxconn</code> and <code>tcp_max_syn_backlog</code> kernel parameters " +
            "accordingly to make the configuration takes effect.";

    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG = "max.connections.per.ip.overrides";
    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_DEFAULT = "";
    public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_DOC = "A comma-separated list of per-ip or hostname overrides to the default maximum number of connections. " +
            "An example value is \"hostName:100,127.0.0.1:200\"";

    public static final String MAX_CONNECTIONS_PER_IP_CONFIG = "max.connections.per.ip";
    public static final int MAX_CONNECTIONS_PER_IP_DEFAULT = Integer.MAX_VALUE;
    public static final String MAX_CONNECTIONS_PER_IP_DOC = "The maximum number of connections we allow from each ip address. This can be set to 0 if there are overrides " +
            String.format("configured using %s property. New connections from the ip address are dropped if the limit is reached.", MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG);

    public static final String MAX_CONNECTIONS_CONFIG = "max.connections";
    public static final int MAX_CONNECTIONS_DEFAULT = Integer.MAX_VALUE;
    public static final String MAX_CONNECTIONS_DOC = String.format(
            "The maximum number of connections we allow in the broker at any time. This limit is applied in addition " +
                    "to any per-ip limits configured using %s. Listener-level limits may also be configured by prefixing the " +
                    "config name with the listener prefix, for example, <code>listener.name.internal.%1$1s</code>. Broker-wide limit " +
                    "should be configured based on broker capacity while listener limits should be configured based on application requirements. " +
                    "New connections are blocked if either the listener or broker limit is reached. Connections on the inter-broker listener are " +
                    "permitted even if broker-wide limit is reached. The least recently used connection on another listener will be closed in this case.",
            MAX_CONNECTIONS_PER_IP_CONFIG);

    public static final String MAX_CONNECTION_CREATION_RATE_CONFIG = "max.connection.creation.rate";
    public static final int MAX_CONNECTION_CREATION_RATE_DEFAULT = Integer.MAX_VALUE;
    public static final String MAX_CONNECTION_CREATION_RATE_DOC = "The maximum connection creation rate we allow in the broker at any time. Listener-level limits " +
            String.format("may also be configured by prefixing the config name with the listener prefix, for example, <code>listener.name.internal.%s</code>.", MAX_CONNECTION_CREATION_RATE_CONFIG) +
            "Broker-wide connection rate limit should be configured based on broker capacity while listener limits should be configured based on " +
            "application requirements. New connections will be throttled if either the listener or the broker limit is reached, with the exception " +
            "of inter-broker listener. Connections on the inter-broker listener will be throttled only when the listener-level rate limit is reached.";

    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
    public static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = 10 * 60 * 1000L;
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = "Idle connections timeout: the server socket processor threads close the connections that idle more than this";

    public static final String FAILED_AUTHENTICATION_DELAY_MS_CONFIG = "connection.failed.authentication.delay.ms";
    public static final int FAILED_AUTHENTICATION_DELAY_MS_DEFAULT = 100;
    public static final String FAILED_AUTHENTICATION_DELAY_MS_DOC = "Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure. " +
            String.format("This must be configured to be less than %s to prevent connection timeout.", CONNECTIONS_MAX_IDLE_MS_CONFIG);
}
