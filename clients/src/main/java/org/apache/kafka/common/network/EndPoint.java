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

package org.apache.kafka.common.network;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class EndPoint {
    private static final Pattern ENDPOINT_PATTERN =
        Pattern.compile("^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");

    public static final Map<ListenerName, SecurityProtocol> DEFAULT_SECURITY_PROTOCOL_MAP =
        Collections.unmodifiableMap(Arrays.stream(SecurityProtocol.values()).collect(
                Collectors.toMap(sp -> ListenerName.forSecurityProtocol(sp), sp -> sp)));

    private final ListenerName listenerName;
    private final SecurityProtocol securityProtocol;
    private final String host;
    private final int port;

    public static EndPoint parse(String connectionString) {
        return parse(connectionString, DEFAULT_SECURITY_PROTOCOL_MAP);
    }

    public static EndPoint parse(String connectionString,
                                 Map<ListenerName, SecurityProtocol> securityProtocolMap) {
        Matcher matcher = ENDPOINT_PATTERN.matcher(connectionString);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid connection string format: " + connectionString);
        }
        String nameStr = matcher.group(1);
        String hostStr = matcher.group(2);
        String portStr = matcher.group(3);
        ListenerName listenerName  = ListenerName.normalised(nameStr);
        SecurityProtocol securityProtocol = securityProtocolMap.get(listenerName);
        if (securityProtocol == null) {
            throw new IllegalArgumentException("No security protocol defined for listener " +
                listenerName);
        }
        int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Unable to parse port of " + connectionString, e);
        }
        return new EndPoint(hostStr, port, listenerName, securityProtocol);
    }

    public EndPoint(String host, int port, SecurityProtocol securityProtocol) {
        this(host, port, ListenerName.forSecurityProtocol(securityProtocol),
            securityProtocol);
    }

    public EndPoint(String host, int port, ListenerName listenerName,
                    SecurityProtocol securityProtocol) {
        this.listenerName = Objects.requireNonNull(listenerName);
        this.securityProtocol = Objects.requireNonNull(securityProtocol);
        this.host = Objects.requireNonNull(host);
        this.port = port;
    }

    public ListenerName listenerName() {
        return listenerName;
    }

    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EndPoint)) {
            return false;
        }
        EndPoint o = (EndPoint) other;
        return this.listenerName.equals(o.listenerName) &&
            this.host.equals(o.host) &&
            this.port == o.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(listenerName, host, port);
    }

    public InetSocketAddress address() throws UnknownHostException {
        String effectiveHost = host.isEmpty() ? "0.0.0.0" : host;
        return InetSocketAddress.createUnresolved(effectiveHost, port);
    }

    public String connectionString() {
        return this.listenerName.value() + "://" + Utils.formatAddress(host, port);
    }

    @Override
    public String toString() {
        return connectionString();
    }
}
