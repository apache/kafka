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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

import java.util.Locale;
import java.util.Objects;

public class EndPoint {
    private final String host;
    private final int port;
    private final ListenerName listenerName;
    private final SecurityProtocol securityProtocol;

    public EndPoint(String host, int port, ListenerName listenerName, SecurityProtocol securityProtocol) {
        this.host = host;
        this.port = port;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
    }

    public static String parseListenerName(String connectionString) {
        int firstColon = connectionString.indexOf(':');
        if (firstColon < 0) {
            throw new KafkaException("Unable to parse a listener name from " + connectionString);
        }
        return connectionString.substring(0, firstColon).toUpperCase(Locale.ROOT);
    }

    public static EndPoint fromPublic(org.apache.kafka.common.Endpoint endpoint) {
        return new EndPoint(endpoint.host(), endpoint.port(),
                new ListenerName(endpoint.listenerName().get()), endpoint.securityProtocol());
    }

    public String connectionString() {
        String hostport = (host == null) ? (":" + port) : Utils.formatAddress(host, port);
        return listenerName.value() + "://" + hostport;
    }

    public org.apache.kafka.common.Endpoint toPublic() {
        return new org.apache.kafka.common.Endpoint(listenerName.value(), securityProtocol, host, port);
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public ListenerName listenerName() {
        return listenerName;
    }

    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndPoint endPoint = (EndPoint) o;
        return port == endPoint.port &&
                Objects.equals(host, endPoint.host) &&
                Objects.equals(listenerName, endPoint.listenerName) &&
                securityProtocol == endPoint.securityProtocol;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, listenerName, securityProtocol);
    }

    @Override
    public String toString() {
        return "EndPoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", listenerName=" + listenerName +
                ", securityProtocol=" + securityProtocol +
                '}';
    }
}
