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
package org.apache.kafka.server.network;

import java.util.Objects;

/**
 * BrokerEndPoint is used to connect to specific host:port pair.
 * It is typically used by clients (or brokers when connecting to other brokers)
 * and contains no information about the security protocol used on the connection.
 * Clients should know which security protocol to use from configuration.
 * This allows us to keep the wire protocol with the clients unchanged where the protocol is not needed.
 */
public class BrokerEndPoint {

    private final int id;
    private final String host;
    private final int port;

    public BrokerEndPoint(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrokerEndPoint that = (BrokerEndPoint) o;
        return id == that.id && host.equals(that.host) && port == that.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, host, port);
    }

    public String toString() {
        return String.format("BrokerEndPoint(id=%s, host=%s:%s)", id, host, port);
    }
}
