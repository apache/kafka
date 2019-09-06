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
package org.apache.kafka.common;

import java.util.Objects;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.SecurityProtocol;

/**
 * Represents a broker endpoint.
 */

@InterfaceStability.Evolving
public class Endpoint {

    private final String listener;
    private final SecurityProtocol securityProtocol;
    private final String host;
    private final int port;

    public Endpoint(String listener, SecurityProtocol securityProtocol, String host, int port) {
        this.listener = listener;
        this.securityProtocol = securityProtocol;
        this.host = host;
        this.port = port;
    }

    /**
     * Returns the listener name of this endpoint.
     */
    public String listener() {
        return listener;
    }

    /**
     * Returns the security protocol of this endpoint.
     */
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    /**
     * Returns advertised host name of this endpoint.
     */
    public String host() {
        return host;
    }

    /**
     * Returns the port to which the listener is bound.
     */
    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Endpoint)) {
            return false;
        }

        Endpoint that = (Endpoint) o;
        return Objects.equals(this.listener, that.listener) &&
            Objects.equals(this.securityProtocol, that.securityProtocol) &&
            Objects.equals(this.host, that.host) &&
            this.port == that.port;

    }

    @Override
    public int hashCode() {
        return Objects.hash(listener, securityProtocol, host, port);
    }

    @Override
    public String toString() {
        return "Endpoint(" +
            "listener='" + listener + '\'' +
            ", securityProtocol=" + securityProtocol +
            ", host='" + host + '\'' +
            ", port=" + port +
            ')';
    }
}
