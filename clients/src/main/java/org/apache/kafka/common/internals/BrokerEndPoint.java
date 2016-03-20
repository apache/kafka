/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.internals;

import java.io.Serializable;

/**
 * Broker id, host and port
 */
public final class BrokerEndPoint implements Serializable {

    private int hash = 0;
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
    public int hashCode() {
        if (hash != 0)
            return hash;
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BrokerEndPoint other = (BrokerEndPoint) obj;
        if (id != other.id)
            return false;
        if (port != other.port)
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[" + id + ", " + host + ":" + port + "]";
    }

}
