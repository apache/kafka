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

/**
 * Information about a Kafka node
 */
public class Node {

    private static final Node NO_NODE = new Node(-1, "", -1);

    private final int id;
    private final String idString;
    private final String host;
    private final int port;
    private final String rack;
    private final boolean isFenced;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g. RecordAccumulator.ready)
    private Integer hash;

    public Node(int id, String host, int port) {
        this(id, host, port, null, false);
    }

    public Node(int id, String host, int port, String rack) {
        this.id = id;
        this.idString = Integer.toString(id);
        this.host = host;
        this.port = port;
        this.rack = rack;
        this.isFenced = false;
    }

    public Node(int id, String host, int port, String rack, boolean isFenced) {
        this.id = id;
        this.idString = Integer.toString(id);
        this.host = host;
        this.port = port;
        this.rack = rack;
        this.isFenced = isFenced;
    }

    public static Node noNode() {
        return NO_NODE;
    }

    /**
     * Check whether this node is empty, which may be the case if noNode() is used as a placeholder
     * in a response payload with an error.
     * @return true if it is, false otherwise
     */
    public boolean isEmpty() {
        return host == null || host.isEmpty() || port < 0;
    }

    /**
     * The node id of this node
     */
    public int id() {
        return id;
    }

    /**
     * String representation of the node id.
     * Typically the integer id is used to serialize over the wire, the string representation is used as an identifier with NetworkClient code
     */
    public String idString() {
        return idString;
    }

    /**
     * The host name for this node
     */
    public String host() {
        return host;
    }

    /**
     * The port for this node
     */
    public int port() {
        return port;
    }

    /**
     * True if this node has a defined rack
     */
    public boolean hasRack() {
        return rack != null;
    }

    /**
     * The rack for this node
     */
    public String rack() {
        return rack;
    }

    /**
     * Whether if this node is fenced
     */
    public boolean isFenced() {
        return isFenced;
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = 31 + ((host == null) ? 0 : host.hashCode());
            result = 31 * result + id;
            result = 31 * result + port;
            result = 31 * result + ((rack == null) ? 0 : rack.hashCode());
            result = 31 * result + Objects.hashCode(isFenced);
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        Node other = (Node) obj;
        return id == other.id &&
            port == other.port &&
            Objects.equals(host, other.host) &&
            Objects.equals(rack, other.rack) &&
            Objects.equals(isFenced, other.isFenced);
    }

    @Override
    public String toString() {
        return host + ":" + port + " (id: " + idString + " rack: " + rack + " isFenced: " + isFenced + ")";
    }

}
