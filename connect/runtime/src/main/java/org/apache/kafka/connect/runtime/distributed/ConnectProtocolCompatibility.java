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
package org.apache.kafka.connect.runtime.distributed;

import java.util.Locale;

/**
 * An enumeration of the modes available to the worker to signal which Connect protocols are
 * enabled at any time.
 *
 * {@code EAGER} signifies that this worker only supports prompt release of assigned connectors
 * and tasks in every rebalance. Corresponds to Connect protocol V0.
 *
 * {@code COMPATIBLE} signifies that this worker supports both eager and incremental cooperative
 * Connect protocols and will use the version that is elected by the Kafka broker coordinator
 * during rebalance.
 *
 * {@code SESSIONED} signifies that this worker supports all of the above protocols in addition to
 * a protocol that uses incremental cooperative rebalancing for worker assignment and uses session
 * keys distributed via the config topic to verify internal REST requests
 */
public enum ConnectProtocolCompatibility {
    EAGER {
        @Override
        public String protocol() {
            return "default";
        }

        @Override
        public short protocolVersion() {
            return 0;
        }
    },

    COMPATIBLE {
        @Override
        public String protocol() {
            return "compatible";
        }

        @Override
        public short protocolVersion() {
            return 1;
        }
    },

    SESSIONED {
        @Override
        public String protocol() {
            return "sessioned";
        }

        @Override
        public short protocolVersion() {
            return 2;
        }
    };

    /**
     * Return the enum that corresponds to the name that is given as an argument;
     * if no mapping is found {@code IllegalArgumentException} is thrown.
     *
     * @param name the name of the protocol compatibility mode
     * @return the enum that corresponds to the protocol compatibility mode
     */
    public static ConnectProtocolCompatibility fromName(String name) {
        if (EAGER.name().equalsIgnoreCase(name)) return EAGER;
        if (COMPATIBLE.name().equalsIgnoreCase(name)) return COMPATIBLE;
        if (SESSIONED.name().equalsIgnoreCase(name)) return SESSIONED;
        throw new IllegalArgumentException("Unknown Connect protocol compatibility mode: " + name);
    }

    /**
     * Return the enum that corresponds to the Connect protocol version that is given as an argument;
     * if no mapping is found {@code IllegalArgumentException} is thrown.
     *
     * @param protocolVersion the version of the protocol;
     * @return the enum that corresponds to the protocol compatibility mode
     */
    public static ConnectProtocolCompatibility fromProtocolVersion(short protocolVersion) {
        if (EAGER.protocolVersion() == protocolVersion) return EAGER;
        if (COMPATIBLE.protocolVersion() == protocolVersion) return COMPATIBLE;
        if (SESSIONED.protocolVersion() == protocolVersion) return SESSIONED;
        throw new IllegalArgumentException("Unknown Connect protocol version: " + protocolVersion);
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Return the version of the protocol for this mode.
     *
     * @return the protocol version
     */
    public abstract short protocolVersion();

    /**
     * Return the name of the protocol that this mode will use in {@code ProtocolMetadata}.
     *
     * @return the protocol name
     */
    public abstract String protocol();

    /**
     * Return the enum that corresponds to the protocol name that is given as an argument;
     * if no mapping is found {@code IllegalArgumentException} is thrown.
     *
     * @param protocolName the name of the connect protocol
     * @return the enum that corresponds to the protocol compatibility mode that supports the
     * given protocol
     */
    public static ConnectProtocolCompatibility fromProtocol(String protocolName) {
        if (EAGER.protocol().equalsIgnoreCase(protocolName)) return EAGER;
        if (COMPATIBLE.protocol().equalsIgnoreCase(protocolName)) return COMPATIBLE;
        if (SESSIONED.protocol().equalsIgnoreCase(protocolName)) return SESSIONED;
        throw new IllegalArgumentException("Not found Connect protocol compatibility mode for protocol: " + protocolName);
    }
}
