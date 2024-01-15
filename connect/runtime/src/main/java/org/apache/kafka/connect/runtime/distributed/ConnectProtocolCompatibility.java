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

import java.util.Arrays;
import java.util.Locale;

import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V1;
import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol.CONNECT_PROTOCOL_V2;

/**
 * An enumeration of the modes available to the worker to signal which Connect protocols are
 * enabled at any time.
 * <p>
 * {@code EAGER} signifies that this worker only supports prompt release of assigned connectors
 * and tasks in every rebalance. Corresponds to Connect protocol V0.
 * <p>
 * {@code COMPATIBLE} signifies that this worker supports both eager and incremental cooperative
 * Connect protocols and will use the version that is elected by the Kafka broker coordinator
 * during rebalance.
 * <p>
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
            return CONNECT_PROTOCOL_V0;
        }
    },

    COMPATIBLE {
        @Override
        public String protocol() {
            return "compatible";
        }

        @Override
        public short protocolVersion() {
            return CONNECT_PROTOCOL_V1;
        }
    },

    SESSIONED {
        @Override
        public String protocol() {
            return "sessioned";
        }

        @Override
        public short protocolVersion() {
            return CONNECT_PROTOCOL_V2;
        }
    };

    /**
     * Return the enum that corresponds to the name that is given as an argument;
     * if no mapping is found {@code IllegalArgumentException} is thrown.
     *
     * @param name the name of the protocol compatibility mode
     * @return the enum that corresponds to the protocol compatibility mode
     */
    public static ConnectProtocolCompatibility compatibility(String name) {
        return Arrays.stream(ConnectProtocolCompatibility.values())
                .filter(mode -> mode.name().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unknown Connect protocol compatibility mode: " + name));
    }

    /**
     * Return the enum that corresponds to the Connect protocol version that is given as an argument;
     * if no mapping is found {@code IllegalArgumentException} is thrown.
     *
     * @param protocolVersion the version of the protocol; for example,
     * {@link ConnectProtocol#CONNECT_PROTOCOL_V0 CONNECT_PROTOCOL_V0}. May not be null
     * @return the enum that corresponds to the protocol compatibility mode
     */
    public static ConnectProtocolCompatibility fromProtocolVersion(short protocolVersion) {
        switch (protocolVersion) {
            case CONNECT_PROTOCOL_V0:
                return EAGER;
            case CONNECT_PROTOCOL_V1:
                return COMPATIBLE;
            case CONNECT_PROTOCOL_V2:
                return SESSIONED;
            default:
                throw new IllegalArgumentException("Unknown Connect protocol version: " + protocolVersion);
        }
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
        return Arrays.stream(ConnectProtocolCompatibility.values())
                .filter(mode -> mode.protocol().equalsIgnoreCase(protocolName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Not found Connect protocol compatibility mode for protocol: " + protocolName));
    }
}
