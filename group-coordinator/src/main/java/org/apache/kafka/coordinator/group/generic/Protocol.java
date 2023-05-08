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

package org.apache.kafka.coordinator.group.generic;

import java.util.Arrays;
import java.util.Objects;

/**
 * This class represents a protocol that is supported by a
 * {@link GenericGroupMember}.
 */
public class Protocol {

    /**
     * Represents an empty metadata.
     */
    public static final byte[] EMPTY_METADATA = new byte[0];

    /**
     * The name of the protocol.
     */
    private final String name;

    /**
     * The protocol's metadata.
     */
    private final byte[] metadata;

    public Protocol(String name, byte[] metadata) {
        this.name = Objects.requireNonNull(name);
        this.metadata = metadata;
    }

    /**
     * @return the name of the protocol.
     */
    public String name() {
        return this.name;
    }

    /**
     * @return the metadata stored inside the protocol.
     */
    public byte[] metadata() {
        return this.metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Protocol protocol = (Protocol) o;

        if (!name.equals(protocol.name)) return false;
        return Arrays.equals(metadata, protocol.metadata);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + Arrays.hashCode(metadata);
        return result;
    }

    @Override
    public String toString() {
        return "Protocol(name=" + name +
            ", metadata=" + Arrays.toString(metadata) +
            ")";
    }
}
