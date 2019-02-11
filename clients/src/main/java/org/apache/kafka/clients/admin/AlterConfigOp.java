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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class representing a alter configuration entry containing name, value and operation type.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class AlterConfigOp {

    public enum OpType {
        SET((byte) 0), DELETE((byte) 1), APPEND((byte) 2), SUBSTRACT((byte) 3);

        private static final Map<Byte, OpType> OPTYPES = Collections.unmodifiableMap(
                Arrays.stream(values()).collect(Collectors.toMap(OpType::id, Function.identity()))
        );

        private final byte id;

        OpType(final byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static OpType forId(final byte id) {
            return OPTYPES.get(id);
        }
    }

    private final ConfigResource configResource;
    private final ConfigEntry configEntry;
    private final OpType opType;

    public AlterConfigOp(ConfigResource configResource, ConfigEntry configEntry, OpType operationType) {
        this.configResource = configResource;
        this.configEntry = configEntry;
        this.opType =  operationType;
    }

    public ConfigResource resource() {
        return configResource;
    }

    public ConfigEntry configEntry() {
        return configEntry;
    };

    public OpType opType() {
        return opType;
    };

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AlterConfigOp that = (AlterConfigOp) o;
        return opType == that.opType &&
                Objects.equals(configResource, that.configResource) &&
                Objects.equals(configEntry, that.configEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, configResource, configEntry);
    }

    @Override
    public String toString() {
        return "AlterConfigOp{" +
                "opType=" + opType +
                ", configResource=" + configResource +
                ", configEntry=" + configEntry +
                '}';
    }
}
