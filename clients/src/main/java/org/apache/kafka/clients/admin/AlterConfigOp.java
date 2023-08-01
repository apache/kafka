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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class representing a alter configuration entry containing name, value and operation type.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AlterConfigOp {

    public enum OpType {
        /**
         * Set the value of the configuration entry.
         */
        SET((byte) 0),
        /**
         * Revert the configuration entry to the default value (possibly null).
         */
        DELETE((byte) 1),
        /**
         * (For list-type configuration entries only.) Add the specified values to the
         * current value of the configuration entry. If the configuration value has not been set,
         * adds to the default value.
         */
        APPEND((byte) 2),
        /**
         * (For list-type configuration entries only.) Removes the specified values from the current
         * value of the configuration entry. It is legal to remove values that are not currently in the
         * configuration entry. Removing all entries from the current configuration value leaves an empty
         * list and does NOT revert to the default value of the entry.
         */
        SUBTRACT((byte) 3);

        private static final Map<Byte, OpType> OP_TYPES = Collections.unmodifiableMap(
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
            return OP_TYPES.get(id);
        }
    }

    private final ConfigEntry configEntry;
    private final OpType opType;

    public AlterConfigOp(ConfigEntry configEntry, OpType operationType) {
        this.configEntry = configEntry;
        this.opType =  operationType;
    }

    public ConfigEntry configEntry() {
        return configEntry;
    }

    public OpType opType() {
        return opType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AlterConfigOp that = (AlterConfigOp) o;
        return opType == that.opType &&
                Objects.equals(configEntry, that.configEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, configEntry);
    }

    @Override
    public String toString() {
        return "AlterConfigOp{" +
                "opType=" + opType +
                ", configEntry=" + configEntry +
                '}';
    }
}
