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
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.Objects;


public class SubscriptionWrapper<K> {
    final static byte CURRENT_VERSION = 0;
    //This is the maximum version because SubscriptionWrapperSerde uses
    final private static byte MAXIMUM_VERSION_INCLUSIVE = (byte) Math.pow(2, SubscriptionWrapperSerde.VERSION_BITS);

    final private long[] hash;
    final private Instruction instruction;
    final private byte version;
    final private K primaryKey;

    public enum Instruction {
        //Send nothing. Do not propagate.
        DELETE_KEY_NO_PROPAGATE((byte) 0x00),

        //Send (k, null)
        DELETE_KEY_AND_PROPAGATE((byte) 0x01),

        //(changing foreign key, but FK+Val may not exist)
        //Send (k, fk-val) OR
        //Send (k, null) if fk-val does not exist
        PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE((byte) 0x02),

        //(first time ever sending key)
        //Send (k, fk-val) only if fk-val exists.
        PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE((byte) 0x03);

        private byte value;
        Instruction(final byte value) {
            this.value = value;
        }

        public byte getByte() {
            return value;
        }

        public static Instruction fromValue(final byte value) {
            for (final Instruction i: values()) {
                if (i.value == value) {
                    return i;
                }
            }
            throw new IllegalArgumentException("Unknown instruction byte value = " + value);
        }
    }

    public SubscriptionWrapper(final long[] hash, final Instruction instruction, final K primaryKey) {
        this(hash, instruction, primaryKey, CURRENT_VERSION);
    }

    public SubscriptionWrapper(final long[] hash, final Instruction instruction, final K primaryKey, final byte version) {
        Objects.requireNonNull(instruction, "instruction cannot be null. Required by downstream processor.");
        Objects.requireNonNull(primaryKey, "primaryKey cannot be null. Required by downstream processor.");
        if (version != CURRENT_VERSION) {
            throw new UnsupportedVersionException("SubscriptionWrapper does not support version " + version);
        }

        this.instruction = instruction;
        this.hash = hash;
        this.primaryKey = primaryKey;
        this.version = version;
    }

    public Instruction getInstruction() {
        return instruction;
    }

    public long[] getHash() {
        return hash;
    }

    public K getPrimaryKey() {
        return primaryKey;
    }

    public byte getVersion() {
        return version;
    }
}

