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

public class SubscriptionWrapper {
    final private long[] hash;
    final private Instruction instruction;

    public enum Instruction {
        DELETE_KEY_NO_PROPAGATE((byte) 0x00),
        //Send nothing. Do not propagate.
        DELETE_KEY_AND_PROPAGATE((byte) 0x01),
        //Send (k, null)
        PROPAGATE_NULL_IF_NO_FK_VAL_AVAILABLE((byte) 0x02), //(changing foreign key, but FK+Val may not exist)
        //Send (k, fk-val) OR
        //Send (k, null) if fk-val does not exist
        PROPAGATE_ONLY_IF_FK_VAL_AVAILABLE((byte) 0x03); //(first time ever sending key)
        //Send (k, fk-val) only if fk-val exists.

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

    public SubscriptionWrapper(final long[] hash, final Instruction instruction) {
        this.instruction = instruction;
        this.hash = hash;
    }

    public Instruction getInstruction() {
        return instruction;
    }

    public long[] getHash() {
        return hash;
    }
}

