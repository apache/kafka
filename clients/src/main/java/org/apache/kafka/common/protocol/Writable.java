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

package org.apache.kafka.common.protocol;

import java.nio.charset.StandardCharsets;

public interface Writable {
    void writeByte(byte val);
    void writeShort(short val);
    void writeInt(int val);
    void writeLong(long val);
    void writeArray(byte[] arr);

    /**
     * Write a nullable byte array delimited by a four-byte length prefix.
     */
    default void writeNullableBytes(byte[] arr) {
        if (arr == null) {
            writeInt(-1);
        } else {
            writeBytes(arr);
        }
    }

    /**
     * Write a byte array delimited by a four-byte length prefix.
     */
    default void writeBytes(byte[] arr) {
        writeInt(arr.length);
        writeArray(arr);
    }

    /**
     * Write a nullable string delimited by a two-byte length prefix.
     */
    default void writeNullableString(String string) {
        if (string == null) {
            writeShort((short) -1);
        } else {
            writeString(string);
        }
    }

    /**
     * Write a string delimited by a two-byte length prefix.
     */
    default void writeString(String string) {
        byte[] arr = string.getBytes(StandardCharsets.UTF_8);
        if (arr.length > Short.MAX_VALUE) {
            throw new RuntimeException("Can't store string longer than " +
                Short.MAX_VALUE);
        }
        writeShort((short) arr.length);
        writeArray(arr);
    }
}
