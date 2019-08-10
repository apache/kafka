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
    void writeUnsignedVarint(int i);
    void writeUnsignedVarlong(long i);

    /**
     * Write a string delimited by a two-byte length prefix.
     */
    default void writeString(String string) {
        byte[] arr = string.getBytes(StandardCharsets.UTF_8);
        writeArray(arr);
    }
}
