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

import java.nio.ByteBuffer;

public final class MessageTestUtil {
    public static int messageSize(Message message, short version) {
        return message.size(new ObjectSerializationCache(), version);
    }

    public static ByteBuffer messageToByteBuffer(Message message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = message.size(cache, version);
        ByteBuffer bytes = ByteBuffer.allocate(size);
        message.write(new ByteBufferAccessor(bytes), cache, version);
        bytes.flip();
        return bytes;
    }

    public static void messageFromByteBuffer(ByteBuffer bytes, Message message, short version) {
        message.read(new ByteBufferAccessor(bytes.duplicate()), version);
    }

    public static String byteBufferToString(ByteBuffer buf) {
        ByteBuffer buf2 = buf.duplicate();
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        while (buf2.hasRemaining()) {
            bld.append(String.format("%s%02x", prefix, (int) buf2.get()));
            prefix = " ";
        }
        return bld.toString();
    }
}
