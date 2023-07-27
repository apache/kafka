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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

/**
 * Do not need to flip before call <i>serialize(String, ByteBuffer)</i>. For example:
 *
 * <blockquote>
 * <pre>
 * ByteBufferSerializer serializer = ...; // Create Serializer
 * ByteBuffer buffer = ...;               // Allocate ByteBuffer
 * buffer.put(data);                      // Put data into buffer, do not need to flip
 * serializer.serialize(topic, buffer);   // Serialize buffer
 * </pre>
 * </blockquote>
 */
public class ByteBufferSerializer implements Serializer<ByteBuffer> {

    @Override
    public byte[] serialize(String topic, ByteBuffer data) {
        if (data == null) {
            return null;
        }

        if (data.hasArray()) {
            final byte[] arr = data.array();
            if (data.arrayOffset() == 0 && arr.length == data.remaining()) {
                return arr;
            }
        }

        data.flip();
        return Utils.toArray(data);
    }
}
