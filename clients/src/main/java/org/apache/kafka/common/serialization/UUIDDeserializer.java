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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 *  We are converting the byte array to String before deserializing to UUID. String encoding defaults to UTF8 and can be customized by setting
 *  the property key.deserializer.encoding, value.deserializer.encoding or deserializer.encoding. The first two take precedence over the last.
 */
public class UUIDDeserializer implements Deserializer<UUID> {
    private String encoding = StandardCharsets.UTF_8.name();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public UUID deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;
            else
                return UUID.fromString(new String(data, encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to UUID due to unsupported encoding " + encoding, e);
        } catch (IllegalArgumentException e) {
            throw new SerializationException("Error parsing data into UUID", e);
        }
    }

    @Override
    public UUID deserialize(String topic, Headers headers, ByteBuffer data) {
        try {
            if (data == null) {
                return null;
            }

            if (data.hasArray()) {
                return UUID.fromString(new String(data.array(), data.arrayOffset() + data.position(), data.remaining(), encoding));
            }
            return UUID.fromString(new String(Utils.toArray(data), encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing ByteBuffer to UUID due to unsupported encoding " + encoding, e);
        } catch (IllegalArgumentException e) {
            throw new SerializationException("Error parsing data into UUID", e);
        }
    }
}
