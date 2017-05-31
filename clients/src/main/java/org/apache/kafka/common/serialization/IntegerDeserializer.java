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

import java.util.Map;

public class IntegerDeserializer implements Deserializer<Integer> {

    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to do
    }

    public Integer deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        if (data.length != 4) {
            throw new SerializationException("Size of data received by IntegerDeserializer is not 4");
        }

        int value = 0;
        for (byte b : data) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    public void close() {
        // nothing to do
    }
}
