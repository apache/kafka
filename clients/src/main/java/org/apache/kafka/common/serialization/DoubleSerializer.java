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

public class DoubleSerializer implements Serializer<Double> {
    @Override
    public byte[] serialize(String topic, Double data) {
        if (data == null)
            return null;

        long bits = Double.doubleToLongBits(data);
        return new byte[] {
            (byte) (bits >>> 56),
            (byte) (bits >>> 48),
            (byte) (bits >>> 40),
            (byte) (bits >>> 32),
            (byte) (bits >>> 24),
            (byte) (bits >>> 16),
            (byte) (bits >>> 8),
            (byte) bits
        };
    }
}