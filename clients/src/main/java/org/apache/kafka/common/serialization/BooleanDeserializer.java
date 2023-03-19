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

public class BooleanDeserializer implements Deserializer<Boolean> {
    private static final byte TRUE = 0x01;
    private static final byte FALSE = 0x00;

    @Override
    public Boolean deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        if (data.length != 1) {
            throw new SerializationException("Size of data received by BooleanDeserializer is not 1");
        }

        if (data[0] == TRUE) {
            return true;
        } else if (data[0] == FALSE) {
            return false;
        } else {
            throw new SerializationException("Unexpected byte received by BooleanDeserializer: " + data[0]);
        }
    }
}
