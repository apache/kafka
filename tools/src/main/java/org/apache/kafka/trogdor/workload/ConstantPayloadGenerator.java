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
package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A PayloadGenerator which always generates a constant payload.
 */
public class ConstantPayloadGenerator implements PayloadGenerator {
    private final int size;
    private final byte[] value;

    @JsonCreator
    public ConstantPayloadGenerator(@JsonProperty("size") int size,
                                    @JsonProperty("value") byte[] value) {
        this.size = size;
        this.value = (value == null || value.length == 0) ? new byte[size] : value;
    }

    @JsonProperty
    public int size() {
        return size;
    }

    @JsonProperty
    public byte[] value() {
        return value;
    }

    @Override
    public byte[] generate(long position) {
        byte[] next = new byte[size];
        for (int i = 0; i < next.length; i += value.length) {
            System.arraycopy(value, 0, next, i, Math.min(next.length - i, value.length));
        }
        return next;
    }
}
