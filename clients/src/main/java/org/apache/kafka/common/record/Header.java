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
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Header {
    private final String key;
    private final ByteBuffer value;

    public Header(String key, ByteBuffer value) {
        Objects.requireNonNull(key, "Null header keys are not permitted");
        this.key = key;
        this.value = value;
    }

    public Header(String key, byte[] value) {
        this(key, Utils.wrapNullable(value));
    }

    public String key() {
        return key;
    }

    public ByteBuffer value() {
        return value == null ? null : value.duplicate();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Header header = (Header) o;
        return (key == null ? header.key == null : key.equals(header.key)) &&
                (value == null ? header.value == null : value.equals(header.value));
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
