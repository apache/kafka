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
package org.apache.kafka.streams.header;

import java.util.Arrays;
import org.apache.kafka.common.utils.Utils;

public class StreamHeader implements Header {

    final String key;
    final byte[] value;

    public StreamHeader(final String key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    public static StreamHeader wrap(final org.apache.kafka.common.header.Header header) {
        return new StreamHeader(header.key(), header.value());
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public String valueAsUtf8() {
        return Utils.utf8(value);
    }

    @Override
    public String toString() {
        return "StreamHeader(" +
            "key='" + key + '\'' +
            ",value=" + Arrays.toString(value) +
            ')';
    }
}
