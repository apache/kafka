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

package org.apache.kafka.message;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * TranslationStyle contains hints about how to translate various fields in
 * an efficient way.  This does not affect what is sent on the wire.  It is
 * just a hint for generating efficient Java code.
 */
public enum TranslationStyle {
    /**
     * Use the default translation style for this type.
     */
    @JsonProperty("default")
    DEFAULT,

    /**
     * Translate this type as a byte array.
     */
    @JsonProperty("byteArray")
    BYTE_ARRAY,

    /**
     * Translate this type as a zero-copy ByteBuffer.
     *
     * Zero-copy fields are more efficient for receiving or sending
     * a lot of data.  However, they make memory management more complex
     * because they hold a reference to the containing buffer, which
     * cannot then be safely freed while they are in use.
     */
    @JsonProperty("zeroCopyByteBuffer")
    ZERO_COPY_BYTE_BUFFER;
}
