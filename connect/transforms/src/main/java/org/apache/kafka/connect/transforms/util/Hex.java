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

package org.apache.kafka.connect.transforms.util;

public class Hex {
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    /**
     * Encodes a byte array as a hexadecimal string.
     *
     * @implNote https://stackoverflow.com/a/9855338/1781549
     */
    public static String encode(final byte[] bytes) {
        final char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            final int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4]; // hi nibble
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F]; // lo nibble
        }
        return new String(hexChars);
    }
}
