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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import java.util.zip.Checksum;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Crc32Test {

    @Test
    public void testUpdate() {
        final byte[] bytes = "Any String you want".getBytes();
        final int len = bytes.length;

        Checksum crc1 = Crc32C.create();
        Checksum crc2 = Crc32C.create();
        Checksum crc3 = Crc32C.create();

        crc1.update(bytes, 0, len);
        for (int i = 0; i < len; i++)
            crc2.update(bytes[i]);
        crc3.update(bytes, 0, len / 2);
        crc3.update(bytes, len / 2, len - len / 2);

        assertEquals(crc1.getValue(), crc2.getValue(), "Crc values should be the same");
        assertEquals(crc1.getValue(), crc3.getValue(), "Crc values should be the same");
    }

    @Test
    public void testValue() {
        final byte[] bytes = "Some String".getBytes();
        assertEquals(2021503672, Crc32.crc32(bytes));
    }

}
