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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompressionTypeTest {

    @Test
    public void testZstdWindowLogDefault() {
        assertEquals(12, CompressionType.ZSTD.windowLog());
    }

    @Test
    public void testZstdLongRangeModeDefault() {
        assertFalse(CompressionType.ZSTD.longRangeMode());
    }

    @Test
    public void testZstdChecksumDefault() {
        assertTrue(CompressionType.ZSTD.checksum());
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"NONE", "GZIP", "SNAPPY", "LZ4"})
    public void testWindowLogNotSupportedForNonZstdTypes(CompressionType type) {
        assertThrows(UnsupportedOperationException.class, type::windowLog);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"NONE", "GZIP", "SNAPPY", "LZ4"})
    public void testLongRangeModeNotSupportedForNonZstdTypes(CompressionType type) {
        assertThrows(UnsupportedOperationException.class, type::longRangeMode);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"NONE", "GZIP", "SNAPPY", "LZ4"})
    public void testChecksumNotSupportedForNonZstdTypes(CompressionType type) {
        assertThrows(UnsupportedOperationException.class, type::checksum);
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class, names = {"NONE", "SNAPPY"})
    public void testLevelsNotSupportedForNoneAndSnappy(CompressionType type) {
        assertThrows(UnsupportedOperationException.class, type::defaultLevel);
        assertThrows(UnsupportedOperationException.class, type::maxLevel);
        assertThrows(UnsupportedOperationException.class, type::minLevel);
        assertThrows(UnsupportedOperationException.class, type::levelValidator);
    }

    @Test
    public void testZstdLevelDefaults() {
        assertEquals(3, CompressionType.ZSTD.defaultLevel());
        assertEquals(-131072, CompressionType.ZSTD.minLevel());
        assertEquals(22, CompressionType.ZSTD.maxLevel());
    }
}
