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
package org.apache.kafka.connect.data;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DecimalTest {
    private static final int TEST_SCALE = 2;
    private static final BigDecimal TEST_DECIMAL = new BigDecimal(new BigInteger("156"), TEST_SCALE);
    private static final BigDecimal TEST_DECIMAL_NEGATIVE = new BigDecimal(new BigInteger("-156"), TEST_SCALE);
    private static final byte[] TEST_BYTES = new byte[]{0, -100};
    private static final byte[] TEST_BYTES_NEGATIVE = new byte[]{-1, 100};

    @Test
    public void testBuilder() {
        Schema plain = Decimal.builder(2).build();
        assertEquals(Decimal.LOGICAL_NAME, plain.name());
        assertEquals(2, plain.parameters().size());
        assertEquals("2", plain.parameters().get(Decimal.SCALE_FIELD));
        assertEquals("UNNECESSARY", plain.parameters().get(Decimal.ROUNDING_MODE_FIELD));
        assertEquals(1, (Object) plain.version());
    }

    @Test
    public void testFromLogical() {
        Schema schema = Decimal.schema(TEST_SCALE);
        byte[] encoded = Decimal.fromLogical(schema, TEST_DECIMAL);
        assertArrayEquals(TEST_BYTES, encoded);

        encoded = Decimal.fromLogical(schema, TEST_DECIMAL_NEGATIVE);
        assertArrayEquals(TEST_BYTES_NEGATIVE, encoded);
    }

    @Test
    public void testToLogical() {
        Schema schema = Decimal.schema(2);
        BigDecimal converted = Decimal.toLogical(schema, TEST_BYTES);
        assertEquals(TEST_DECIMAL, converted);

        converted = Decimal.toLogical(schema, TEST_BYTES_NEGATIVE);
        assertEquals(TEST_DECIMAL_NEGATIVE, converted);
    }

    @Test
    public void testExpandScale() {
        Schema schema = Decimal.schema(2);
        BigDecimal input = BigDecimal.valueOf (5, 0);
        byte[] encoded = Decimal.fromLogical(schema, input);
        BigDecimal output = Decimal.toLogical(schema, encoded);
        assertEquals(input.setScale(2), output);
    }

    @Test
    public void testReduceScale() {
        Schema schema = Decimal.builder(2).parameter(Decimal.ROUNDING_MODE_FIELD, RoundingMode.HALF_UP.name());
        BigDecimal input = BigDecimal.valueOf (5.123);
        byte[] encoded = Decimal.fromLogical(schema, input);
        BigDecimal output = Decimal.toLogical(schema, encoded);
        assertEquals(BigDecimal.valueOf(5.12), output);
    }

}
