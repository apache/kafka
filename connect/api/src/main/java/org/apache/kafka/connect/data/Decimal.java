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

import org.apache.kafka.connect.errors.DataException;

import java.awt.geom.RoundRectangle2D;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * <p>
 *     An arbitrary-precision signed decimal number. The value is unscaled * 10 ^ -scale where:
 *     <ul>
 *         <li>unscaled is an integer </li>
 *         <li>scale is an integer representing how many digits the decimal point should be shifted on the unscaled value</li>
 *     </ul>
 * </p>
 * <p>
 *     Decimal does not provide a fixed schema because it is parameterized by the scale, which is fixed on the schema
 *     rather than being part of the value.
 * </p>
 * <p>
 *     The underlying representation of this type is bytes containing a two's complement integer
 * </p>
 */
public class Decimal {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Decimal";
    public static final String SCALE_FIELD = "scale";
    public static final String ROUNDING_MODE_FIELD = "roundingMode";

    /**
     * Returns a SchemaBuilder for a Decimal with the given scale factor. By returning a SchemaBuilder you can override
     * additional schema settings such as required/optional, default value, and documentation.
     * @param scale the scale factor to apply to unscaled values
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder(int scale) {
        return builder(scale, RoundingMode.UNNECESSARY);
    }

    /**
     * Returns a SchemaBuilder for a Decimal with the given scale factor and rounding mode. By returning a SchemaBuilder
     * you can override additional schema settings such as required/optional, default value, and documentation.
     * For backward compatibility
     * @param scale the scale factor to apply to unscaled values
     * @param roundingMode rounding mode to use when the scale has to be adopted
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder(int scale, RoundingMode roundingMode) {
        Objects.requireNonNull(roundingMode);
        return SchemaBuilder.bytes()
                .name(LOGICAL_NAME)
                .parameter(SCALE_FIELD, Integer.toString(scale))
                .parameter(ROUNDING_MODE_FIELD, roundingMode.name())
                .version(1);
    }

    public static Schema schema(int scale) {
        return builder(scale).build();
    }

    public static Schema schema(int scale, RoundingMode roundingMode) {
        return builder(scale, roundingMode).build();
    }

    /**
     * Convert a value from its logical format (BigDecimal) to it's encoded format.
     * @param value the logical value
     * @return the encoded value
     */
    public static byte[] fromLogical(Schema schema, BigDecimal value) {
        return value.setScale(scale(schema), roundingMode(schema)).unscaledValue().toByteArray();
    }

    public static BigDecimal toLogical(Schema schema, byte[] value) {
        return new BigDecimal(new BigInteger(value), scale(schema));
    }

    private static int scale(Schema schema) {
        String scaleString = schema.parameters().get(SCALE_FIELD);
        if (scaleString == null)
            throw new DataException("Invalid Decimal schema: scale parameter not found.");
        try {
            return Integer.parseInt(scaleString);
        } catch (NumberFormatException e) {
            throw new DataException("Invalid scale parameter found in Decimal schema: ", e);
        }
    }

    private static RoundingMode roundingMode(Schema schema) {
        String roundingModeString = schema.parameters().get(ROUNDING_MODE_FIELD);
        if (roundingModeString==null) {
            return RoundingMode.UNNECESSARY;
        }
        try {
            return RoundingMode.valueOf(roundingModeString);
        } catch (IllegalArgumentException e) {
            throw new DataException("Invalid roundingMode parameter found in Decimal schema: "+roundingModeString, e);
        }
    }
}
