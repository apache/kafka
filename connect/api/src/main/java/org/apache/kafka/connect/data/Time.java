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

import java.util.Calendar;
import java.util.TimeZone;

/**
 * <p>
 *     A time representing a specific point in a day, not tied to any specific date. The corresponding Java type is a
 *     java.util.Date where only hours, minutes, seconds, and milliseconds can be non-zero. This effectively makes it a
 *     point in time during the first day after the Unix epoch. The underlying representation is an integer
 *     representing the number of milliseconds after midnight.
 * </p>
 */
public class Time {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.Time";

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Returns a SchemaBuilder for a Time. By returning a SchemaBuilder you can override additional schema settings such
     * as required/optional, default value, and documentation.
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int32()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final Schema SCHEMA = builder().schema();

    /**
     * Convert a value from its logical format (Time) to it's encoded format.
     * @param value the logical value
     * @return the encoded value
     */
    public static int fromLogical(Schema schema, java.util.Date value) {
        if (schema.name() == null || !(schema.name().equals(LOGICAL_NAME)))
            throw new DataException("Requested conversion of Time object but the schema does not match.");
        Calendar calendar = Calendar.getInstance(UTC);
        calendar.setTime(value);
        long unixMillis = calendar.getTimeInMillis();
        if (unixMillis < 0 || unixMillis > MILLIS_PER_DAY) {
            throw new DataException("Kafka Connect Time type should not have any date fields set to non-zero values.");
        }
        return (int) unixMillis;
    }

    public static java.util.Date toLogical(Schema schema, int value) {
        if (schema.name() == null || !(schema.name().equals(LOGICAL_NAME)))
            throw new DataException("Requested conversion of Date object but the schema does not match.");
        if (value  < 0 || value > MILLIS_PER_DAY)
            throw new DataException("Time values must use number of milliseconds greater than 0 and less than 86400000");
        return new java.util.Date(value);
    }
}
