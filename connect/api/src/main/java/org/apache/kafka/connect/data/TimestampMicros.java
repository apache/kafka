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

import java.time.Instant;

/**
 * <p>
 *     A timestamp representing an absolute time with microsecond precision, without timezone information. This logical
 *     type uses {@link java.time.Instant} for its logical representation to preserve microsecond precision that would
 *     be lost with {@link java.util.Date}. The underlying representation is a long representing the number of 
 *     microseconds since Unix epoch (January 1, 1970, 00:00:00 UTC).
 * </p>
 */
public class TimestampMicros {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.TimestampMicros";
    private static final long MICROS_PER_MILLI = 1000L;

    /**
     * Returns a SchemaBuilder for a TimestampMicros. By returning a SchemaBuilder you can override additional schema settings such
     * as required/optional, default value, and documentation.
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final Schema SCHEMA = builder().schema();

    /**
     * Convert a value from its logical format ({@link java.time.Instant}) to its encoded format (long microseconds since epoch).
     * @param schema the schema
     * @param value the logical value
     * @return the encoded value
     */
    public static long fromLogical(Schema schema, Instant value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of TimestampMicros object but the schema does not match.");

        return value.getEpochSecond() * 1_000_000L + value.getNano() / 1_000L;
    }

    /**
     * Convert a value from its encoded format (long microseconds since epoch) to its logical format ({@link java.time.Instant}).
     * @param schema the schema
     * @param value the encoded value
     * @return the logical value
     */
    public static Instant toLogical(Schema schema, long value) {
        if (!(LOGICAL_NAME.equals(schema.name())))
            throw new DataException("Requested conversion of TimestampMicros object but the schema does not match.");

        long seconds = value / 1_000_000L;
        int nanos = (int) ((value % 1_000_000L) * 1_000L);
        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Convert a standard millisecond timestamp to microsecond precision.
     * This is a utility method to help convert from the traditional millisecond-based timestamps.
     * 
     * @param millis timestamp in milliseconds
     * @return timestamp in microseconds
     */
    public static long fromMillis(long millis) {
        return millis * MICROS_PER_MILLI;
    }

    /**
     * Convert a microsecond timestamp to standard millisecond precision.
     * This is a utility method to help convert to the traditional millisecond-based timestamps.
     * Note that this conversion may lose precision.
     * 
     * @param micros timestamp in microseconds
     * @return timestamp in milliseconds
     */
    public static long toMillis(long micros) {
        return micros / MICROS_PER_MILLI;
    }
}
