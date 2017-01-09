/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

public class UUID {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.UUID";

    /**
     * Returns a SchemaBuilder for a UUID. By returning a SchemaBuilder you can override additional schema settings such
     * as required/optional, default value, and documentation.
     * @return a SchemaBuilder
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.string()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final Schema SCHEMA = builder().schema();

    /**
     * Convert a value from its logical format (UUID) to it's encoded format.
     * @param value the logical value
     * @return the encoded value
     */
    public static String fromLogical(Schema schema, java.util.UUID value) {
        if (schema.name() == null || !(schema.name().equals(LOGICAL_NAME)))
            throw new DataException("Requested conversion of UUID object but the schema does not match.");
        return value.toString();
    }

    public static java.util.UUID toLogical(Schema schema, String value) {
        if (schema.name() == null || !(schema.name().equals(LOGICAL_NAME)))
            throw new DataException("Requested conversion of UUID object but the schema does not match.");
        // Strings must conform to UUID format, https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html#toString()
        try {
            return java.util.UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new DataException(String.format("Requested conversion to UUID object but %s does not conform with UUID string representation", value));
        }
    }
}
