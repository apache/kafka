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

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * <p>
 *     Definition of an abstract data type. Data types can be primitive types (integer types, floating point types,
 *     boolean, strings, and bytes) or complex types (typed arrays, maps with one key schema and value schema,
 *     and structs that have a fixed set of field names each with an associated value schema). Any type can be specified
 *     as optional, allowing it to be omitted (resulting in null values when it is missing) and can specify a default
 *     value.
 * </p>
 * <p>
 *     All schemas may have some associated metadata: a name, version, and documentation. These are all considered part
 *     of the schema itself and included when comparing schemas. Besides adding important metadata, these fields enable
 *     the specification of logical types that specify additional constraints and semantics (e.g. UNIX timestamps are
 *     just an int64, but the user needs the know about the additional semantics to interpret it properly).
 * </p>
 * <p>
 *     Schemas can be created directly, but in most cases using {@link SchemaBuilder} will be simpler.
 * </p>
 */
public interface Schema {
    /**
     * The type of a schema. These only include the core types; logical types must be determined by checking the schema name.
     */
    enum Type {
        /**
         *  8-bit signed integer
         *
         *  Note that if you have an unsigned 8-bit data source, {@link Type#INT16} will be required to safely capture all valid values
         */
        INT8,
        /**
         *  16-bit signed integer
         *
         *  Note that if you have an unsigned 16-bit data source, {@link Type#INT32} will be required to safely capture all valid values
         */
        INT16,
        /**
         *  32-bit signed integer
         *
         *  Note that if you have an unsigned 32-bit data source, {@link Type#INT64} will be required to safely capture all valid values
         */
        INT32,
        /**
         *  64-bit signed integer
         *
         *  Note that if you have an unsigned 64-bit data source, the {@link Decimal} logical type (encoded as {@link Type#BYTES})
         *  will be required to safely capture all valid values
         */
        INT64,
        /**
         *  32-bit IEEE 754 floating point number
         */
        FLOAT32,
        /**
         *  64-bit IEEE 754 floating point number
         */
        FLOAT64,
        /**
         * Boolean value (true or false)
         */
        BOOLEAN,
        /**
         * Character string that supports all Unicode characters.
         *
         * Note that this does not imply any specific encoding (e.g. UTF-8) as this is an in-memory representation.
         */
        STRING,
        /**
         * Sequence of unsigned 8-bit bytes
         */
        BYTES,
        /**
         * An ordered sequence of elements, each of which shares the same type.
         */
        ARRAY,
        /**
         * A mapping from keys to values. Both keys and values can be arbitrarily complex types, including complex types
         * such as {@link Struct}.
         */
        MAP,
        /**
         * A structured record containing a set of named fields, each field using a fixed, independent {@link Schema}.
         */
        STRUCT;

        private String name;

        Type() {
            this.name = this.name().toLowerCase(Locale.ROOT);
        }

        public String getName() {
            return name;
        }

        public boolean isPrimitive() {
            switch (this) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                case BYTES:
                    return true;
            }
            return false;
        }
    }


    Schema INT8_SCHEMA = SchemaBuilder.int8().build();
    Schema INT16_SCHEMA = SchemaBuilder.int16().build();
    Schema INT32_SCHEMA = SchemaBuilder.int32().build();
    Schema INT64_SCHEMA = SchemaBuilder.int64().build();
    Schema FLOAT32_SCHEMA = SchemaBuilder.float32().build();
    Schema FLOAT64_SCHEMA = SchemaBuilder.float64().build();
    Schema BOOLEAN_SCHEMA = SchemaBuilder.bool().build();
    Schema STRING_SCHEMA = SchemaBuilder.string().build();
    Schema BYTES_SCHEMA = SchemaBuilder.bytes().build();

    Schema OPTIONAL_INT8_SCHEMA = SchemaBuilder.int8().optional().build();
    Schema OPTIONAL_INT16_SCHEMA = SchemaBuilder.int16().optional().build();
    Schema OPTIONAL_INT32_SCHEMA = SchemaBuilder.int32().optional().build();
    Schema OPTIONAL_INT64_SCHEMA = SchemaBuilder.int64().optional().build();
    Schema OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.float32().optional().build();
    Schema OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.float64().optional().build();
    Schema OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
    Schema OPTIONAL_STRING_SCHEMA = SchemaBuilder.string().optional().build();
    Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.bytes().optional().build();

    /**
     * @return the type of this schema
     */
    Type type();

    /**
     * @return true if this field is optional, false otherwise
     */
    boolean isOptional();

    /**
     * @return the default value for this schema
     */
    Object defaultValue();

    /**
     * @return the name of this schema
     */
    String name();

    /**
     * Get the optional version of the schema. If a version is included, newer versions *must* be larger than older ones.
     * @return the version of this schema
     */
    Integer version();

    /**
     * @return the documentation for this schema
     */
    String doc();

    /**
     * Get a map of schema parameters.
     * @return Map containing parameters for this schema, or null if there are no parameters
     */
    Map<String, String> parameters();

    /**
     * Get the key schema for this map schema. Throws a DataException if this schema is not a map.
     * @return the key schema
     */
    Schema keySchema();

    /**
     * Get the value schema for this map or array schema. Throws a DataException if this schema is not a map or array.
     * @return the value schema
     */
    Schema valueSchema();

    /**
     * Get the list of fields for this Schema. Throws a DataException if this schema is not a struct.
     * @return the list of fields for this Schema
     */
    List<Field> fields();

    /**
     * Get a field for this Schema by name. Throws a DataException if this schema is not a struct.
     * @param fieldName the name of the field to look up
     * @return the Field object for the specified field, or null if there is no field with the given name
     */
    Field field(String fieldName);

    /**
     * Return a concrete instance of the {@link Schema}
     * @return the {@link Schema}
     */
    Schema schema();
}
