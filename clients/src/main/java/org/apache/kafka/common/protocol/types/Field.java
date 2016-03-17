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
 */
package org.apache.kafka.common.protocol.types;

/**
 * A field in a schema
 */
public class Field {

    public static final Object NO_DEFAULT = new Object();

    final int index;
    public final String name;
    public final Type type;
    public final Object defaultValue;
    public final String doc;
    final Schema schema;

    /**
     * Create the field.
     *
     * @throws SchemaException If the default value is not primitive and the validation fails
     */
    public Field(int index, String name, Type type, String doc, Object defaultValue, Schema schema) {
        this.index = index;
        this.name = name;
        this.type = type;
        this.doc = doc;
        this.defaultValue = defaultValue;
        this.schema = schema;
        if (defaultValue != NO_DEFAULT)
            type.validate(defaultValue);
    }

    public Field(int index, String name, Type type, String doc, Object defaultValue) {
        this(index, name, type, doc, defaultValue, null);
    }

    public Field(String name, Type type, String doc, Object defaultValue) {
        this(-1, name, type, doc, defaultValue);
    }

    public Field(String name, Type type, String doc) {
        this(name, type, doc, NO_DEFAULT);
    }

    public Field(String name, Type type) {
        this(name, type, "");
    }

    public Type type() {
        return type;
    }

    public Schema schema() {
        return schema;
    }

}
