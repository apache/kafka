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
package org.apache.kafka.common.protocol.types;

public class Field {
    public final String name;
    public final String docString;
    public final Type type;
    public final boolean hasDefaultValue;
    public final Object defaultValue;

    public Field(String name, Type type, String docString, boolean hasDefaultValue, Object defaultValue) {
        this.name = name;
        this.docString = docString;
        this.type = type;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;

        if (hasDefaultValue)
            type.validate(defaultValue);
    }

    public Field(String name, Type type, String docString) {
        this(name, type, docString, false, null);
    }

    public Field(String name, Type type, String docString, Object defaultValue) {
        this(name, type, docString, true, defaultValue);
    }

    public Field(String name, Type type) {
        this(name, type, null, false, null);
    }

    public static class Int8 extends Field {
        public Int8(String name, String docString) {
            super(name, Type.INT8, docString, false, null);
        }
        public Int8(String name, String docString, byte defaultValue) {
            super(name, Type.INT8, docString, true, defaultValue);
        }
    }

    public static class Int32 extends Field {
        public Int32(String name, String docString) {
            super(name, Type.INT32, docString, false, null);
        }

        public Int32(String name, String docString, int defaultValue) {
            super(name, Type.INT32, docString, true, defaultValue);
        }
    }

    public static class Int64 extends Field {
        public Int64(String name, String docString) {
            super(name, Type.INT64, docString, false, null);
        }

        public Int64(String name, String docString, long defaultValue) {
            super(name, Type.INT64, docString, true, defaultValue);
        }
    }

    public static class Int16 extends Field {
        public Int16(String name, String docString) {
            super(name, Type.INT16, docString, false, null);
        }
    }

    public static class Str extends Field {
        public Str(String name, String docString) {
            super(name, Type.STRING, docString, false, null);
        }
    }

    public static class NullableStr extends Field {
        public NullableStr(String name, String docString) {
            super(name, Type.NULLABLE_STRING, docString, false, null);
        }
    }

    public static class Bool extends Field {
        public Bool(String name, String docString) {
            super(name, Type.BOOLEAN, docString, false, null);
        }
    }

    public static class Array extends Field {
        public Array(String name, Type elementType, String docString) {
            super(name, new ArrayOf(elementType), docString, false, null);
        }
    }

    public static class ComplexArray {
        public final String name;
        public final String docString;

        public ComplexArray(String name, String docString) {
            this.name = name;
            this.docString = docString;
        }

        public Field withFields(Field... fields) {
            Schema elementType = new Schema(fields);
            return new Field(name, new ArrayOf(elementType), docString, false, null);
        }

        public Field nullableWithFields(Field... fields) {
            Schema elementType = new Schema(fields);
            return new Field(name, ArrayOf.nullable(elementType), docString, false, null);
        }

        public Field withFields(String docStringOverride, Field... fields) {
            Schema elementType = new Schema(fields);
            return new Field(name, new ArrayOf(elementType), docStringOverride, false, null);
        }
    }
}
