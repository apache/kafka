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

public class FieldDef {
    public final String name;
    public final String docString;
    public final Type type;
    public final boolean hasDefaultValue;
    public final Object defaultValue;

    private FieldDef(String name, String docString, Type type, boolean hasDefaultValue, Object defaultValue) {
        this.name = name;
        this.docString = docString;
        this.type = type;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
    }

    public static class Int8 extends FieldDef {
        public Int8(String name, String docString) {
            super(name, docString, Type.INT8, false, null);
        }
    }

    public static class Int32 extends FieldDef {
        public Int32(String name, String docString) {
            super(name, docString, Type.INT32, false, null);
        }

        public Int32(String name, String docString, int defaultValue) {
            super(name, docString, Type.INT32, true, defaultValue);
        }
    }

    public static class Int16 extends FieldDef {
        public Int16(String name, String docString) {
            super(name, docString, Type.INT16, false, null);
        }
    }

    public static class Str extends FieldDef {
        public Str(String name, String docString) {
            super(name, docString, Type.STRING, false, null);
        }
    }

    public static class NullableStr extends FieldDef {
        public NullableStr(String name, String docString) {
            super(name, docString, Type.NULLABLE_STRING, false, null);
        }
    }
}
