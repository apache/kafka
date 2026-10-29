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

    public static class TaggedFieldsSection extends Field {
        private static final String NAME = "_tagged_fields";
        private static final String DOC_STRING = "The tagged fields";

        /**
         * Create a new TaggedFieldsSection with the given tags and fields.
         *
         * @param fields    This is an array containing Integer tags followed
         *                  by associated Field objects.
         * @return          The new {@link TaggedFieldsSection}
         */
        public static TaggedFieldsSection of(Object... fields) {
            return new TaggedFieldsSection(TaggedFields.of(fields));
        }

        public TaggedFieldsSection(Type type) {
            super(NAME, type, DOC_STRING, false, null);
        }
    }
}
