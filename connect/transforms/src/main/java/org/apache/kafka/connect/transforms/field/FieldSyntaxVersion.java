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
package org.apache.kafka.connect.transforms.field;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.Locale;

/**
 * Defines semantics of field paths by versioning.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-821%3A+Connect+Transforms+support+for+nested+structures">KIP-821</a>
 * @see SingleFieldPath
 * @see MultiFieldPaths
 */
public enum FieldSyntaxVersion {
    /**
     * No support for nested fields. Only access attributes on the root data value.
     * Backward compatibility before KIP-821.
     */
    V1,
    /**
     * Support for nested fields using dotted notation with backtick pairs to wrap field names that
     * include dots.
     * @since 3.x
     */
    V2;

    public static final String FIELD_SYNTAX_VERSION_CONFIG = "field.syntax.version";
    public static final String FIELD_SYNTAX_VERSION_DOC =
            "Defines the version of the syntax to access fields. "
                    + "If set to `V1`, then the field paths are limited to access the elements at the root level of the struct or map."
                    + "If set to `V2`, the syntax will support accessing nested elements. To access nested elements, "
                    + "dotted notation is used. If dots are already included in the field name, then backtick pairs "
                    + "can be used to wrap field names containing dots. "
                    + "E.g. to access the subfield `baz` from a field named \"foo.bar\" in a struct/map  "
                    + "the following format can be used to access its elements: \"`foo.bar`.baz\".";

    public static final String FIELD_SYNTAX_VERSION_DEFAULT_VALUE = V1.name();

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(
                        FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG,
                        ConfigDef.Type.STRING,
                        FieldSyntaxVersion.FIELD_SYNTAX_VERSION_DEFAULT_VALUE,
                        ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(FieldSyntaxVersion.class)),
                        ConfigDef.Importance.HIGH,
                        FieldSyntaxVersion.FIELD_SYNTAX_VERSION_DOC);
    }

    public static FieldSyntaxVersion fromConfig(AbstractConfig config) {
        final String name = config.getString(FIELD_SYNTAX_VERSION_CONFIG);
        try {
            return FieldSyntaxVersion.valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConfigException(FIELD_SYNTAX_VERSION_CONFIG, name, "Unrecognized field syntax version");
        }
    }
}
