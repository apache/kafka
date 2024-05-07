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
 */
public enum FieldSyntaxVersion {
    /**
     * No support to access nested fields, only attributes at the root of data structure.
     * Backward compatible (i.e. before KIP-821).
     */
    V1,
    /**
     * Support to access nested fields using dotted notation
     * (with backtick pairs to wrap field names that include dots).
     */
    V2;

    public static final String FIELD_SYNTAX_VERSION_CONFIG = "field.syntax.version";
    public static final String FIELD_SYNTAX_VERSION_DOC =
            "Defines the version of the syntax to access fields. "
                    + "If set to `V1`, then the field paths are limited to access the elements at the root level of the struct or map. "
                    + "If set to `V2`, the syntax will support accessing nested elements. "
                    + "To access nested elements, dotted notation is used. "
                    + "If dots are already included in the field name, "
                    + "then backtick pairs can be used to wrap field names containing dots. "
                    + "E.g. to access the subfield `baz` from a field named \"foo.bar\" in a struct/map "
                    + "the following format can be used to access its elements: \"`foo.bar`.baz\".";

    public static final String FIELD_SYNTAX_VERSION_DEFAULT_VALUE = V1.name();

    /**
     * Extend existing config definition by adding field syntax version.
     * To be used by transforms supporting nested fields.
     *
     * @param configDef exiting config definition
     * @return config definition including field syntax version definition
     */
    public static ConfigDef appendConfigTo(ConfigDef configDef) {
        return configDef
                .define(
                        FieldSyntaxVersion.FIELD_SYNTAX_VERSION_CONFIG,
                        ConfigDef.Type.STRING,
                        FieldSyntaxVersion.FIELD_SYNTAX_VERSION_DEFAULT_VALUE,
                        ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(FieldSyntaxVersion.class)),
                        ConfigDef.Importance.HIGH,
                        FieldSyntaxVersion.FIELD_SYNTAX_VERSION_DOC);
    }

    /**
     * Gather version from config values.
     *
     * @param config including value for field syntax version configuration
     * @return field syntax version
     * @throws ConfigException if fails to collect version, e.g. wrong value
     */
    public static FieldSyntaxVersion fromConfig(AbstractConfig config) {
        final String fieldSyntaxVersion = config.getString(FIELD_SYNTAX_VERSION_CONFIG);
        try {
            return FieldSyntaxVersion.valueOf(fieldSyntaxVersion.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ConfigException(FIELD_SYNTAX_VERSION_CONFIG, fieldSyntaxVersion, "Unrecognized field syntax version");
        }
    }
}
