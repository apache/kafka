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

package org.apache.kafka.clients.admin;

public class ConfigEntry {

    private final String name;
    private final String value;
    private final boolean isDefault;
    private final boolean isSensitive;
    private final boolean isReadOnly;

    public ConfigEntry(String name, String value) {
        this(name, value, false, false, false);
    }

    public ConfigEntry(String name, String value, boolean isDefault, boolean isSensitive, boolean isReadOnly) {
        this.name = name;
        this.value = value;
        this.isDefault = isDefault;
        this.isSensitive = isSensitive;
        this.isReadOnly = isReadOnly;
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public boolean isSensitive() {
        return isSensitive;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }
}
