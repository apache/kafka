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

package org.apache.kafka.common.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class representing resources that have configs.
 */
public final class ConfigResource {

    /**
     * Type of resource.
     */
    public enum Type {
        BROKER_LOGGER((byte) 8), BROKER((byte) 4), TOPIC((byte) 2), UNKNOWN((byte) 0);

        private static final Map<Byte, Type> TYPES = Collections.unmodifiableMap(
            Arrays.stream(values()).collect(Collectors.toMap(Type::id, Function.identity()))
        );

        private final byte id;

        Type(final byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Type forId(final byte id) {
            return TYPES.getOrDefault(id, UNKNOWN);
        }
    }

    private final Type type;
    private final String name;

    /**
     * Create an instance of this class with the provided parameters.
     *
     * @param type a non-null resource type
     * @param name a non-null resource name
     */
    public ConfigResource(Type type, String name) {
        Objects.requireNonNull(type, "type should not be null");
        Objects.requireNonNull(name, "name should not be null");
        this.type = type;
        this.name = name;
    }

    /**
     * Return the resource type.
     */
    public Type type() {
        return type;
    }

    /**
     * Return the resource name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns true if this is the default resource of a resource type.
     * Resource name is empty for the default resource.
     */
    public boolean isDefault() {
        return name.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConfigResource that = (ConfigResource) o;

        return type == that.type && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ConfigResource(type=" + type + ", name='" + name + "')";
    }
}
