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



package org.apache.kafka.copycat.data;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Base class for objects that have Object-valued properties.
 */
public abstract class ObjectProperties {
    public static class Null {
        private Null() {
        }
    }

    /** A value representing a JSON <code>null</code>. */
    public static final Null NULL_VALUE = new Null();

    Map<String, Object> props = new LinkedHashMap<String, Object>(1);

    private Set<String> reserved;

    ObjectProperties(Set<String> reserved) {
        this.reserved = reserved;
    }

    /**
     * Returns the value of the named, string-valued property in this schema.
     * Returns <tt>null</tt> if there is no string-valued property with that name.
     */
    public String getProp(String name) {
        Object value = getObjectProp(name);
        return (value instanceof String) ? (String) value : null;
    }

    /**
     * Returns the value of the named property in this schema.
     * Returns <tt>null</tt> if there is no property with that name.
     */
    public synchronized Object getObjectProp(String name) {
        return props.get(name);
    }

    /**
     * Adds a property with the given name <tt>name</tt> and
     * value <tt>value</tt>. Neither <tt>name</tt> nor <tt>value</tt> can be
     * <tt>null</tt>. It is illegal to add a property if another with
     * the same name but different value already exists in this schema.
     *
     * @param name The name of the property to add
     * @param value The value for the property to add
     */
    public synchronized void addProp(String name, Object value) {
        if (reserved.contains(name))
            throw new DataRuntimeException("Can't set reserved property: " + name);

        if (value == null)
            throw new DataRuntimeException("Can't set a property to null: " + name);

        Object old = props.get(name);
        if (old == null)
            props.put(name, value);
        else if (!old.equals(value))
            throw new DataRuntimeException("Can't overwrite property: " + name);
    }
}
