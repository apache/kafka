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

import org.apache.kafka.connect.errors.DataException;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FutureSchema implements Schema {
    private Schema child;
    private final String name;
    private final boolean optional;

    public FutureSchema(String name, boolean optional) {
        this.child = null;
        this.name = name;
        this.optional = optional;
    }

    private void checkChild() {
        if (child == null)
            throw new DataException("Accessing unresolved FutureSchema(name:" + name + " optional:" + optional + ")");
    }

    /**
     * Resolve this future schema by searching through the parents for a concrete schema that matches.
     * @param parents a list of schemas that are parents of this schema
     * @return the resolved schema
     */
    @Override
    public Schema resolve(List<Schema> parents) {
        if (child == null) {
            if (parents != null) {
                for (Schema parent : parents) {
                    /*
                     * Optionality (nullability) is used as an identifying characteristic because
                     * it is embedded in the schema, as opposed to other systems where
                     * optionality is assigned to the field where the schema is used.
                     */
                    if (parent.name() == name && parent.isOptional() == optional) {
                        child = parent;
                        return child;
                    }
                }
            }
            return this;
        }
        return child;
    }

    /**
     * Return the hashcode of the child if set, so that the wrapper is transparent for comparisons.
     * @return the hashcode
     */
    @Override
    public int hashCode() {
        if (child != null) {
            return child.hashCode();
        } else {
            return Objects.hash(name, optional);
        }
    }

    /**
     * Test equality with the child if set, so that the wrapper is transparent for comparisons.
     * @param o the other object to compare to
     * @return if the objects are equal
     */
    @Override
    public boolean equals(Object o) {
        if (child != null) {
            return child.equals(o);
        } else {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FutureSchema schema = (FutureSchema) o;

            return Objects.equals(name, schema.name) &&
                    Objects.equals(optional, schema.optional);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Type type() {
        checkChild();
        return child.type();
    }

    @Override
    public Object defaultValue() {
        checkChild();
        return child.defaultValue();
    }

    @Override
    public Integer version() {
        checkChild();
        return child.version();
    }

    @Override
    public String doc() {
        checkChild();
        return child.doc();
    }

    @Override
    public Map<String, String> parameters() {
        checkChild();
        return child.parameters();
    }

    @Override
    public Schema keySchema() {
        checkChild();
        return child.keySchema();
    }

    @Override
    public Schema valueSchema() {
        checkChild();
        return child.valueSchema();
    }

    @Override
    public List<Field> fields() {
        checkChild();
        return child.fields();
    }

    @Override
    public Field field(String fieldName) {
        checkChild();
        return child.field(fieldName);
    }

    @Override
    public Schema schema() {
        checkChild();
        return child.schema();
    }
}
