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

class FutureSchema extends ConnectSchema {
    private Schema child;

    public FutureSchema(String name, boolean optional, Object defaultValue) {
        super(null, optional, defaultValue, name, null, null);
        this.child = null;
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
                    if (parent.name() == name) {
                        child = parent;
                        return this;
                    }
                }
            }
        }
        return this;
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
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (child == null) {
            if (this == o) return true;
            if (o == null || !(o instanceof FutureSchema)) return false;
            FutureSchema schema = (FutureSchema) o;
            return Objects.equals(name(), schema.name()) &&
                Objects.equals(isOptional(), schema.isOptional()) &&
                Objects.equals(defaultValue(), schema.defaultValue());
        }
        return super.equals(o);
    }

    @Override
    public Schema schema() {
        return this;
    }

    @Override
    public int hashCode() {
        if (child == null) {
            return Objects.hash(name(), isOptional(), defaultValue());
        }
        return super.hashCode();
    }

    @Override
    public Type type() {
        checkChild();
        return child.type();
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
}