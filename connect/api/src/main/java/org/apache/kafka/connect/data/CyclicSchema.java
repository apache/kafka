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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class CyclicSchema implements Schema {

    protected abstract Schema underlying();

    @Override
    public Type type() {
        return underlying().type();
    }

    @Override
    public boolean isOptional() {
        return underlying().isOptional();
    }

    @Override
    public Object defaultValue() {
        return underlying().defaultValue();
    }

    @Override
    public String name() {
        return underlying().name();
    }

    @Override
    public Integer version() {
        return underlying().version();
    }

    @Override
    public String doc() {
        return underlying().doc();
    }

    @Override
    public Map<String, String> parameters() {
        return underlying().parameters();
    }

    @Override
    public List<Field> fields() {
        return underlying().fields();
    }

    @Override
    public Field field(String fieldName) {
        return underlying().field(fieldName);
    }

    @Override
    public Schema keySchema() {
        return underlying().keySchema();
    }

    @Override
    public Schema valueSchema() {
        return underlying().valueSchema();
    }

    @Override
    public Schema schema() {
        return this;
    }

    private static final class SchemaPairIdentityComparison {
        final Schema a;
        final Schema b;

        private SchemaPairIdentityComparison(Schema a, Schema b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaPairIdentityComparison that = (SchemaPairIdentityComparison) o;
            return a == that.a && b == that.b;
        }

        @Override
        public int hashCode() {
            return Objects.hash(System.identityHashCode(a), System.identityHashCode(b));
        }
    }

    private final ThreadLocal<Set<SchemaPairIdentityComparison>> comparisonContext = new ThreadLocal<Set<SchemaPairIdentityComparison>>() {
        @Override
        protected Set<SchemaPairIdentityComparison> initialValue() {
            return new HashSet<>();
        }
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CyclicSchema)) return false;
        final CyclicSchema that = (CyclicSchema) o;

        final Set<SchemaPairIdentityComparison> currentComparisonContext = comparisonContext.get();

        final SchemaPairIdentityComparison schemaPair = new SchemaPairIdentityComparison(this, that);

        if (currentComparisonContext.contains(schemaPair)) {
            return true;
        }

        if (!(Objects.equals(type(), that.type())
                && Objects.equals(isOptional(), that.isOptional())
                && Objects.equals(defaultValue(), that.defaultValue())
                && Objects.equals(name(), that.name())
                && Objects.equals(version(), that.version())
                && Objects.equals(doc(), that.doc()))) {
            return false;
        }

        if (!type().isPrimitive()) {
            try {
                currentComparisonContext.add(schemaPair);
            } catch (Throwable t) {
                // Fail-safe -- don't want to leave dirty thread-local state
                currentComparisonContext.clear();
                throw t;
            }

            try {
                switch (type()) {
                    case STRUCT:
                        return Objects.equals(fields(), that.fields());
                    case ARRAY:
                        return Objects.equals(valueSchema(), that.valueSchema());
                    case MAP:
                        return Objects.equals(keySchema(), that.keySchema()) && Objects.equals(valueSchema(), that.valueSchema());
                }
            } finally {
                currentComparisonContext.remove(schemaPair);
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type(), isOptional(), defaultValue(), name(), version(), doc());
    }

    @Override
    public String toString() {
        return "CyclicSchema{" + name() + ":" + type() + "}";
    }

}
