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

package org.apache.kafka.message;

/**
 * For versions of a field that are nullable, IsNullCondition creates a null check.
 */
public final class IsNullConditional {
    interface ConditionalGenerator {
        String generate(String name, boolean negated);
    }

    private static class PrimitiveConditionalGenerator implements ConditionalGenerator {
        final static PrimitiveConditionalGenerator INSTANCE = new PrimitiveConditionalGenerator();

        @Override
        public String generate(String name, boolean negated) {
            if (negated) {
                return String.format("%s != null", name);
            } else {
                return String.format("%s == null", name);
            }
        }
    }

    static IsNullConditional forName(String name) {
        return new IsNullConditional(name);
    }

    static IsNullConditional forField(FieldSpec field) {
        IsNullConditional cond = new IsNullConditional(field.camelCaseName());
        cond.nullableVersions(field.nullableVersions());
        return cond;
    }

    private final String name;
    private Versions nullableVersions = Versions.ALL;
    private Versions possibleVersions = Versions.ALL;
    private Runnable ifNull = null;
    private Runnable ifShouldNotBeNull = null;
    private boolean alwaysEmitBlockScope = false;
    private ConditionalGenerator conditionalGenerator = PrimitiveConditionalGenerator.INSTANCE;

    private IsNullConditional(String name) {
        this.name = name;
    }

    IsNullConditional nullableVersions(Versions nullableVersions) {
        this.nullableVersions = nullableVersions;
        return this;
    }

    IsNullConditional possibleVersions(Versions possibleVersions) {
        this.possibleVersions = possibleVersions;
        return this;
    }

    IsNullConditional ifNull(Runnable ifNull) {
        this.ifNull = ifNull;
        return this;
    }

    IsNullConditional ifShouldNotBeNull(Runnable ifShouldNotBeNull) {
        this.ifShouldNotBeNull = ifShouldNotBeNull;
        return this;
    }

    IsNullConditional alwaysEmitBlockScope(boolean alwaysEmitBlockScope) {
        this.alwaysEmitBlockScope = alwaysEmitBlockScope;
        return this;
    }

    IsNullConditional conditionalGenerator(ConditionalGenerator conditionalGenerator) {
        this.conditionalGenerator = conditionalGenerator;
        return this;
    }

    void generate(CodeBuffer buffer) {
        if (nullableVersions.intersect(possibleVersions).empty()) {
            if (ifShouldNotBeNull != null) {
                if (alwaysEmitBlockScope) {
                    buffer.printf("{%n");
                    buffer.incrementIndent();
                }
                ifShouldNotBeNull.run();
                if (alwaysEmitBlockScope) {
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                }
            }
        } else {
            if (ifNull != null) {
                buffer.printf("if (%s) {%n", conditionalGenerator.generate(name, false));
                buffer.incrementIndent();
                ifNull.run();
                buffer.decrementIndent();
                if (ifShouldNotBeNull != null) {
                    buffer.printf("} else {%n");
                    buffer.incrementIndent();
                    ifShouldNotBeNull.run();
                    buffer.decrementIndent();
                }
                buffer.printf("}%n");
            } else if (ifShouldNotBeNull != null) {
                buffer.printf("if (%s) {%n", conditionalGenerator.generate(name, true));
                buffer.incrementIndent();
                ifShouldNotBeNull.run();
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }
}
