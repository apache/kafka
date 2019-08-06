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
 * Creates an if statement based on whether or not a particular field is null.
 */
public final class IsNullConditional {
    static IsNullConditional forField(FieldSpec field, Versions possibleVersions) {
        return new IsNullConditional(field, possibleVersions);
    }

    private final FieldSpec field;
    private final Versions possibleVersions;
    private Runnable ifNull = null;
    private Runnable ifNotNull = null;

    private IsNullConditional(FieldSpec field, Versions possibleVersions) {
        this.field = field;
        this.possibleVersions = possibleVersions;
    }

    IsNullConditional ifNull(Runnable ifNull) {
        this.ifNull = ifNull;
        return this;
    }

    IsNullConditional ifNotNull(Runnable ifNotNull) {
        this.ifNotNull = ifNotNull;
        return this;
    }

    void generate(CodeBuffer buffer) {
        // check if the current version is a nullable version
        VersionConditional.forVersions(field.nullableVersions(), possibleVersions).
            ifMember(() -> {
                if (ifNull != null) {
                    buffer.printf("if (this.%s == null) {%n", field.camelCaseName());
                    buffer.incrementIndent();
                    ifNull.run();
                    buffer.decrementIndent();
                    if (ifNotNull != null) {
                        buffer.printf("} else {%n");
                        buffer.incrementIndent();
                        ifNotNull.run();
                    }
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                } else if (ifNotNull != null) {
                    buffer.printf("if (this.%s != null) {%n", field.camelCaseName());
                    buffer.incrementIndent();
                    ifNull.run();
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                }
            }).
            ifNotMember(ifNotNull).
            generate(buffer);
    }
}
