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

import java.util.Collections;
import java.util.function.Function;

public final class Target {
    private final FieldSpec field;
    private final String sourceVariable;
    private final String humanReadableName;
    private final Function<String, String> assignmentStatementGenerator;

    Target(FieldSpec field, String sourceVariable, String humanReadableName,
           Function<String, String> assignmentStatementGenerator) {
        this.field = field;
        this.sourceVariable = sourceVariable;
        this.humanReadableName = humanReadableName;
        this.assignmentStatementGenerator = assignmentStatementGenerator;
    }

    public String assignmentStatement(String rightHandSide) {
        return assignmentStatementGenerator.apply(rightHandSide);
    }

    public Target nonNullableCopy() {
        FieldSpec nonNullableField = new FieldSpec(field.name(),
            field.versionsString(),
            field.fields(),
            field.typeString(),
            field.mapKey(),
            Versions.NONE.toString(),
            field.defaultString(),
            field.ignorable(),
            field.entityType(),
            field.about(),
            field.taggedVersionsString(),
            field.flexibleVersionsString(),
            field.tagInteger(),
            field.zeroCopy());
        return new Target(nonNullableField, sourceVariable, humanReadableName, assignmentStatementGenerator);
    }

    public Target arrayElementTarget(Function<String, String> assignmentStatementGenerator) {
        if (!field.type().isArray()) {
            throw new RuntimeException("Field " + field + " is not an array.");
        }
        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
        FieldSpec elementField = new FieldSpec(field.name() + "Element",
                field.versions().toString(),
                Collections.emptyList(),
                arrayType.elementType().toString(),
                false,
                Versions.NONE.toString(),
                "",
                false,
                EntityType.UNKNOWN,
                "",
                Versions.NONE.toString(),
                field.flexibleVersionsString(),
                null,
                field.zeroCopy());
        return new Target(elementField, "_element", humanReadableName + " element",
            assignmentStatementGenerator);
    }

    public FieldSpec field() {
        return field;
    }

    public String sourceVariable() {
        return sourceVariable;
    }

    public String humanReadableName() {
        return humanReadableName;
    }
}
