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

package org.apache.kafka.message.checker;

import org.apache.kafka.message.EntityType;
import org.apache.kafka.message.FieldSpec;
import org.apache.kafka.message.FieldType;
import org.apache.kafka.message.MessageGenerator;
import org.apache.kafka.message.MessageSpec;
import org.apache.kafka.message.StructRegistry;
import org.apache.kafka.message.StructSpec;
import org.apache.kafka.message.Versions;

import java.util.List;

import static org.apache.kafka.message.checker.CheckerUtils.max;
import static org.apache.kafka.message.checker.CheckerUtils.min;

/**
 * The unifier attempts to unify the types of two KRPC messages. In other words, to see them as
 * two different points in the evolution of a single message
 * specification.
 */
class Unifier {
    private final MessageSpec topLevelMessage1;
    private final StructRegistry structRegistry1;
    private final MessageSpec topLevelMessage2;
    private final StructRegistry structRegistry2;

    Unifier(
        MessageSpec topLevelMessage1,
        MessageSpec topLevelMessage2
    ) throws Exception {
        this.topLevelMessage1 = topLevelMessage1;
        this.structRegistry1 = new StructRegistry();
        this.structRegistry1.register(topLevelMessage1);
        this.topLevelMessage2 = topLevelMessage2;
        this.structRegistry2 = new StructRegistry();
        this.structRegistry1.register(topLevelMessage2);
    }

    static FieldSpec structSpecToFieldSpec(StructSpec structSpec) {
        return new FieldSpec(structSpec.name(),
            structSpec.versions().toString(),
            structSpec.fields(),
            MessageGenerator.capitalizeFirst(structSpec.name()),
            false,
            "",
            "",
            false,
            EntityType.UNKNOWN,
            "Top level StructSpec",
            "",
            null,
            null,
            false);
    }

    void unify() {
        unify(structSpecToFieldSpec(topLevelMessage1.struct()),
            structSpecToFieldSpec(topLevelMessage2.struct()));
    }

    void unify(FieldSpec field1, FieldSpec field2) {
        // If the types don't match, then these fields cannot be unified. Of course, even if the
        // types do match, we might be looking at two different structs, or something like that.
        if (!field2.type().toString().equals(field1.type().toString())) {
            throw new UnificationException("Field type for field2 " + field2.name() + " is " +
                    field2.type() + ", but field type for field1 " + field1.name() + " is " +
                    field1.type());
        }

        // The maximum supported version in field2 must be not be lower than the maximum supported
        // version in field1.
        short f1Highest = min(field1.versions().highest(), topLevelMessage1.validVersions().highest());
        short f2Highest = min(field2.versions().highest(), topLevelMessage2.validVersions().highest());
        if (f2Highest < f1Highest) {
            throw new UnificationException("Maximum effective valid version for field2 " +
                field2.name() + ", '" + f2Highest + "' cannot be lower than the " +
                "maximum effective valid version for field1 " + field1.name() + ", '" + f1Highest + "'");
        }
        // The minimum supported version in field2 must not be different from the minimum supported
        // version in field1.
        short f1Lowest = max(field1.versions().lowest(), topLevelMessage2.validVersions().lowest());
        short f2Lowest = max(field2.versions().lowest(), topLevelMessage2.validVersions().lowest());
        if (f2Lowest != f1Lowest) {
            throw new UnificationException("Minimum effective valid version for field2 " +
                field2.name() + ", '" + f2Lowest + "' cannot be different than the " +
                "minimum effective valid version for field1 " + field1.name() + ", '" +
                f1Lowest + "'");
        }
        // The maximum nullable version in field2 must not be lower than the maximum nullable
        // version in field1.
        short f1HighestNull = min(f1Highest, field2.nullableVersions().highest());
        short f2HighestNull = min(f2Highest, field1.nullableVersions().highest());
        if (f2HighestNull < f1HighestNull) {
            throw new UnificationException("Maximum effective nullable version for field2 " +
                field2.name() + ", '" + f2HighestNull + "' cannot be lower than the " +
                "minimum effective nullable version for field1 " + field1.name() + ", '" +
                f1HighestNull + "'");
        }
        // The minimum nullable version in field2 must not be different from the minimum nullable
        // version in field1.
        short f1LowestNull = max(field1.nullableVersions().lowest(), topLevelMessage2.validVersions().lowest());
        short f2LowestNull = max(field2.nullableVersions().lowest(), topLevelMessage2.validVersions().lowest());
        if (f2LowestNull != f1LowestNull) {
            throw new UnificationException("Minimum effective nullable version for field2 " +
                field2.name() + ", '" + f2LowestNull + "' cannot be different than the " +
                "minimum effective nullable version for field1 " + field1.name() + ", '" +
                f1LowestNull + "'");
        }
        // Check that the flexibleVersions match exactly. Currently, there is only one case where
        // flexibleVersions is set on a FieldSpec object: the FieldSpec is the ClientId string
        // used in RequestHeader.json In every other case, FieldSpec.flexibleVersions() will be
        // Optional.empty.
        Versions field2EffectiveFlexibleVersions = field2.flexibleVersions().
                orElseGet(() -> topLevelMessage2.flexibleVersions());
        Versions field1EffectiveFlexibleVersions = field1.flexibleVersions().
                orElseGet(() -> topLevelMessage1.flexibleVersions());
        if (!field2EffectiveFlexibleVersions.contains(field1EffectiveFlexibleVersions)) {
            throw new UnificationException("Flexible versions for field2 " + field2.name() +
                " is " + field2.flexibleVersions().orElseGet(() -> Versions.NONE) +
                ", but flexible versions for field1 is " +
                field1.flexibleVersions().orElseGet(() -> Versions.NONE));
        }
        // Check that defaults match exactly.
        if (!field2.defaultString().equals(field1.defaultString())) {
            throw new UnificationException("Default for field2 " + field2.name() + " is '" +
                field2.defaultString() + "', but default for field1 " + field1.name() + " is '" +
                field1.defaultString() + "'");
        }
        // Recursive step.
        if (field1.type().isStruct()) {
            unifyStructs(field1.name(),
                field1.fields(),
                field2.name(),
                field2.fields());
        } else if (field2.type().isStructArray()) {
            unifyStructs(((FieldType.ArrayType) field1.type()).elementName(),
                field1.fields(),
                ((FieldType.ArrayType) field2.type()).elementName(),
                field2.fields());
        }
    }

    void unifyStructs(
        String struct1Name,
        List<FieldSpec> struct1Fields,
        String struct2Name,
        List<FieldSpec> struct2Fields
    ) {
        // By convention, structure names are always uppercase.
        struct1Name = MessageGenerator.capitalizeFirst(struct1Name);
        struct2Name = MessageGenerator.capitalizeFirst(struct2Name);
        // If the list of struct fields is empty, it is assumed that the structure is defined in
        // commonStructs. We have to look it up there in order to find its fields.
        if (struct1Fields.isEmpty()) {
            struct1Fields = lookupCommonStructFields(struct1Name, structRegistry1);
        }
        if (struct2Fields.isEmpty()) {
            struct2Fields = lookupCommonStructFields(struct2Name, structRegistry2);
        }
        for (FieldSpec field1 : struct1Fields) {
            CheckerUtils.validateTaggedVersions("field1", field1, topLevelMessage1.flexibleVersions());
        }
        for (FieldSpec field2 : struct2Fields) {
            CheckerUtils.validateTaggedVersions("field2", field2, topLevelMessage2.flexibleVersions());
        }
        // Iterate over fields1 and fields2.
        FieldSpecPairIterator iterator = new FieldSpecPairIterator(struct1Fields.iterator(),
            struct2Fields.iterator(),
            topLevelMessage1.validVersions(),
            topLevelMessage2.validVersions());
        while (iterator.hasNext()) {
            FieldSpecPair pair = iterator.next();
            unify(pair.field1(), pair.field2());
        }
    }

    List<FieldSpec> lookupCommonStructFields(
        String structName,
        StructRegistry structRegistry
    ) {
        StructSpec struct = structRegistry.findStruct(structName);
        // TODO: we should probably validate the versions, etc. settings of the common struct.
        // Maybe force them to always be 0+ since there it makes more sense to restrict them
        // at the point of usage, not definition.
        return struct.fields();
    }
}
