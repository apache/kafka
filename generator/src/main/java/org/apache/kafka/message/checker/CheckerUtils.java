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

import org.apache.kafka.message.FieldSpec;
import org.apache.kafka.message.MessageGenerator;
import org.apache.kafka.message.MessageSpec;
import org.apache.kafka.message.Versions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utilities for the metadata schema checker.
 */
class CheckerUtils {
    /**
     * A min function defined for shorts.
     *
     * @param a     The first short integer to compare.
     * @param b     The second short integer to compare.
     * @return      The minimum short integer.
     */
    static short min(short a, short b) {
        return a < b ? a : b;
    }

    /**
     * A max function defined for shorts.
     *
     * @param a     The first short integer to compare.
     * @param b     The second short integer to compare.
     * @return      The maximum short integer.
     */
    static short max(short a, short b) {
        return a > b ? a : b;
    }

    /**
     * Validate the a field doesn't have tagged versions that are outside of the top-level flexible
     * versions.
     *
     * @param what                      A description of the field.
     * @param field                     The field to validate.
     * @param topLevelFlexibleVersions  The top-level flexible versions.
     */
    static void validateTaggedVersions(
        String what,
        FieldSpec field,
        Versions topLevelFlexibleVersions
    ) {
        if (!field.flexibleVersions().isPresent()) {
            if (!topLevelFlexibleVersions.contains(field.taggedVersions())) {
                throw new RuntimeException("Tagged versions for " + what + " " +
                        field.name() + " are " + field.taggedVersions() + ", but top " +
                        "level flexible versions are " + topLevelFlexibleVersions);
            }
        }
    }

    /**
     * Read a MessageSpec file from a path.
     *
     * @param schemaPath    The path to read the file from.
     * @return              The MessageSpec.
     */
    static MessageSpec readMessageSpecFromFile(String schemaPath) {
        if (!Files.isReadable(Paths.get(schemaPath))) {
            throw new RuntimeException("Path " + schemaPath + " does not point to " +
                    "a readable file.");
        }
        try {
            return MessageGenerator.JSON_SERDE.readValue(new File(schemaPath), MessageSpec.class);
        } catch (Exception e) {
            throw new RuntimeException("Unable to parse file as MessageSpec: " + schemaPath, e);
        }
    }
}
