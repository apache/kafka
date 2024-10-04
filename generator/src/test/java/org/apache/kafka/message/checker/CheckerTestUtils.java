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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.OptionalInt;

public class CheckerTestUtils {
    static String singleQuotesToDoubleQuotes(String input) {
        return input.replaceAll("'", "\"");
    }

    static MessageSpec toMessage(String input) throws Exception {
        return MessageGenerator.JSON_SERDE.
            readValue(singleQuotesToDoubleQuotes(input), MessageSpec.class);
    }

    static FieldSpec field(
        String name,
        String versions,
        String type
    ) {
        return new FieldSpec(name,
            versions,
            null,
            type,
            false,
            null,
            null,
            false,
            null,
            "",
            null,
            null,
            null,
            false);
    }

    static FieldSpec fieldWithTag(
        String name,
        OptionalInt tag
    ) {
        return new FieldSpec(name,
            "0+",
            null,
            "int8",
            false,
            null,
            null,
            false,
            null,
            null,
            tag.isPresent() ? "0+" : "",
            null,
            tag.isPresent() ? tag.getAsInt() : null,
            false);
    }

    static FieldSpec fieldWithTag(
        String name,
        int tag,
        String validVersions,
        String taggedVersions
    ) {
        return new FieldSpec(name,
            validVersions,
            null,
            "int8",
            false,
            null,
            null,
            false,
            null,
            null,
            taggedVersions,
            null,
            tag,
            false);
    }

    static FieldSpec fieldWithNulls(
        String name,
        String versions,
        String type,
        String nullableVersions
    ) {
        return new FieldSpec(name,
            versions,
            null,
            type,
            false,
            nullableVersions,
            null,
            false,
            null,
            "",
            null,
            null,
            null,
            false);
    }

    static FieldSpec fieldWithDefaults(
        String name,
        String versions,
        String fieldDefault,
        String flexibleVersions
    ) {
        return new FieldSpec(name,
            versions,
            null,
            "string",
            false,
            null,
            fieldDefault,
            false,
            null,
            "",
            null,
            flexibleVersions,
            null,
            false);
    }

    static String messageSpecStringToTempFile(String input) throws IOException {
        File file = Files.createTempFile("MetadataSchemaCheckerToolTest", null).toFile();
        file.deleteOnExit();
        MessageSpec messageSpec = MessageGenerator.JSON_SERDE.
                readValue(input.replaceAll("'", "\""), MessageSpec.class);
        MessageGenerator.JSON_SERDE.writeValue(file, messageSpec);
        return file.getAbsolutePath();
    }
}
