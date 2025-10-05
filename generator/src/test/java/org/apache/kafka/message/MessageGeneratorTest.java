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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class MessageGeneratorTest {

    @Test
    public void testCapitalizeFirst() {
        assertEquals("", MessageGenerator.capitalizeFirst(""));
        assertEquals("AbC", MessageGenerator.capitalizeFirst("abC"));
    }

    @Test
    public void testLowerCaseFirst() {
        assertEquals("", MessageGenerator.lowerCaseFirst(""));
        assertEquals("fORTRAN", MessageGenerator.lowerCaseFirst("FORTRAN"));
        assertEquals("java", MessageGenerator.lowerCaseFirst("java"));
    }

    @Test
    public void testFirstIsCapitalized() {
        assertFalse(MessageGenerator.firstIsCapitalized(""));
        assertTrue(MessageGenerator.firstIsCapitalized("FORTRAN"));
        assertFalse(MessageGenerator.firstIsCapitalized("java"));
    }

    @Test
    public void testToSnakeCase() {
        assertEquals("", MessageGenerator.toSnakeCase(""));
        assertEquals("foo_bar_baz", MessageGenerator.toSnakeCase("FooBarBaz"));
        assertEquals("foo_bar_baz", MessageGenerator.toSnakeCase("fooBarBaz"));
        assertEquals("fortran", MessageGenerator.toSnakeCase("FORTRAN"));
    }

    @Test
    public void stripSuffixTest() {
        assertEquals("FooBa", MessageGenerator.stripSuffix("FooBar", "r"));
        assertEquals("", MessageGenerator.stripSuffix("FooBar", "FooBar"));
        assertEquals("Foo", MessageGenerator.stripSuffix("FooBar", "Bar"));
        assertThrows(RuntimeException.class, () -> MessageGenerator.stripSuffix("FooBar", "Baz"));
    }

    @Test
    public void testConstants() {
        assertEquals(MessageGenerator.UNSIGNED_SHORT_MAX, 0xFFFF);
        assertEquals(MessageGenerator.UNSIGNED_INT_MAX, 0xFFFFFFFFL);
    }

    @Test
    public void testGenerateAndWriteMessageClasses(@TempDir Path tempDir) throws Exception {
        var generatorTypes = List.of("MessageDataGenerator", "JsonConverterGenerator");

        MessageSpec testRequestSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", List.of(
                "{",
                "  \"apiKey\": 0,",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBarRequest\",",
                "  \"validVersions\": \"none\"",
                "}")), MessageSpec.class);
        MessageSpec testResponseSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", List.of(
                "{",
                "  \"apiKey\": 0,",
                "  \"type\": \"response\",",
                "  \"name\": \"FooBarRespose\",",
                "  \"validVersions\": \"none\"",
                "}")), MessageSpec.class);

        var outputFiles = MessageGenerator.generateAndWriteMessageClasses(testRequestSpec, "kafka",
            tempDir.toAbsolutePath().toString(), generatorTypes);
        assertEquals(Set.of(), outputFiles);
        outputFiles = MessageGenerator.generateAndWriteMessageClasses(testResponseSpec, "kafka",
                tempDir.toAbsolutePath().toString(), generatorTypes);
        assertEquals(Set.of(), outputFiles);
        var typeGenerator = new ApiMessageTypeGenerator("kafka");
        typeGenerator.registerMessageType(testRequestSpec);
        typeGenerator.registerMessageType(testResponseSpec);
        typeGenerator.generateAndWrite(new BufferedWriter(new StringWriter()));

        testRequestSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"apiKey\": 0,",
                "  \"type\": \"request\",",
                "  \"name\": \"FooBarRequest\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"none\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" }",
                "  ]",
                "}")), MessageSpec.class);
        testResponseSpec = MessageGenerator.JSON_SERDE.readValue(String.join("", Arrays.asList(
                "{",
                "  \"apiKey\": 0,",
                "  \"type\": \"response\",",
                "  \"name\": \"FooBarResponse\",",
                "  \"validVersions\": \"0-2\",",
                "  \"flexibleVersions\": \"none\",",
                "  \"fields\": [",
                "    { \"name\": \"field1\", \"type\": \"int32\", \"versions\": \"0+\" }",
                "  ]",
                "}")), MessageSpec.class);

        outputFiles = MessageGenerator.generateAndWriteMessageClasses(testRequestSpec, "kafka",
                tempDir.toAbsolutePath().toString(), generatorTypes);
        assertEquals(Set.of("FooBarRequestDataJsonConverter.java", "FooBarRequestData.java"), outputFiles);
        outputFiles = MessageGenerator.generateAndWriteMessageClasses(testResponseSpec, "kafka",
                tempDir.toAbsolutePath().toString(), generatorTypes);
        assertEquals(Set.of("FooBarResponseDataJsonConverter.java", "FooBarResponseData.java"), outputFiles);
        typeGenerator = new ApiMessageTypeGenerator("kafka");
        typeGenerator.registerMessageType(testRequestSpec);
        typeGenerator.registerMessageType(testResponseSpec);
        typeGenerator.generateAndWrite(new BufferedWriter(new StringWriter()));
    }

}
