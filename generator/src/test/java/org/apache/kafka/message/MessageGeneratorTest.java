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
        assertEquals(0xFFFF, MessageGenerator.UNSIGNED_SHORT_MAX);
        assertEquals(0xFFFFFFFFL, MessageGenerator.UNSIGNED_INT_MAX);
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
                "  \"name\": \"FooBarResponse\",",
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

    private static MessageSpec spec(int apiKey, String type, String name, String validVersions,
                                    String flexibleVersions, String headerVersions) throws Exception {
        String json = "{'apiKey': " + apiKey + ", 'type': '" + type + "', 'name': '" + name + "', " +
            "'validVersions': '" + validVersions + "', 'flexibleVersions': '" + flexibleVersions + "'" +
            (headerVersions == null ? "" : ", 'headerVersions': " + headerVersions) +
            ", 'fields': [{'name': 'Field1', 'type': 'int32', 'versions': '0+'}]}";
        return MessageGenerator.JSON_SERDE.readValue(json.replace('\'', '"'), MessageSpec.class);
    }

    // Leading indentation is stripped so the assertions below pin the emitted structure, not its column.
    private static String generateApiMessageTypeSource(MessageSpec... specs) throws Exception {
        ApiMessageTypeGenerator generator = new ApiMessageTypeGenerator("org.apache.kafka.common.message");
        for (MessageSpec spec : specs) {
            generator.registerMessageType(spec);
        }
        StringWriter writer = new StringWriter();
        BufferedWriter buffered = new BufferedWriter(writer);
        generator.generateAndWrite(buffered);
        buffered.flush();
        return writer.toString().replaceAll("(?m)^[ \\t]+", "");
    }

    @Test
    public void testHeaderVersionExplicitTwoRangeChain() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(1, "request", "BarRequest", "0-5", "2+", "{'0-1': '1', '2+': '2'}"),
            spec(1, "response", "BarResponse", "0-5", "2+", "{'0-1': '0', '2+': '1'}"));
        assertTrue(source.contains(
            "case 1: // Bar\n" +
            "if (_version >= 2) {\n" +
            "return (short) 2;\n" +
            "} else {\n" +
            "return (short) 1;\n" +
            "}\n"), source);
    }

    @Test
    public void testHeaderVersionExplicitSingleRange() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(2, "request", "BazRequest", "0-5", "0+", "{'0+': '2'}"),
            spec(2, "response", "BazResponse", "0-5", "0+", "{'0+': '1'}"));
        assertTrue(source.contains("case 2: // Baz\nreturn (short) 2;\n"), source);
    }

    @Test
    public void testHeaderVersionFallbackFlexibleDerived() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(3, "request", "QuxRequest", "0-5", "2+", null),
            spec(3, "response", "QuxResponse", "0-5", "2+", null));
        assertTrue(source.contains(
            "case 3: // Qux\n" +
            "if (_version >= 2) {\n" +
            "return (short) 2;\n" +
            "} else {\n" +
            "return (short) 1;\n" +
            "}\n"), source);
    }

    @Test
    public void testHeaderVersionFallbackApiVersionsResponse() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(18, "request", "ApiVersionsRequest", "0-3", "3+", null),
            spec(18, "response", "ApiVersionsResponse", "0-3", "3+", null));
        assertTrue(source.contains(
            "case 18: // ApiVersions\n" +
            "// ApiVersionsResponse always includes a v0 header.\n" +
            "// See KIP-511 for details.\n" +
            "return (short) 0;\n"), source);
    }

    @Test
    public void testHeaderVersionExplicitApiVersionsResponse() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(18, "request", "ApiVersionsRequest", "0-3", "3+", "{'0-2': '1', '3+': '2'}"),
            spec(18, "response", "ApiVersionsResponse", "0-3", "3+", "{'0+': '0'}"));
        assertTrue(source.contains(
            "case 18: // ApiVersions\n" +
            "// ApiVersionsResponse always includes a v0 header.\n" +
            "// See KIP-511 for details.\n" +
            "return (short) 0;\n"), source);
    }

    @Test
    public void testHeaderVersionMixedExplicitAndFallback() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(1, "request", "BarRequest", "0-5", "0+", "{'0+': '2'}"),
            spec(1, "response", "BarResponse", "0-5", "0+", "{'0+': '1'}"),
            spec(3, "request", "QuxRequest", "0-5", "2+", null),
            spec(3, "response", "QuxResponse", "0-5", "2+", null));
        assertTrue(source.contains("case 1: // Bar\nreturn (short) 2;\n"), source);
        assertTrue(source.contains(
            "case 3: // Qux\n" +
            "if (_version >= 2) {\n" +
            "return (short) 2;\n" +
            "} else {\n" +
            "return (short) 1;\n" +
            "}\n"), source);
    }

    @Test
    public void testHeaderVersionCrossValidationFailsForOutOfRangeVersion() throws Exception {
        MessageSpec requestHeader = MessageGenerator.JSON_SERDE.readValue(
            ("{'type': 'header', 'name': 'RequestHeader', 'validVersions': '1-2', 'flexibleVersions': '2+', " +
             "'fields': [{'name': 'Field1', 'type': 'int32', 'versions': '1+'}]}").replace('\'', '"'),
            MessageSpec.class);
        ApiMessageTypeGenerator generator = new ApiMessageTypeGenerator("org.apache.kafka.common.message");
        generator.registerMessageType(requestHeader);
        generator.registerMessageType(spec(5, "request", "FooRequest", "0-5", "0+", "{'0+': '3'}"));
        generator.registerMessageType(spec(5, "response", "FooResponse", "0-5", "0+", "{'0+': '1'}"));
        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> generator.generateAndWrite(new BufferedWriter(new StringWriter())));
        assertTrue(exception.getMessage().contains("not among the valid request header versions"),
            exception.getMessage());
    }

    @Test
    public void testHeaderVersionCrossValidationSkippedWithoutHeaderSpec() throws Exception {
        String source = generateApiMessageTypeSource(
            spec(5, "request", "FooRequest", "0-5", "0+", "{'0+': '3'}"),
            spec(5, "response", "FooResponse", "0-5", "0+", "{'0+': '1'}"));
        assertTrue(source.contains("case 5: // Foo\nreturn (short) 3;\n"), source);
    }

}
