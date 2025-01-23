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

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class CoordinatorRecordJsonConvertersGenerator implements TypeClassGenerator {
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, CoordinatorRecord> records;

    private static final class CoordinatorRecord {
        final short id;
        MessageSpec key;
        MessageSpec value;

        CoordinatorRecord(short id) {
            this.id = id;
        }
    }

    public CoordinatorRecordJsonConvertersGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.records = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName() {
        return MessageGenerator.COORDINATOR_RECORD_JSON_CONVERTERS_JAVA;
    }

    @Override
    public void registerMessageType(MessageSpec spec) {
        switch (spec.type()) {
            case COORDINATOR_KEY: {
                short id = spec.apiKey().get();
                CoordinatorRecord record = records.computeIfAbsent(id, __ -> new CoordinatorRecord(id));
                if (record.key != null) {
                    throw new RuntimeException("Duplicate coordinator record key for type " +
                        id + ". Original claimant: " + record.key.name() + ". New " +
                        "claimant: " + spec.name());
                }
                record.key = spec;
                break;
            }

            case COORDINATOR_VALUE: {
                short id = spec.apiKey().get();
                CoordinatorRecord record = records.computeIfAbsent(id, __ -> new CoordinatorRecord(id));
                if (record.value != null) {
                    throw new RuntimeException("Duplicate coordinator record value for type " +
                        id + ". Original claimant: " + record.key.name() + ". New " +
                        "claimant: " + spec.name());
                }
                record.value = spec;
                break;
            }

            default:
                // Ignore
        }
    }

    @Override
    public void generateAndWrite(BufferedWriter writer) throws IOException {
        buffer.printf("public class CoordinatorRecordJsonConverters {%n");
        buffer.incrementIndent();
        generateWriteKeyJson();
        buffer.printf("%n");
        generateWriteValueJson();
        buffer.printf("%n");
        generateReadKeyFromJson();
        buffer.printf("%n");
        generateReadValueFromJson();
        buffer.printf("%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();

        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }

    private void generateWriteKeyJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);

        buffer.printf("public static JsonNode writeRecordKeyAsJson(ApiMessage key) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (key.apiKey()) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, CoordinatorRecord> entry : records.entrySet()) {
            String apiMessageClassName = MessageGenerator.capitalizeFirst(entry.getValue().key.name());
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %sJsonConverter.write((%s) key, (short) 0);%n", apiMessageClassName, apiMessageClassName);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown record id \"" +
            " + key.apiKey());%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateWriteValueJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);

        buffer.printf("public static JsonNode writeRecordValueAsJson(ApiMessage value, short version) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (value.apiKey()) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, CoordinatorRecord> entry : records.entrySet()) {
            String apiMessageClassName = MessageGenerator.capitalizeFirst(entry.getValue().value.name());
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %sJsonConverter.write((%s) value, version);%n", apiMessageClassName, apiMessageClassName);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown record id \"" +
            " + value.apiKey());%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateReadKeyFromJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);

        buffer.printf("public static ApiMessage readRecordKeyFromJson(JsonNode json, short apiKey) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, CoordinatorRecord> entry : records.entrySet()) {
            String apiMessageClassName = MessageGenerator.capitalizeFirst(entry.getValue().key.name());
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %sJsonConverter.read(json, (short) 0);%n", apiMessageClassName);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown record id \"" +
            " + apiKey);%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateReadValueFromJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);

        buffer.printf("public static ApiMessage readRecordValueFromJson(JsonNode json, short apiKey, short version) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, CoordinatorRecord> entry : records.entrySet()) {
            String apiMessageClassName = MessageGenerator.capitalizeFirst(entry.getValue().value.name());
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %sJsonConverter.read(json, version);%n", apiMessageClassName);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown record id \"" +
            " + apiKey);%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }
}
