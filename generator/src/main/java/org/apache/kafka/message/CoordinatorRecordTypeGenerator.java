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
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class CoordinatorRecordTypeGenerator implements TypeClassGenerator {
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

    public CoordinatorRecordTypeGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.records = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName() {
        return MessageGenerator.COORDINATOR_RECORD_TYPE_JAVA;
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
        generate();
        write(writer);
    }

    private void generate() {
        buffer.printf("public enum CoordinatorRecordType {%n");
        buffer.incrementIndent();
        generateEnumValues();
        buffer.printf("%n");
        generateInstanceVariables();
        buffer.printf("%n");
        generateEnumConstructor();
        buffer.printf("%n");
        generateFromApiKey();
        buffer.printf("%n");
        generateAccessor("id", "short");
        buffer.printf("%n");
        generateAccessor("lowestSupportedVersion", "short");
        buffer.printf("%n");
        generateAccessor("highestSupportedVersion", "short");
        buffer.printf("%n");
        generateToString();
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();
    }

    private String cleanName(String name) {
        return name
            .replace("Key", "")
            .replace("Value", "");
    }

    private void generateEnumValues() {
        int numProcessed = 0;
        for (Map.Entry<Short, CoordinatorRecord> entry : records.entrySet()) {
            MessageSpec key = entry.getValue().key;
            if (key == null) {
                throw new RuntimeException("Coordinator record " + entry.getKey() + " has not key.");
            }
            MessageSpec value = entry.getValue().value;
            if (value == null) {
                throw new RuntimeException("Coordinator record " + entry.getKey() + " has not key.");
            }
            String name = cleanName(key.name());
            numProcessed++;
            buffer.printf("%s(\"%s\", (short) %d, (short) %d, (short) %d)%s%n",
                MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT),
                MessageGenerator.capitalizeFirst(name),
                entry.getKey(),
                value.validVersions().lowest(),
                value.validVersions().highest(),
                (numProcessed == records.size()) ? ";" : ",");
        }
    }

    private void generateInstanceVariables() {
        buffer.printf("private final String name;%n");
        buffer.printf("private final short id;%n");
        buffer.printf("private final short lowestSupportedVersion;%n");
        buffer.printf("private final short highestSupportedVersion;%n");
    }

    private void generateEnumConstructor() {
        buffer.printf("CoordinatorRecordType(String name, short id, short lowestSupportedVersion, short highestSupportedVersion) {%n");
        buffer.incrementIndent();
        buffer.printf("this.name = name;%n");
        buffer.printf("this.id = id;%n");
        buffer.printf("this.lowestSupportedVersion = lowestSupportedVersion;%n");
        buffer.printf("this.highestSupportedVersion = highestSupportedVersion;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFromApiKey() {
        buffer.printf("public static CoordinatorRecordType fromId(short id) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (id) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, CoordinatorRecord> entry : records.entrySet()) {
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %s;%n", MessageGenerator.
                toSnakeCase(cleanName(entry.getValue().key.name())).toUpperCase(Locale.ROOT));
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown metadata id \"" +
            " + id);%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateAccessor(String name, String type) {
        buffer.printf("public %s %s() {%n", type, name);
        buffer.incrementIndent();
        buffer.printf("return this.%s;%n", name);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateToString() {
        buffer.printf("@Override%n");
        buffer.printf("public String toString() {%n");
        buffer.incrementIndent();
        buffer.printf("return this.name();%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void write(BufferedWriter writer) throws IOException {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }
}
