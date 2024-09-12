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

public final class MetadataRecordTypeGenerator implements TypeClassGenerator {
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, MessageSpec> apis;

    public MetadataRecordTypeGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.apis = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName() {
        return MessageGenerator.METADATA_RECORD_TYPE_JAVA;
    }

    @Override
    public void registerMessageType(MessageSpec spec) {
        if (spec.type() == MessageSpecType.METADATA) {
            short id = spec.apiKey().get();
            MessageSpec prevSpec = apis.put(id, spec);
            if (prevSpec != null) {
                throw new RuntimeException("Duplicate metadata record entry for type " +
                    id + ". Original claimant: " + prevSpec.name() + ". New " +
                    "claimant: " + spec.name());
            }
        }
    }

    @Override
    public void generateAndWrite(BufferedWriter writer) throws IOException {
        generate();
        write(writer);
    }

    private void generate() {
        buffer.printf("public enum MetadataRecordType {%n");
        buffer.incrementIndent();
        generateEnumValues();
        buffer.printf("%n");
        generateInstanceVariables();
        buffer.printf("%n");
        generateEnumConstructor();
        buffer.printf("%n");
        generateFromApiKey();
        buffer.printf("%n");
        generateNewMetadataRecord();
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

    private void generateEnumValues() {
        int numProcessed = 0;
        for (Map.Entry<Short, MessageSpec> entry : apis.entrySet()) {
            MessageSpec spec = entry.getValue();
            String name = spec.name();
            numProcessed++;
            buffer.printf("%s(\"%s\", (short) %d, (short) %d, (short) %d)%s%n",
                MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT),
                MessageGenerator.capitalizeFirst(name),
                entry.getKey(),
                entry.getValue().validVersions().lowest(),
                entry.getValue().validVersions().highest(),
                (numProcessed == apis.size()) ? ";" : ",");
        }
    }

    private void generateInstanceVariables() {
        buffer.printf("private final String name;%n");
        buffer.printf("private final short id;%n");
        buffer.printf("private final short lowestSupportedVersion;%n");
        buffer.printf("private final short highestSupportedVersion;%n");
    }

    private void generateEnumConstructor() {
        buffer.printf("MetadataRecordType(String name, short id, short lowestSupportedVersion, short highestSupportedVersion) {%n");
        buffer.incrementIndent();
        buffer.printf("this.name = name;%n");
        buffer.printf("this.id = id;%n");
        buffer.printf("this.lowestSupportedVersion = lowestSupportedVersion;%n");
        buffer.printf("this.highestSupportedVersion = highestSupportedVersion;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFromApiKey() {
        buffer.printf("public static MetadataRecordType fromId(short id) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (id) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, MessageSpec> entry : apis.entrySet()) {
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %s;%n", MessageGenerator.
                toSnakeCase(entry.getValue().name()).toUpperCase(Locale.ROOT));
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

    private void generateNewMetadataRecord() {
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);
        buffer.printf("public ApiMessage newMetadataRecord() {%n");
        buffer.incrementIndent();
        buffer.printf("switch (id) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, MessageSpec> entry : apis.entrySet()) {
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return new %s();%n",
                MessageGenerator.capitalizeFirst(entry.getValue().name()));
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
