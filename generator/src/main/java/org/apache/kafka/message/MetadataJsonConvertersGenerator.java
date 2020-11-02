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

public class MetadataJsonConvertersGenerator implements TypeClassGenerator {
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, MessageSpec> apis;

    public MetadataJsonConvertersGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.apis = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName() {
        return MessageGenerator.METADATA_JSON_CONVERTERS_JAVA;
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
        buffer.printf("public class MetadataJsonConverters {%n");
        buffer.incrementIndent();
        generateWriteJson();
        buffer.printf("%n");
        generateReadJson();
        buffer.printf("%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();

        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }

    private void generateWriteJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);

        buffer.printf("public static JsonNode writeJson(ApiMessage apiMessage, short apiVersion) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (apiMessage.apiKey()) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, MessageSpec> entry : apis.entrySet()) {
            String apiMessageClassName = MessageGenerator.capitalizeFirst(entry.getValue().name());
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %sJsonConverter.write((%s) apiMessage, apiVersion);%n", apiMessageClassName, apiMessageClassName);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown metadata id \"" +
                " + apiMessage.apiKey());%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateReadJson() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);

        buffer.printf("public static ApiMessage readJson(JsonNode json, short apiKey, short apiVersion) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, MessageSpec> entry : apis.entrySet()) {
            String apiMessageClassName = MessageGenerator.capitalizeFirst(entry.getValue().name());
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %sJsonConverter.read(json, apiVersion);%n", apiMessageClassName);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unknown metadata id \"" +
                " + apiKey);%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }
}
