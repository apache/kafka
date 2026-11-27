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

public final class GeneratedRequestConvertToJsonGenerator implements TypeClassGenerator {
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, ApiData> apis;

    private static final class ApiData {
        MessageSpec requestSpec;
        MessageSpec responseSpec;
    }

    public GeneratedRequestConvertToJsonGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.apis = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName() {
        return MessageGenerator.GENERATED_REQUEST_CONVERT_TO_JSON_JAVA;
    }

    @Override
    public void registerMessageType(MessageSpec spec) {
        switch (spec.type()) {
            case REQUEST: {
                short apiKey = spec.apiKey().get();
                ApiData data = apis.computeIfAbsent(apiKey, __ -> new ApiData());
                if (data.requestSpec != null) {
                    throw new RuntimeException(
                            "Found more than one request with API key " + apiKey);
                }
                data.requestSpec = spec;
                break;

            }
            case RESPONSE: {
                short apiKey = spec.apiKey().get();
                ApiData data = apis.computeIfAbsent(apiKey, __ -> new ApiData());
                if (data.responseSpec != null) {
                    throw new RuntimeException(
                            "Found more than one response with API key " + apiKey);
                }
                data.responseSpec = spec;
                break;
            }
            default:
                break;
        }
    }

    @Override
    public void generateAndWrite(BufferedWriter writer) throws IOException {
        generate();
        write(writer);
    }

    private void generate() {
        buffer.printf("public class GeneratedRequestConvertToJson {%n%n");
        buffer.incrementIndent();

        generateRequestNodeMethod();
        buffer.printf("%n");
        generateResponseNodeMethod();

        buffer.decrementIndent();
        buffer.printf("}%n");

        addStandardImports();
        headerGenerator.generate();
    }

    private void addStandardImports() {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        headerGenerator.addImport("org.apache.kafka.common.requests.AbstractRequest");
        headerGenerator.addImport("org.apache.kafka.common.requests.AbstractResponse");
    }

    private void generateRequestNodeMethod() {
        buffer.printf("public static JsonNode request(AbstractRequest request) {%n");
        buffer.incrementIndent();
        buffer.printf("return switch (request.apiKey()) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            ApiData data = entry.getValue();
            MessageSpec spec = data.requestSpec;
            if (spec == null || !spec.hasValidVersion()) {
                continue;
            }
            String name = MessageGenerator.stripSuffix(spec.name(), MessageGenerator.REQUEST_SUFFIX);
            String dataClass = spec.dataClassName();
            String converterClass = dataClass + "JsonConverter";
            String upperSnake = MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT);

            headerGenerator.addImport(String.format("org.apache.kafka.common.message.%s", dataClass));
            headerGenerator.addImport(String.format("org.apache.kafka.common.message.%s", converterClass));

            buffer.printf("case %s ->%n", upperSnake);
            buffer.incrementIndent();
            if (spec.name().equals("ProduceRequest")) {
                buffer.printf("%s.write((%s) request.data(), request.version(), false);%n", converterClass, dataClass);
            } else {
                buffer.printf("%s.write((%s) request.data(), request.version());%n", converterClass, dataClass);
            }
            buffer.decrementIndent();
        }
        buffer.printf("default ->%n");
        buffer.incrementIndent();
        buffer.printf("throw new IllegalStateException(\"ApiKey \" + request.apiKey() + \" is not currently handled in `request`, the \" +%n");
        buffer.printf("    \"code should be updated to do so.\");%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("};%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateResponseNodeMethod() {
        buffer.printf("public static JsonNode response(AbstractResponse response, short version) {%n");
        buffer.incrementIndent();
        buffer.printf("return switch (response.apiKey()) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            ApiData data = entry.getValue();
            MessageSpec spec = data.responseSpec;
            if (spec == null || !spec.hasValidVersion()) {
                continue;
            }

            String name = MessageGenerator.stripSuffix(spec.name(), MessageGenerator.RESPONSE_SUFFIX);
            String dataClass = spec.dataClassName();
            String converterClass = dataClass + "JsonConverter";
            String upperSnake = MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT);

            headerGenerator.addImport(String.format("org.apache.kafka.common.message.%s", dataClass));
            headerGenerator.addImport(String.format("org.apache.kafka.common.message.%s", converterClass));

            buffer.printf("case %s ->%n", upperSnake);
            buffer.incrementIndent();
            if (spec.name().equals("FetchResponse")) {
                buffer.printf("%s.write((%s) response.data(), version, false);%n", converterClass, dataClass);
            } else {
                buffer.printf("%s.write((%s) response.data(), version);%n", converterClass, dataClass);
            }
            buffer.decrementIndent();
        }
        buffer.printf("default ->%n");
        buffer.incrementIndent();
        buffer.printf("throw new IllegalStateException(\"ApiKey \" + response.apiKey() + \" is not currently handled in `response`, the \" +%n");
        buffer.printf("    \"code should be updated to do so.\");%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("};%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void write(BufferedWriter writer) throws IOException {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }
}
