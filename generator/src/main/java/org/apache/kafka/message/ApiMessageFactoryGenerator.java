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

public final class ApiMessageFactoryGenerator {
    private final TreeMap<Short, String> requestApis;
    private final TreeMap<Short, String> responseApis;
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;

    public void registerMessageType(MessageSpec spec) {
        if (spec.type() == MessageSpecType.REQUEST) {
            if (requestApis.containsKey(spec.apiKey().get())) {
                throw new RuntimeException("Found more than one request with " +
                    "API key " + spec.apiKey().get());
            }
            requestApis.put(spec.apiKey().get(), spec.generatedClassName());
        } else if (spec.type() == MessageSpecType.RESPONSE) {
            if (responseApis.containsKey(spec.apiKey().get())) {
                throw new RuntimeException("Found more than one response with " +
                    "API key " + spec.apiKey().get());
            }
            responseApis.put(spec.apiKey().get(), spec.generatedClassName());
        }
    }

    public ApiMessageFactoryGenerator() {
        this.requestApis = new TreeMap<>();
        this.responseApis = new TreeMap<>();
        this.headerGenerator = new HeaderGenerator();
        this.buffer = new CodeBuffer();
    }

    public void generate() {
        buffer.printf("public final class ApiMessageFactory {%n");
        buffer.incrementIndent();
        generateFactoryMethod("request", requestApis);
        buffer.printf("%n");
        generateFactoryMethod("response", responseApis);
        buffer.printf("%n");
        generateSchemasAccessor("request", requestApis);
        buffer.printf("%n");
        generateSchemasAccessor("response", responseApis);
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();
    }

    public void generateFactoryMethod(String type, TreeMap<Short, String> apis) {
        headerGenerator.addImport(MessageGenerator.MESSAGE_CLASS);
        buffer.printf("public static Message new%s(short apiKey) {%n",
            MessageGenerator.capitalizeFirst(type));
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, String> entry : apis.entrySet()) {
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return new %s();%n", entry.getValue());
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unsupported %s API key \"" +
            " + apiKey);%n", type);
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    public void generateSchemasAccessor(String type, TreeMap<Short, String> apis) {
        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS);
        buffer.printf("public static Schema[] %sSchemas(short apiKey) {%n",
            MessageGenerator.lowerCaseFirst(type));
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, String> entry : apis.entrySet()) {
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %s.SCHEMAS;%n", entry.getValue());
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unsupported %s API key \"" +
            " + apiKey);%n", type);
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    public void write(BufferedWriter writer) throws IOException {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }
}
