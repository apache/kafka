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

public final class ApiMessageTypeGenerator {
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, ApiData> apis;

    private static final class ApiData {
        short apiKey;
        MessageSpec requestSpec;
        boolean requestContainsZeroCopyFields;
        MessageSpec responseSpec;
        boolean responseContainsZeroCopyFields;

        ApiData(short apiKey) {
            this.apiKey = apiKey;
        }

        String name() {
            if (requestSpec != null) {
                return MessageGenerator.stripSuffix(requestSpec.name(),
                    MessageGenerator.REQUEST_SUFFIX);
            } else if (responseSpec != null) {
                return MessageGenerator.stripSuffix(responseSpec.name(),
                    MessageGenerator.RESPONSE_SUFFIX);
            } else {
                throw new RuntimeException("Neither requestSpec nor responseSpec is defined " +
                    "for API key " + apiKey);
            }
        }

        String requestSchema() {
            if (requestSpec == null) {
                return "null";
            } else {
                return String.format("%sData.SCHEMAS", requestSpec.name());
            }
        }

        String responseSchema() {
            if (responseSpec == null) {
                return "null";
            } else {
                return String.format("%sData.SCHEMAS", responseSpec.name());
            }
        }
    }

    public ApiMessageTypeGenerator(String packageName) {
        this.headerGenerator = new HeaderGenerator(packageName);
        this.apis = new TreeMap<>();
        this.buffer = new CodeBuffer();
    }

    public boolean hasRegisteredTypes() {
        return !apis.isEmpty();
    }

    void registerMessageType(MessageSpec spec, boolean containsZeroCopyFields) {
        switch (spec.type()) {
            case REQUEST: {
                short apiKey = spec.apiKey().get();
                ApiData data = apis.get(apiKey);
                if (!apis.containsKey(apiKey)) {
                    data = new ApiData(apiKey);
                    apis.put(apiKey, data);
                }
                if (data.requestSpec != null) {
                    throw new RuntimeException("Found more than one request with " +
                        "API key " + spec.apiKey().get());
                }
                data.requestSpec = spec;
                data.requestContainsZeroCopyFields = containsZeroCopyFields;
                break;
            }
            case RESPONSE: {
                short apiKey = spec.apiKey().get();
                ApiData data = apis.get(apiKey);
                if (!apis.containsKey(apiKey)) {
                    data = new ApiData(apiKey);
                    apis.put(apiKey, data);
                }
                if (data.responseSpec != null) {
                    throw new RuntimeException("Found more than one response with " +
                        "API key " + spec.apiKey().get());
                }
                data.responseSpec = spec;
                data.responseContainsZeroCopyFields = containsZeroCopyFields;
                break;
            }
            default:
                // do nothing
                break;
        }
    }

    public void generate() {
        buffer.printf("public enum ApiMessageType {%n");
        buffer.incrementIndent();
        generateEnumValues();
        buffer.printf("%n");
        generateInstanceVariables();
        buffer.printf("%n");
        generateEnumConstructor();
        buffer.printf("%n");
        generateFromApiKey();
        buffer.printf("%n");
        generateNewApiMessageMethod("request");
        buffer.printf("%n");
        generateNewApiMessageMethod("response");
        buffer.printf("%n");
        generateAccessor("apiKey", "short");
        buffer.printf("%n");
        generateAccessor("requestSchemas", "Schema[]");
        buffer.printf("%n");
        generateAccessor("responseSchemas", "Schema[]");
        buffer.printf("%n");
        generateAccessor("requestContainsZeroCopyFields", "boolean");
        buffer.printf("%n");
        generateAccessor("responseContainsZeroCopyFields", "boolean");
        buffer.printf("%n");
        generateToString();
        buffer.printf("%n");
        generateHeaderVersion("request");
        buffer.printf("%n");
        generateHeaderVersion("response");
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();
    }

    private void generateEnumValues() {
        int numProcessed = 0;
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            ApiData apiData = entry.getValue();
            String name = apiData.name();
            numProcessed++;
            buffer.printf("%s(\"%s\", (short) %d, %s, %s, %s, %s)%s%n",
                MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT),
                MessageGenerator.capitalizeFirst(name),
                entry.getKey(),
                apiData.requestSchema(),
                apiData.responseSchema(),
                Boolean.toString(apiData.requestContainsZeroCopyFields),
                Boolean.toString(apiData.responseContainsZeroCopyFields),
                (numProcessed == apis.size()) ? ";" : ",");
        }
    }

    private void generateInstanceVariables() {
        buffer.printf("private final String name;%n");
        buffer.printf("private final short apiKey;%n");
        buffer.printf("private final Schema[] requestSchemas;%n");
        buffer.printf("private final Schema[] responseSchemas;%n");
        buffer.printf("private final boolean requestContainsZeroCopyFields;%n");
        buffer.printf("private final boolean responseContainsZeroCopyFields;%n");
        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS);
    }

    private void generateEnumConstructor() {
        buffer.printf("ApiMessageType(String name, short apiKey, " +
            "Schema[] requestSchemas, Schema[] responseSchemas, " +
            "boolean requestContainsZeroCopyFields, boolean responseContainsZeroCopyFields) {%n");
        buffer.incrementIndent();
        buffer.printf("this.name = name;%n");
        buffer.printf("this.apiKey = apiKey;%n");
        buffer.printf("this.requestSchemas = requestSchemas;%n");
        buffer.printf("this.responseSchemas = responseSchemas;%n");
        buffer.printf("this.requestContainsZeroCopyFields = requestContainsZeroCopyFields;%n");
        buffer.printf("this.responseContainsZeroCopyFields = responseContainsZeroCopyFields;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFromApiKey() {
        buffer.printf("public static ApiMessageType fromApiKey(short apiKey) {%n");
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            ApiData apiData = entry.getValue();
            String name = apiData.name();
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return %s;%n", MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT));
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unsupported API key \"" +
            " + apiKey);%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateNewApiMessageMethod(String type) {
        headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);
        buffer.printf("public ApiMessage new%s() {%n",
            MessageGenerator.capitalizeFirst(type));
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            buffer.printf("case %d:%n", entry.getKey());
            buffer.incrementIndent();
            buffer.printf("return new %s%sData();%n",
                entry.getValue().name(),
                MessageGenerator.capitalizeFirst(type));
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

    private void generateHeaderVersion(String type) {
        buffer.printf("public short %sHeaderVersion(short _version) {%n", type);
        buffer.incrementIndent();
        buffer.printf("switch (apiKey) {%n");
        buffer.incrementIndent();
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            short apiKey = entry.getKey();
            buffer.printf("case %d:%n", apiKey);
            buffer.incrementIndent();
            if (type.equals("response") && apiKey == 18) {
                // ApiVersionsResponse always includes a v0 header.
                // See KIP-511 for details.
                buffer.printf("return (short) 0;%n");
                buffer.decrementIndent();
                continue;
            }
            if (type.equals("request") && apiKey == 7) {
                // Version 0 of ControlledShutdownRequest has a non-standard request header
                // which does not include clientId.  Version 1 of ControlledShutdownRequest
                // and later use the standard request header.
                buffer.printf("if (_version == 0) {%n");
                buffer.incrementIndent();
                buffer.printf("return (short) 0;%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            ApiData data = entry.getValue();
            MessageSpec spec = null;
            if (type.equals("request")) {
                spec = data.requestSpec;
            } else if (type.equals("response")) {
                spec = data.responseSpec;
            } else {
                throw new RuntimeException("Invalid type " + type + " for generateHeaderVersion");
            }
            if (spec == null) {
                throw new RuntimeException("failed to find " + type + " for API key " + apiKey);
            }
            VersionConditional.forVersions(spec.flexibleVersions(),
                spec.validVersions()).
                ifMember(__ -> {
                    if (type.equals("request")) {
                        buffer.printf("return (short) 2;%n");
                    } else {
                        buffer.printf("return (short) 1;%n");
                    }
                }).
                ifNotMember(__ -> {
                    if (type.equals("request")) {
                        buffer.printf("return (short) 1;%n");
                    } else {
                        buffer.printf("return (short) 0;%n");
                    }
                }).generate(buffer);
            buffer.decrementIndent();
        }
        buffer.printf("default:%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Unsupported API key \"" +
            " + apiKey);%n");
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
