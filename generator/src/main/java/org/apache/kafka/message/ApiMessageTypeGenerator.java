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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class ApiMessageTypeGenerator implements TypeClassGenerator {
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;
    private final TreeMap<Short, ApiData> apis;
    private final EnumMap<RequestListenerType, List<ApiData>> apisByListener = new EnumMap<>(RequestListenerType.class);

    private static final class ApiData {
        short apiKey;
        MessageSpec requestSpec;
        MessageSpec responseSpec;

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

    @Override
    public String outputName() {
        return MessageGenerator.API_MESSAGE_TYPE_JAVA;
    }

    @Override
    public void registerMessageType(MessageSpec spec) {
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

                if (spec.listeners() != null) {
                    for (RequestListenerType listener : spec.listeners()) {
                        apisByListener.putIfAbsent(listener, new ArrayList<>());
                        apisByListener.get(listener).add(data);
                    }
                }
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
                break;
            }
            default:
                // do nothing
                break;
        }
    }

    @Override
    public void generateAndWrite(BufferedWriter writer) throws IOException {
        generate();
        write(writer);
    }

    private void generate() {
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
        generateAccessor("lowestSupportedVersion", "short");
        buffer.printf("%n");
        generateHighestSupportedVersion();
        buffer.printf("%n");
        generateAccessor("lowestDeprecatedVersion", "short");
        buffer.printf("%n");
        generateAccessor("highestDeprecatedVersion", "short");
        buffer.printf("%n");
        generateAccessor("listeners", "EnumSet<ListenerType>");
        buffer.printf("%n");
        generateAccessor("latestVersionUnstable", "boolean");
        buffer.printf("%n");
        generateAccessor("apiKey", "short");
        buffer.printf("%n");
        generateAccessor("requestSchemas", "Schema[]");
        buffer.printf("%n");
        generateAccessor("responseSchemas", "Schema[]");
        buffer.printf("%n");
        generateToString();
        buffer.printf("%n");
        generateHeaderVersion("request");
        buffer.printf("%n");
        generateHeaderVersion("response");
        buffer.printf("%n");
        generateListenerTypesEnum();
        buffer.printf("%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();
    }

    private String generateListenerTypeEnumSet(Collection<String> values) {
        if (values.isEmpty()) {
            return "EnumSet.noneOf(ListenerType.class)";
        }
        StringBuilder bldr = new StringBuilder("EnumSet.of(");
        Iterator<String> iter = values.iterator();
        while (iter.hasNext()) {
            bldr.append("ListenerType.");
            bldr.append(iter.next());
            if (iter.hasNext()) {
                bldr.append(", ");
            }
        }
        bldr.append(")");
        return bldr.toString();
    }

    private void generateEnumValues() {
        int numProcessed = 0;
        for (Map.Entry<Short, ApiData> entry : apis.entrySet()) {
            ApiData apiData = entry.getValue();
            String name = apiData.name();
            numProcessed++;

            final Collection<String> listeners;
            if (apiData.requestSpec.listeners() == null) {
                listeners = Collections.emptyList();
            } else {
                listeners = apiData.requestSpec.listeners().stream()
                    .map(RequestListenerType::name)
                    .collect(Collectors.toList());
            }

            buffer.printf("%s(\"%s\", (short) %d, %s, %s, (short) %d, (short) %d, (short) %d, (short) %d, %s, %s)%s%n",
                MessageGenerator.toSnakeCase(name).toUpperCase(Locale.ROOT),
                MessageGenerator.capitalizeFirst(name),
                entry.getKey(),
                apiData.requestSchema(),
                apiData.responseSchema(),
                apiData.requestSpec.struct().versions().lowest(),
                apiData.requestSpec.struct().versions().highest(),
                apiData.requestSpec.struct().deprecatedVersions().lowest(),
                apiData.requestSpec.struct().deprecatedVersions().highest(),
                generateListenerTypeEnumSet(listeners),
                apiData.requestSpec.latestVersionUnstable(),
                (numProcessed == apis.size()) ? ";" : ",");
        }
    }

    private void generateInstanceVariables() {
        buffer.printf("public final String name;%n");
        buffer.printf("private final short apiKey;%n");
        buffer.printf("private final Schema[] requestSchemas;%n");
        buffer.printf("private final Schema[] responseSchemas;%n");
        buffer.printf("private final short lowestSupportedVersion;%n");
        buffer.printf("private final short highestSupportedVersion;%n");
        buffer.printf("private final short lowestDeprecatedVersion;%n");
        buffer.printf("private final short highestDeprecatedVersion;%n");
        buffer.printf("private final EnumSet<ListenerType> listeners;%n");
        buffer.printf("private final boolean latestVersionUnstable;%n");
        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS);
        headerGenerator.addImport(MessageGenerator.ENUM_SET_CLASS);
    }

    private void generateEnumConstructor() {
        buffer.printf("ApiMessageType(String name, short apiKey, " +
            "Schema[] requestSchemas, Schema[] responseSchemas, " +
            "short lowestSupportedVersion, short highestSupportedVersion, " +
            "short lowestDeprecatedVersion, short highestDeprecatedVersion, " +
            "EnumSet<ListenerType> listeners, boolean latestVersionUnstable) {%n");
        buffer.incrementIndent();
        buffer.printf("this.name = name;%n");
        buffer.printf("this.apiKey = apiKey;%n");
        buffer.printf("this.requestSchemas = requestSchemas;%n");
        buffer.printf("this.responseSchemas = responseSchemas;%n");
        buffer.printf("this.lowestSupportedVersion = lowestSupportedVersion;%n");
        buffer.printf("this.highestSupportedVersion = highestSupportedVersion;%n");
        buffer.printf("this.lowestDeprecatedVersion = lowestDeprecatedVersion;%n");
        buffer.printf("this.highestDeprecatedVersion = highestDeprecatedVersion;%n");
        buffer.printf("this.listeners = listeners;%n");
        buffer.printf("this.latestVersionUnstable = latestVersionUnstable;%n");
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
            ApiData apiData = entry.getValue();
            String name = apiData.name();
            buffer.printf("case %d: // %s%n", apiKey, MessageGenerator.capitalizeFirst(name));
            buffer.incrementIndent();
            if (type.equals("response") && apiKey == 18) {
                buffer.printf("// ApiVersionsResponse always includes a v0 header.%n");
                buffer.printf("// See KIP-511 for details.%n");
                buffer.printf("return (short) 0;%n");
                buffer.decrementIndent();
                continue;
            }
            if (type.equals("request") && apiKey == 7) {
                buffer.printf("// Version 0 of ControlledShutdownRequest has a non-standard request header%n");
                buffer.printf("// which does not include clientId.  Version 1 of ControlledShutdownRequest%n");
                buffer.printf("// and later use the standard request header.%n");
                buffer.printf("if (_version == 0) {%n");
                buffer.incrementIndent();
                buffer.printf("return (short) 0;%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            ApiData data = entry.getValue();
            MessageSpec spec;
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

    private void generateListenerTypesEnum() {
        buffer.printf("public enum ListenerType {%n");
        buffer.incrementIndent();
        Iterator<RequestListenerType> listenerIter = Arrays.stream(RequestListenerType.values()).iterator();
        while (listenerIter.hasNext()) {
            RequestListenerType scope = listenerIter.next();
            buffer.printf("%s%s%n", scope.name(), listenerIter.hasNext() ? "," : ";");
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHighestSupportedVersion() {
        buffer.printf("public short highestSupportedVersion(boolean enableUnstableLastVersion) {%n");
        buffer.incrementIndent();
        buffer.printf("if (!this.latestVersionUnstable || enableUnstableLastVersion) {%n");
        buffer.incrementIndent();
        buffer.printf("return this.highestSupportedVersion;%n");
        buffer.decrementIndent();
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        buffer.printf("// A negative value means that the API has no enabled versions.%n");
        buffer.printf("return (short) (this.highestSupportedVersion - 1);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void write(BufferedWriter writer) throws IOException {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }
}
