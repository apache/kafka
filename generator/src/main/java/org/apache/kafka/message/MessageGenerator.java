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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.BufferedWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;

/**
 * The Kafka message generator.
 */
public final class MessageGenerator {
    static final String JSON_SUFFIX = ".json";

    static final String JSON_GLOB = "*" + JSON_SUFFIX;

    static final String JAVA_SUFFIX = ".java";

    static final String API_MESSAGE_TYPE_JAVA = "ApiMessageType.java";

    static final String API_MESSAGE_CLASS = "org.apache.kafka.common.protocol.ApiMessage";

    static final String MESSAGE_CLASS = "org.apache.kafka.common.protocol.Message";

    static final String MESSAGE_UTIL_CLASS = "org.apache.kafka.common.protocol.MessageUtil";

    static final String READABLE_CLASS = "org.apache.kafka.common.protocol.Readable";

    static final String WRITABLE_CLASS = "org.apache.kafka.common.protocol.Writable";

    static final String ARRAYS_CLASS = "java.util.Arrays";

    static final String OBJECTS_CLASS = "java.util.Objects";

    static final String LIST_CLASS = "java.util.List";

    static final String ARRAYLIST_CLASS = "java.util.ArrayList";

    static final String IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS =
        "org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection";

    static final String UNSUPPORTED_VERSION_EXCEPTION_CLASS =
        "org.apache.kafka.common.errors.UnsupportedVersionException";

    static final String ITERATOR_CLASS = "java.util.Iterator";

    static final String TYPE_CLASS = "org.apache.kafka.common.protocol.types.Type";

    static final String FIELD_CLASS = "org.apache.kafka.common.protocol.types.Field";

    static final String SCHEMA_CLASS = "org.apache.kafka.common.protocol.types.Schema";

    static final String ARRAYOF_CLASS = "org.apache.kafka.common.protocol.types.ArrayOf";

    static final String COMPACT_ARRAYOF_CLASS = "org.apache.kafka.common.protocol.types.CompactArrayOf";

    static final String STRUCT_CLASS = "org.apache.kafka.common.protocol.types.Struct";

    static final String BYTES_CLASS = "org.apache.kafka.common.utils.Bytes";

    static final String UUID_CLASS = "java.util.UUID";

    static final String REQUEST_SUFFIX = "Request";

    static final String RESPONSE_SUFFIX = "Response";

    static final String BYTE_UTILS_CLASS = "org.apache.kafka.common.utils.ByteUtils";

    static final String STANDARD_CHARSETS = "java.nio.charset.StandardCharsets";

    static final String TAGGED_FIELDS_SECTION_CLASS = "org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection";

    static final String OBJECT_SERIALIZATION_CACHE_CLASS = "org.apache.kafka.common.protocol.ObjectSerializationCache";

    static final String RAW_TAGGED_FIELD_CLASS = "org.apache.kafka.common.protocol.types.RawTaggedField";

    static final String RAW_TAGGED_FIELD_WRITER_CLASS = "org.apache.kafka.common.protocol.types.RawTaggedFieldWriter";

    static final String TREE_MAP_CLASS = "java.util.TreeMap";

    static final String BYTE_BUFFER_CLASS = "java.nio.ByteBuffer";

    static final String NAVIGABLE_MAP_CLASS = "java.util.NavigableMap";

    static final String MAP_ENTRY_CLASS = "java.util.Map.Entry";

    /**
     * The Jackson serializer we use for JSON objects.
     */
    static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    public static void processDirectories(String packageName, String outputDir, String inputDir) throws Exception {
        Files.createDirectories(Paths.get(outputDir));
        int numProcessed = 0;
        ApiMessageTypeGenerator messageTypeGenerator = new ApiMessageTypeGenerator(packageName);
        HashSet<String> outputFileNames = new HashSet<>();
        try (DirectoryStream<Path> directoryStream = Files
                .newDirectoryStream(Paths.get(inputDir), JSON_GLOB)) {
            for (Path inputPath : directoryStream) {
                try {
                    MessageSpec spec = JSON_SERDE.
                        readValue(inputPath.toFile(), MessageSpec.class);
                    String javaName = spec.generatedClassName() + JAVA_SUFFIX;
                    outputFileNames.add(javaName);
                    Path outputPath = Paths.get(outputDir, javaName);
                    try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
                        MessageDataGenerator generator = new MessageDataGenerator(packageName);
                        generator.generate(spec);
                        generator.write(writer);
                    }
                    numProcessed++;
                    messageTypeGenerator.registerMessageType(spec);
                } catch (Exception e) {
                    throw new RuntimeException("Exception while processing " + inputPath.toString(), e);
                }
            }
        }
        if (messageTypeGenerator.hasRegisteredTypes()) {
            Path factoryOutputPath = Paths.get(outputDir, API_MESSAGE_TYPE_JAVA);
            outputFileNames.add(API_MESSAGE_TYPE_JAVA);
            try (BufferedWriter writer = Files.newBufferedWriter(factoryOutputPath)) {
                messageTypeGenerator.generate();
                messageTypeGenerator.write(writer);
            }
            numProcessed++;
        }
        try (DirectoryStream<Path> directoryStream = Files.
                newDirectoryStream(Paths.get(outputDir))) {
            for (Path outputPath : directoryStream) {
                Path fileName = outputPath.getFileName();
                if (fileName != null) {
                    if (!outputFileNames.contains(fileName.toString())) {
                        Files.delete(outputPath);
                    }
                }
            }
        }
        System.out.printf("MessageGenerator: processed %d Kafka message JSON files(s).%n", numProcessed);
    }

    static String capitalizeFirst(String string) {
        if (string.isEmpty()) {
            return string;
        }
        return string.substring(0, 1).toUpperCase(Locale.ENGLISH) +
            string.substring(1);
    }

    static String lowerCaseFirst(String string) {
        if (string.isEmpty()) {
            return string;
        }
        return string.substring(0, 1).toLowerCase(Locale.ENGLISH) +
            string.substring(1);
    }

    static boolean firstIsCapitalized(String string) {
        if (string.isEmpty()) {
            return false;
        }
        return Character.isUpperCase(string.charAt(0));
    }

    static String toSnakeCase(String string) {
        StringBuilder bld = new StringBuilder();
        boolean prevWasCapitalized = true;
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isUpperCase(c)) {
                if (!prevWasCapitalized) {
                    bld.append('_');
                }
                bld.append(Character.toLowerCase(c));
                prevWasCapitalized = true;
            } else {
                bld.append(c);
                prevWasCapitalized = false;
            }
        }
        return bld.toString();
    }

    static String stripSuffix(String str, String suffix) {
        if (str.endsWith(suffix)) {
            return str.substring(0, str.length() - suffix.length());
        } else {
            throw new RuntimeException("String " + str + " does not end with the " +
                "expected suffix " + suffix);
        }
    }

    /**
     * Return the number of bytes needed to encode an integer in unsigned variable-length format.
     */
    static int sizeOfUnsignedVarint(int value) {
        int bytes = 1;
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes;
    }

    private final static String USAGE = "MessageGenerator: [output Java package] [output Java file] [input JSON file]";

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println(USAGE);
            System.exit(0);
        } else if (args.length != 3) {
            System.out.println(USAGE);
            System.exit(1);
        }
        processDirectories(args[0], args[1], args[2]);
    }
}
