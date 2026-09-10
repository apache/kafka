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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ProtobufMessageFormatter implements MessageFormatter {

    private Descriptors.Descriptor messageDescriptor;
    private JsonFormat.Printer jsonPrinter;

    @Override
    public void configure(Map<String, ?> configs) {
        String protoDir = (String) configs.get("proto.dir");
        String messageType = (String) configs.get("message.type");
        if (protoDir == null) {
            throw new ConfigException("Missing required formatter property: proto.dir");
        }
        if (messageType == null) {
            throw new ConfigException("Missing required formatter property: message.type");
        }

        Path protoDirPath = Paths.get(protoDir);
        if (!Files.isDirectory(protoDirPath)) {
            throw new ConfigException("proto.dir is not a directory: " + protoDir);
        }

        FileDescriptorSet descriptorSet = compileProtoDir(protoDirPath);

        Map<String, Descriptors.FileDescriptor> builtFiles = new HashMap<>();
        for (FileDescriptorProto proto : descriptorSet.getFileList()) {
            Descriptors.FileDescriptor[] deps = proto.getDependencyList().stream()
                    .map(builtFiles::get)
                    .toArray(Descriptors.FileDescriptor[]::new);
            try {
                Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(proto, deps);
                builtFiles.put(proto.getName(), fd);
            } catch (Descriptors.DescriptorValidationException e) {
                throw new ConfigException("Invalid .proto definitions in proto.dir: " + protoDir, e);
            }
        }

        Descriptors.Descriptor found = null;
        for (Descriptors.FileDescriptor fd : builtFiles.values()) {
            found = findMessageType(fd.getMessageTypes(), messageType);
            if (found != null) {
                break;
            }
        }
        if (found == null) {
            throw new ConfigException("message.type not found under proto.dir: " + messageType);
        }

        this.messageDescriptor = found;
        this.jsonPrinter = JsonFormat.printer();
    }

    private static FileDescriptorSet compileProtoDir(Path protoDirPath) {
        List<String> protoFiles;
        try (Stream<Path> paths = Files.walk(protoDirPath)) {
            protoFiles = paths.filter(p -> p.toString().endsWith(".proto"))
                    .map(p -> protoDirPath.relativize(p).toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new ConfigException("Unable to scan proto.dir: " + protoDirPath, e);
        }
        if (protoFiles.isEmpty()) {
            throw new ConfigException("No .proto files found in proto.dir: " + protoDirPath);
        }

        Path descriptorSetFile;
        try {
            descriptorSetFile = Files.createTempFile("kafka-protobuf-formatter-", ".desc");
            descriptorSetFile.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new ConfigException("Unable to create a temporary file for protoc output", e);
        }

        List<String> command = new ArrayList<>();
        command.add("protoc");
        command.add("--proto_path=" + protoDirPath);
        command.add("--descriptor_set_out=" + descriptorSetFile);
        command.add("--include_imports");
        command.addAll(protoFiles);

        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output;
            try (InputStream processOutput = process.getInputStream()) {
                output = new String(processOutput.readAllBytes(), StandardCharsets.UTF_8);
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new ConfigException("protoc failed to compile proto.dir (" + protoDirPath + "):\n" + output);
            }
        } catch (IOException e) {
            throw new ConfigException("Unable to run protoc - is it installed and on PATH?", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConfigException("Interrupted while running protoc", e);
        }

        try (InputStream in = Files.newInputStream(descriptorSetFile)) {
            return FileDescriptorSet.parseFrom(in);
        } catch (IOException e) {
            throw new ConfigException("Unable to read descriptor set produced by protoc for proto.dir: " + protoDirPath, e);
        } finally {
            try {
                Files.deleteIfExists(descriptorSetFile);
            } catch (IOException e) {
                // Best-effort cleanup; the file is also marked deleteOnExit above.
            }
        }
    }

    private static Descriptors.Descriptor findMessageType(List<Descriptors.Descriptor> candidates, String fullName) {
        for (Descriptors.Descriptor d : candidates) {
            if (d.getFullName().equals(fullName)) {
                return d;
            }
            Descriptors.Descriptor nested = findMessageType(d.getNestedTypes(), fullName);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> record, PrintStream output) {
        if (record.value() == null) {
            output.println("null");
            return;
        }
        try {
            DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor, record.value());
            output.println(jsonPrinter.print(message));
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("Failed to parse Protobuf record", e);
        }
    }
}
