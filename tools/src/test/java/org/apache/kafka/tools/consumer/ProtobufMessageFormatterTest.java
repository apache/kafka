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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProtobufMessageFormatterTest {

    private static final String MESSAGE_TYPE = "com.example.MyEvent";
    private static final String PROTO_SOURCE =
            "syntax = \"proto3\";\n"
                    + "package com.example;\n"
                    + "\n"
                    + "message MyEvent {\n"
                    + "  int32 id = 1;\n"
                    + "  string name = 2;\n"
                    + "}\n";

    @Test
    public void testHappyPath(@TempDir Path tempDir) throws Exception {
        // Given: a proto.dir containing com.example.MyEvent { int32 id = 1; string name = 2; }
        Path protoDir = writeProtoDir(tempDir);
        Descriptors.Descriptor descriptor = buildDescriptor();

        // and a real Protobuf-encoded record matching that schema
        DynamicMessage sourceMessage = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("id"), 42)
                .setField(descriptor.findFieldByName("name"), "alice")
                .build();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "topic", 0, 0, null, sourceMessage.toByteArray());

        // When
        MessageFormatter formatter = configuredFormatter(protoDir);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));

        // Then
        String expected = "{\n  \"id\": 42,\n  \"name\": \"alice\"\n}\n";
        assertEquals(expected, out.toString());
    }

    @Test
    public void testConfigureMissingProtoDirThrows() {
        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("message.type", MESSAGE_TYPE);

        assertThrows(ConfigException.class, () -> formatter.configure(configs));
    }

    @Test
    public void testConfigureMissingMessageTypeThrows(@TempDir Path tempDir) throws Exception {
        Path protoDir = writeProtoDir(tempDir);

        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("proto.dir", protoDir.toString());

        assertThrows(ConfigException.class, () -> formatter.configure(configs));
    }

    @Test
    public void testConfigureProtoDirNotFoundThrows(@TempDir Path tempDir) {
        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("proto.dir", tempDir.resolve("does-not-exist").toString());
        configs.put("message.type", MESSAGE_TYPE);

        assertThrows(ConfigException.class, () -> formatter.configure(configs));
    }

    @Test
    public void testConfigureNoProtoFilesFoundThrows(@TempDir Path tempDir) throws Exception {
        Path emptyDir = Files.createDirectory(tempDir.resolve("empty-protos"));

        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("proto.dir", emptyDir.toString());
        configs.put("message.type", MESSAGE_TYPE);

        assertThrows(ConfigException.class, () -> formatter.configure(configs));
    }

    @Test
    public void testConfigureInvalidProtoSyntaxThrows(@TempDir Path tempDir) throws Exception {
        Path protoDir = Files.createDirectory(tempDir.resolve("protos"));
        Files.writeString(protoDir.resolve("broken.proto"), "this is not valid proto syntax {{{");

        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("proto.dir", protoDir.toString());
        configs.put("message.type", MESSAGE_TYPE);

        assertThrows(ConfigException.class, () -> formatter.configure(configs));
    }

    @Test
    public void testConfigureMessageTypeNotFoundThrows(@TempDir Path tempDir) throws Exception {
        Path protoDir = writeProtoDir(tempDir);

        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("proto.dir", protoDir.toString());
        configs.put("message.type", "com.example.DoesNotExist");

        assertThrows(ConfigException.class, () -> formatter.configure(configs));
    }

    @Test
    public void testWriteToMalformedBytesThrows(@TempDir Path tempDir) throws Exception {
        Path protoDir = writeProtoDir(tempDir);
        MessageFormatter formatter = configuredFormatter(protoDir);

        // A single 0xFF byte is an incomplete varint - not valid Protobuf for any message.
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "topic", 0, 0, null, new byte[] {(byte) 0xFF});

        assertThrows(SerializationException.class,
                () -> formatter.writeTo(record, new PrintStream(new ByteArrayOutputStream())));
    }

    @Test
    public void testWriteToNullValuePrintsNullLiteral(@TempDir Path tempDir) throws Exception {
        Path protoDir = writeProtoDir(tempDir);
        MessageFormatter formatter = configuredFormatter(protoDir);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "topic", 0, 0, null, null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));

        assertEquals("null\n", out.toString());
    }

    @Test
    public void testWriteToEmptyValuePrintsEmptyMessage(@TempDir Path tempDir) throws Exception {
        // An empty (non-null) byte[] is a valid Protobuf message with every field at its
        // proto3 default - distinct from a null/tombstone value.
        Path protoDir = writeProtoDir(tempDir);
        MessageFormatter formatter = configuredFormatter(protoDir);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "topic", 0, 0, null, new byte[0]);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        formatter.writeTo(record, new PrintStream(out));

        assertEquals("{\n}\n", out.toString());
    }

    private static MessageFormatter configuredFormatter(Path protoDir) {
        MessageFormatter formatter = new ProtobufMessageFormatter();
        Map<String, String> configs = new HashMap<>();
        configs.put("proto.dir", protoDir.toString());
        configs.put("message.type", MESSAGE_TYPE);
        formatter.configure(configs);
        return formatter;
    }

    static Path writeProtoDir(Path tempDir) throws Exception {
        Path protoDir = Files.createDirectory(tempDir.resolve("protos"));
        Files.writeString(protoDir.resolve("myevent.proto"), PROTO_SOURCE);
        return protoDir;
    }

    private static Descriptors.Descriptor buildDescriptor() throws Descriptors.DescriptorValidationException {
        FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                .setName("myevent.proto")
                .setSyntax("proto3")
                .setPackage("com.example")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("MyEvent")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("id")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("name")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();

        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
        return fd.findMessageTypeByName("MyEvent");
    }
}
