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
package org.apache.kafka.connect.runtime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * This is a simple Converter implementation that uses header values for serialization / deserialization purposes.
 *
 * It expects "message.type" header to be set with "MessageTypeA" or "MessageTypeB" value indicating target class for serialization / deserialization.
 *
 * To avoid using any dependencies like Avro or Protobuf it uses Java serialization.
 */
public class TestConverterWithHeaders implements Converter {
    private static final String HEADER_MESSAGE_TYPE = "message.type";

    public static class MessageTypeA implements Serializable {
        Integer value1;
        String value2;

        public MessageTypeA(Integer value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    public static class MessageTypeB implements Serializable {
        Boolean value1;
        Long value2;

        public MessageTypeB(Boolean value1, Long value2) {
            this.value1 = value1;
            this.value2 = value2;
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        String messageType = extractMessageType(headers);

        if (messageType.equals("MessageTypeA")) {
            MessageTypeA message = deserialize(value);

            Schema schema =  SchemaBuilder.struct()
                .field("value1", Schema.INT32_SCHEMA)
                .field("value2", Schema.STRING_SCHEMA)
                .build();

            Struct object = new Struct(schema);
            object.put("value1", message.value1);
            object.put("value2", message.value2);

            return new SchemaAndValue(schema, object);
        } else if (messageType.equals("MessageTypeB")) {
            MessageTypeB message = deserialize(value);

            Schema schema =  SchemaBuilder.struct()
                .field("value1", Schema.BOOLEAN_SCHEMA)
                .field("value2", Schema.INT64_SCHEMA)
                .build();

            Struct object = new Struct(schema);
            object.put("value1", message.value1);
            object.put("value2", message.value2);

            return new SchemaAndValue(schema, object);
        } else {
            throw new DataException("Unsupported type: " + messageType);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        String messageType = extractMessageType(headers);

        if (messageType.equals("MessageTypeA")) {
            Struct struct = (Struct) value;

            MessageTypeA message = new MessageTypeA(
                struct.getInt32("value1"),
                struct.getString("value2")
            );

            return serialize(message);
        } else if (messageType.equals("MessageTypeB")) {
            Struct struct = (Struct) value;

            MessageTypeB message = new MessageTypeB(
                struct.getBoolean("value1"),
                struct.getInt64("value2")
            );

            return serialize(message);
        } else {
            throw new DataException("Unsupported type: " + messageType);
        }
    }

    private String extractMessageType(Headers headers) {
        Header header = headers.lastHeader(HEADER_MESSAGE_TYPE);
        if (header == null) {
            throw new DataException("Header '" + HEADER_MESSAGE_TYPE + "' is required!");
        }

        return new String(header.value());
    }

    static <T> T deserialize(final byte[] objectData) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(objectData))) {
            @SuppressWarnings("unchecked")
            final T obj = (T) in.readObject();
            return obj;
        } catch (final ClassNotFoundException | IOException ex) {
            throw new SerializationException(ex);
        }
    }

    static byte[] serialize(final Serializable obj) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);

        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(obj);
        } catch (final IOException ex) {
            throw new SerializationException(ex);
        }

        return baos.toByteArray();
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        throw new DataException("Headers are required for this converter!");
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        throw new DataException("Headers are required for this converter!");
    }
}
