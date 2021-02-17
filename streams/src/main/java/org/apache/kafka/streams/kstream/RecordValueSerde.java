package org.apache.kafka.streams.kstream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;

public class RecordValueSerde<V> implements Serde<RecordValue<V>> {

    final Serde<V> valueSerde;

    public RecordValueSerde(Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
    }

    @Override
    public Serializer<RecordValue<V>> serializer() {
        return new RecordValueSerializer<>(valueSerde.serializer());
    }

    @Override
    public Deserializer<RecordValue<V>> deserializer() {
        return new RecordValueDeserializer<>(valueSerde.deserializer());
    }

    static class RecordValueSerializer<V> implements Serializer<RecordValue<V>> {

        final Serializer<V> valueSerializer;

        RecordValueSerializer(Serializer<V> valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        @Override
        public byte[] serialize(String topic, RecordValue<V> data) {
            try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                try (final DataOutputStream buffer = new DataOutputStream(outputStream)) {
                    // write value
                    final byte[] valueBytes = valueSerializer.serialize(topic, data.value());
                    ByteUtils.writeVarint(valueBytes.length, buffer);
                    buffer.write(valueBytes);

                    // write record metadata: topic-partition-offset
                    final byte[] topicBytes = Utils.utf8(data.topic());
                    ByteUtils.writeVarint(topicBytes.length, buffer);
                    buffer.write(topicBytes);
                    ByteUtils.writeVarint(data.partition(), buffer);
                    ByteUtils.writeVarlong(data.offset(), buffer);
                    ByteUtils.writeVarlong(data.timestamp(), buffer);

                    // write headers
                    final Header[] headers = data.headers().unwrap().toArray();
                    ByteUtils.writeVarint(headers.length, buffer);
                    for (final Header header : headers) {
                        final String headerKey = header.key();
                        if (headerKey == null) {
                            throw new IllegalArgumentException(
                                "Invalid null header key found in headers");
                        }

                        // write record key
                        final byte[] utf8Bytes = Utils.utf8(headerKey);
                        ByteUtils.writeVarint(utf8Bytes.length, buffer);
                        buffer.write(utf8Bytes);

                        // write record value
                        final byte[] headerValue = header.value();
                        if (headerValue == null) {
                            ByteUtils.writeVarint(-1, buffer);
                        } else {
                            ByteUtils.writeVarint(headerValue.length, buffer);
                            buffer.write(headerValue);
                        }
                    }

                    return outputStream.toByteArray();
                }
            } catch (IOException e) {
                throw new RuntimeException(e); //TODO check what to do here
            }
        }
    }

    static class RecordValueDeserializer<V> implements Deserializer<RecordValue<V>> {

        final Deserializer<V> valueDeserializer;

        RecordValueDeserializer(
            Deserializer<V> valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        @Override
        public RecordValue<V> deserialize(String t, byte[] data) {
            final ByteBuffer buffer = ByteBuffer.wrap(data);

            // read value
            final int valueSize = ByteUtils.readVarint(buffer);
            if (valueSize < 0) {
                throw new IllegalArgumentException("");//TODO
            }
            final ByteBuffer valueBuffer = buffer.slice();
            valueBuffer.limit(valueSize);
            buffer.position(buffer.position() + valueSize);
            final V value = valueDeserializer.deserialize(t, valueBuffer.array());

            // read record metadata
            final int topicSize = ByteUtils.readVarint(buffer);
            if (topicSize < 0) {
                throw new IllegalArgumentException("");//TODO
            }
            final ByteBuffer topicBuffer = buffer.slice();
            topicBuffer.limit(topicSize);
            buffer.position(buffer.position() + topicSize);
            final String topic = Utils.utf8(topicBuffer);
            final int partition = ByteUtils.readVarint(buffer);
            final long offset = ByteUtils.readVarlong(buffer);
            final long timestamp = ByteUtils.readVarlong(buffer);

            // read headers
            final int numHeaders = ByteUtils.readVarint(buffer);
            if (numHeaders < 0) {
                throw new InvalidRecordException(
                    "Found invalid number of record headers " + numHeaders);
            }
            final Header[] headers;
            if (numHeaders == 0) {
                headers = Record.EMPTY_HEADERS;
            } else {
                headers = readHeaders(buffer, numHeaders);
            }

            return new RecordValue<>(topic, partition, offset, value, timestamp, headers);
        }

        private Header[] readHeaders(final ByteBuffer buffer, final int numHeaders) {
            final Header[] headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                final int headerKeySize = ByteUtils.readVarint(buffer);
                if (headerKeySize < 0) {
                    throw new InvalidRecordException(
                        "Invalid negative header key size " + headerKeySize);
                }

                final ByteBuffer headerKey = buffer.slice();
                headerKey.limit(headerKeySize);
                buffer.position(buffer.position() + headerKeySize);

                ByteBuffer headerValue = null;
                final int headerValueSize = ByteUtils.readVarint(buffer);
                if (headerValueSize >= 0) {
                    headerValue = buffer.slice();
                    headerValue.limit(headerValueSize);
                    buffer.position(buffer.position() + headerValueSize);
                }

                headers[i] = new RecordHeader(headerKey, headerValue);
            }

            return headers;
        }
    }
}
