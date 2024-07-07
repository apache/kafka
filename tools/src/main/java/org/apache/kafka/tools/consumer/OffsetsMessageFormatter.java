package org.apache.kafka.tools.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValueJsonConverter;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;
import static org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP;

public class OffsetsMessageFormatter implements MessageFormatter {

    private static final String VERSION = "version";
    private static final String DATA = "data";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String NULL = "NULL";

    @Override
    public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
        ObjectNode json = new ObjectNode(JsonNodeFactory.instance);

        byte[] key = consumerRecord.key();
        if (Objects.nonNull(key)) {
            short keyVersion = ByteBuffer.wrap(key).getShort();
            Optional<OffsetCommitKey> offsetCommitKey = readToGroupMetadataKey(ByteBuffer.wrap(key));
            settingKeyNode(json, offsetCommitKey, keyVersion);
        } else {
            json.put(KEY, NULL);
        }

        byte[] value = consumerRecord.value();
        if (Objects.nonNull(value)) {
            short valueVersion = ByteBuffer.wrap(value).getShort();
            Optional<OffsetCommitValue> offsetCommitValue = readToOffsetCommitValue(ByteBuffer.wrap(value));
            settingValueNode(json, offsetCommitValue, valueVersion);
        } else {
            json.put(VALUE, NULL);
        }

        try {
            output.write(json.toString().getBytes(UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Optional<OffsetCommitKey> readToGroupMetadataKey(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= OffsetCommitKey.LOWEST_SUPPORTED_VERSION
                && version <= OffsetCommitKey.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new OffsetCommitKey(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }

    private Optional<OffsetCommitValue> readToOffsetCommitValue(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= OffsetCommitValue.LOWEST_SUPPORTED_VERSION
                && version <= OffsetCommitValue.HIGHEST_SUPPORTED_VERSION) {
            OffsetCommitValue value = new OffsetCommitValue(new ByteBufferAccessor(byteBuffer), version);
            value.setLeaderEpoch(value.leaderEpoch() == NO_PARTITION_LEADER_EPOCH ? 0: value.leaderEpoch());
            value.setExpireTimestamp(value.expireTimestamp() == DEFAULT_TIMESTAMP ? 0L : value.expireTimestamp());
            return Optional.of(value);
        } else {
            throw new IllegalStateException("Unknown offset message version: " + version);
        }
    }

    private void settingKeyNode(ObjectNode json, Optional<OffsetCommitKey> transactionLogKey, short keyVersion) {
        if (transactionLogKey.isPresent()) {
            addDataNode(json, KEY, keyVersion,
                    OffsetCommitKeyJsonConverter.write(transactionLogKey.get(), keyVersion));
        } else {
            addUnknownNode(json, KEY, keyVersion);
        }
    }

    private void settingValueNode(ObjectNode json, Optional<OffsetCommitValue> transactionLogValue, short valueVersion) {
        if (transactionLogValue.isPresent()) {
            addDataNode(json, VALUE, valueVersion,
                    OffsetCommitValueJsonConverter.write(transactionLogValue.get(), valueVersion));
        } else {
            addUnknownNode(json, VALUE, valueVersion);
        }
    }

    private void addUnknownNode(ObjectNode json, String nodeName, short keyVersion) {
        json.putObject(nodeName)
                .put(VERSION, Short.toString(keyVersion))
                .put(DATA, "unknown");
    }

    private static void addDataNode(ObjectNode json, String value,
                                    short valueVersion, JsonNode data) {
        json.putObject(value)
                .put(VERSION, Short.toString(valueVersion))
                .set(DATA, data);
    }
}
