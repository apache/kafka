package com.kafka.replication.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

/**
 * Generates synthetic JSON events matching the required event schema:
 * {
 *   "event_id": "UUID",
 *   "timestamp": epoch_seconds,
 *   "op_type": "INSERT|UPDATE|DELETE",
 *   "key": "doc:hex",
 *   "value": { "status": "..." }
 * }
 */
public class EventGenerator {

    private static final String[] OP_TYPES = {"INSERT", "UPDATE", "DELETE"};
    private static final String[] STATUSES = {"active", "archived", "pending", "processing", "completed", "failed"};

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();
    private String lastEventKey;

    /**
     * Generates a single JSON event string.
     *
     * @return JSON string representing the event
     */
    public String generateEvent() {
        String eventId = UUID.randomUUID().toString();
        long timestamp = Instant.now().getEpochSecond();
        String opType = OP_TYPES[random.nextInt(OP_TYPES.length)];
        String key = "doc:" + generateHexKey();
        String status = STATUSES[random.nextInt(STATUSES.length)];

        this.lastEventKey = key;

        ObjectNode root = mapper.createObjectNode();
        root.put("event_id", eventId);
        root.put("timestamp", timestamp);
        root.put("op_type", opType);
        root.put("key", key);

        ObjectNode value = mapper.createObjectNode();
        value.put("status", status);
        root.set("value", value);

        try {
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }

    /**
     * Returns the key from the last generated event.
     */
    public String getLastEventKey() {
        return lastEventKey;
    }

    private String generateHexKey() {
        return String.format("%04x", random.nextInt(0xFFFF));
    }
}
