package org.apache.kafka.common.requests;

public enum ResourceType {
    UNKNOWN((byte) 0), ANY((byte) 1), TOPIC((byte)2), GROUP((byte) 3), BROKER((byte) 4);

    private static final ResourceType[] values = values();

    private final byte id;

    ResourceType(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static ResourceType forId(byte id) {
        //FIXME Avoid array allocation
        if (id < 0)
            throw new IllegalArgumentException("id should be positive, id: " + id);
        if (id >= values.length)
            return UNKNOWN;
        return values[id];
    }
}
