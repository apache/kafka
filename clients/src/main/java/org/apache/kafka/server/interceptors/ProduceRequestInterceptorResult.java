package org.apache.kafka.server.interceptors;

public class ProduceRequestInterceptorResult {
    private final byte[] key;
    private final byte[] value;

    public ProduceRequestInterceptorResult(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
}
