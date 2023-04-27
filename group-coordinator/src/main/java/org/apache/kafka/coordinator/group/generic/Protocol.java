package org.apache.kafka.coordinator.group.generic;

import java.util.Arrays;
import java.util.Objects;

public class Protocol {
    private final String name;
    private final byte[] metadata;

    public Protocol(String name, byte[] metadata) {
        this.name = name;
        this.metadata = metadata;
    }

    public String name() {
        return this.name;
    }

    public byte[] metadata() {
        return this.metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Protocol that = (Protocol) o;
        return name.equals(that.name) &&
            Arrays.equals(this.metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            Arrays.hashCode(metadata)
        );
    }

    @Override
    public String toString() {
        return name;
    }
}
