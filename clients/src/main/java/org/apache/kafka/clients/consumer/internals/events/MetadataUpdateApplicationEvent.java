package org.apache.kafka.clients.consumer.internals.events;

public class MetadataUpdateApplicationEvent extends ApplicationEvent {

    private final long timestamp;

    public MetadataUpdateApplicationEvent(final long timestamp) {
        super(Type.METADATA_UPDATE);
        this.timestamp = timestamp;
    }
}
