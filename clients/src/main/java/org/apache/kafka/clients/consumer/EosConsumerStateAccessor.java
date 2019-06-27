package org.apache.kafka.clients.consumer;


import java.util.Optional;

public class EosConsumerStateAccessor {
    KafkaConsumer<byte[], byte[]> consumer;

    public EosConsumerStateAccessor(Consumer<byte[], byte[]> consumer) {
        if (!(consumer instanceof KafkaConsumer)) {
            throw new IllegalArgumentException("Should be passed in a consumer");
        }
        this.consumer = (KafkaConsumer<byte[], byte[]>) consumer;
    }

    public String groupId() {
        return consumer.groupId;
    }

    public int generationId() {
        return consumer.generationId();
    }

    public Optional<String> groupInstanceId() {
        return consumer.groupInstanceId();
    }

    public void blockingOffsetFetch() {
        while(consumer.generationId() == DEF) {

        }
        consumer.fullOffsetFetch(Long.MAX_VALUE);
    }
}
