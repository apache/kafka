package org.apache.kafka.test;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MockCallback implements Callback {
    public int invocations = 0;
    public RecordMetadata metadata;
    public Exception exception;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        invocations++;
        this.metadata = metadata;
        this.exception = exception;
    }
}