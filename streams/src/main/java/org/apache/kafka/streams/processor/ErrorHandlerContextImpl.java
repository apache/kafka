package org.apache.kafka.streams.processor;

import org.apache.kafka.common.header.Headers;

public class ErrorHandlerContextImpl implements ErrorHandlerContext {
    private final String topic;
    private final int partition;
    private final long offset;
    private final Headers headers;
    private final byte[] sourceRawKey;
    private final byte[] sourceRawValue;
    private final String processorNodeId;
    private final TaskId taskId;

    public ErrorHandlerContextImpl(String topic,
                                   int partition,
                                   long offset,
                                   Headers headers,
                                   byte[] sourceRawKey,
                                   byte[] sourceRawValue,
                                   String processorNodeId,
                                   TaskId taskId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.headers = headers;
        this.sourceRawKey = sourceRawKey;
        this.sourceRawValue = sourceRawValue;
        this.processorNodeId = processorNodeId;
        this.taskId = taskId;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    @Override
    public byte[] sourceRawKey() {
        return sourceRawKey;
    }

    @Override
    public byte[] sourceRawValue() {
        return sourceRawValue;
    }

    @Override
    public String processorNodeId() {
        return processorNodeId;
    }

    @Override
    public TaskId taskId() {
        return taskId;
    }
}
