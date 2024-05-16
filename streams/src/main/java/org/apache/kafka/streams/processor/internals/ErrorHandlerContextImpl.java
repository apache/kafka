package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.ErrorHandlerContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;

public class ErrorHandlerContextImpl implements ErrorHandlerContext {

    private InternalProcessorContext processorContext;
    private final String topic;
    private final int partition;
    private final long offset;
    private final Headers headers;
    private final byte[] sourceRawKey;
    private final byte[] sourceRawValue;
    private final String processorNodeId;
    private final TaskId taskId;

    public ErrorHandlerContextImpl(InternalProcessorContext processorContext,
                                   String topic,
                                   int partition,
                                   long offset,
                                   Headers headers,
                                   byte[] sourceRawKey,
                                   byte[] sourceRawValue,
                                   String processorNodeId,
                                   TaskId taskId) {
        this.processorContext = processorContext;
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
        return this.topic;
    }

    @Override
    public int partition() {
        return this.partition;
    }

    @Override
    public long offset() {
        return this.offset;
    }

    @Override
    public Headers headers() {
       return this.headers;
    }

    @Override
    public byte[] sourceRawKey() {
        return this.sourceRawKey;
    }

    @Override
    public byte[] sourceRawValue() {
        return this.sourceRawValue;
    }

    @Override
    public String processorNodeId() {
        return this.processorNodeId;
    }

    @Override
    public TaskId taskId() {
        return this.taskId;
    }

    public ProcessorContext convertToProcessorContext() {
        return new RestrictiveProcessorContext(this.processorContext);
    }
}
