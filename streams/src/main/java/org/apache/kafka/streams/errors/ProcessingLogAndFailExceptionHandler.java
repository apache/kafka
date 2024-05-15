package org.apache.kafka.streams.errors;

import org.apache.kafka.streams.processor.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Processing exception handler that logs a processing exception and then
 * signals the processing pipeline to stop processing more records and fail.
 */
public class ProcessingLogAndFailExceptionHandler implements ProcessingExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ProcessingLogAndFailExceptionHandler.class);

    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {
        log.warn("Exception caught during message processing, " +
                        "processor node: {}, taskId: {}, source topic: {}, source partition: {}, source offset: {}",
                context.processorNodeId(), context.taskId(), context.topic(), context.partition(), context.offset(),
                exception);

        return ProcessingHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // ignore
    }
}
