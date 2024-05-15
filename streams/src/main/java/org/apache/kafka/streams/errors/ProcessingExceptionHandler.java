package org.apache.kafka.streams.errors;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.processor.ErrorHandlerContext;

/**
 * An interface that allows user code to inspect a record that has failed processing
 */
public interface ProcessingExceptionHandler extends Configurable {
    /**
     * Inspect a record and the exception received
     *
     * @param context processing context metadata
     * @param record record where the exception occurred
     * @param exception the actual exception
     */
    ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?,?> record, Exception exception);

    enum ProcessingHandlerResponse {
        /* continue with processing */
        CONTINUE(1, "CONTINUE"),
        /* fail the processing and stop */
        FAIL(2, "FAIL");

        /** the permanent and immutable name of processing exception response */
        public final String name;

        /** the permanent and immutable id of processing exception response */
        public final int id;

        ProcessingHandlerResponse(final int id, final String name) {
            this.id = id;
            this.name = name;
        }
    }
}
